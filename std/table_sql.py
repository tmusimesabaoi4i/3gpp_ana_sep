#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
table_sql.py（簡素版・改良）

- Pipeline（規則列）を 1本のSQL（WITH/CTE）にコンパイルする
- apply_plan() で「実行は1回」
- get() でCSV→SQLite（必要列だけロード可能）
- __src_rownum により決定的な重複排除を保証
- デバッグモードでSQL生成内容を確認可能

今回の改良点（互換性優先）
- CSVロード(from_csv)をストリーミング化し、巨大CSVでもメモリを食いにくくした
- Progress のコンストラクタ/メソッド差分に耐えるように（存在すれば表示）した
- where_ne の意味を「!= val のみ（NULLは除外）」に統一（要望に合わせて厳密化）
- where_between は DATE(TRIM/REPLACE/CAST) による正規化を強化（YYYY/MM/DDも許容）
- build_pipeline_sql() を (table, pipeline) / (pipeline) の両方の呼び方に対応
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from progress import Progress
from table_csv import TableCSV
from table_rule import Pipeline


# ============================================================
# SQLユーティリティ
# ============================================================

def qident(name: str) -> str:
    """SQLite識別子をダブルクォートでクォート（SQLインジェクション対策）"""
    return '"' + name.replace('"', '""') + '"'


def qstr(s: str) -> str:
    """SQLite文字列リテラルをシングルクォートでクォート（SQLインジェクション対策）"""
    return "'" + str(s).replace("'", "''") + "'"


# ============================================================
# Progress 互換アダプタ（progress.py のAPI差分に耐える）
# ============================================================

def _progress_new(file_size: int, every_lines: int) -> Optional[Any]:
    """
    Progress のAPIが環境で揺れても落ちないように生成する。
    見つからなければ None を返す（進捗なし）。
    """
    try:
        # 例: Progress(file_size_bytes=..., progress_every_lines=...)
        return Progress(file_size_bytes=file_size, progress_every_lines=every_lines)
    except Exception:
        pass
    try:
        # 例: Progress(file_size=..., every=...)
        return Progress(file_size=file_size, every=every_lines)
    except Exception:
        pass
    try:
        # 例: Progress(file_size_bytes=...)
        return Progress(file_size_bytes=file_size)
    except Exception:
        pass
    try:
        # 例: Progress(file_size=...)
        return Progress(file_size=file_size)
    except Exception:
        return None


def _progress_tick(prog: Any, *, lines: int, bad: int, bytes_pos: int, sep: str, encoding: str, table_name: str) -> None:
    """Progress の tick API差分を吸収して呼ぶ。失敗しても落とさない。"""
    if prog is None:
        return

    # パターン1: tick(lines_total=..., bad_total=..., bytes_pos=..., sep=..., encoding=..., table_name=...)
    fn = getattr(prog, "tick", None)
    if callable(fn):
        try:
            fn(lines_total=lines, bad_total=bad, bytes_pos=bytes_pos, sep=sep, encoding=encoding, table_name=table_name)
            return
        except TypeError:
            pass
        except Exception:
            return

        # パターン2: tick_bytes(n) だけ
    fn = getattr(prog, "tick_bytes", None)
    if callable(fn):
        try:
            fn(bytes_pos)
            return
        except Exception:
            return

    # パターン3: tick(n) だけ
    fn = getattr(prog, "tick", None)
    if callable(fn):
        try:
            fn(bytes_pos)
        except Exception:
            return


def _progress_done(prog: Any, *, lines: int, bad: int, table_name: str) -> None:
    if prog is None:
        return
    fn = getattr(prog, "done", None)
    if callable(fn):
        try:
            fn(lines_total=lines, bad_total=bad, table_name=table_name)
            return
        except TypeError:
            pass
        except Exception:
            return
    # done() だけの可能性
    if callable(fn):
        try:
            fn()
        except Exception:
            return


# ============================================================
# SQLPlan（組み立て結果）
# ============================================================

@dataclass(frozen=True)
class SQLPlan:
    sql: str
    params: Tuple[Any, ...]
    required_heads: List[str]   # baseで読むべき列（CSVロード最適化用）。["*"] は全列。
    return_heads: List[str]     # 最終SELECTの列（__src_rownumを含む、指定された場合のみ）

    def debug_print(self) -> None:
        """デバッグ用: 生成されたSQLとパラメータを表示"""
        print("=" * 80)
        print("【生成されたSQL】")
        print("=" * 80)
        print(self.sql)
        print()
        print("【パラメータ】")
        print(self.params)
        print()
        print("【必要列(required_heads)】")
        print(self.required_heads)
        print()
        print("【返却列(return_heads)】")
        print(self.return_heads)
        print("=" * 80)


# ============================================================
# TableSQL（SQLiteテーブルラッパ）
# ============================================================

class TableSQL:
    def __init__(self, conn: sqlite3.Connection, table_name: str, *, owns_conn: bool = True) -> None:
        self.conn = conn
        self.table_name = table_name
        self.owns_conn = owns_conn

    def close(self) -> None:
        if self.owns_conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def columns(self) -> List[str]:
        """テーブルの列名リストを取得（PRAGMA table_info を使用）"""
        cur = self.conn.execute(f"PRAGMA table_info({qident(self.table_name)})")
        return [row[1] for row in cur.fetchall()]

    def iterquery(self, sql: str, params: Tuple[Any, ...] = ()) -> Iterable[Tuple[Any, ...]]:
        """SQLクエリを実行してイテレータで行を返す（メモリ効率的）"""
        return self.conn.execute(sql, params)

    def create_index(self, head: str, *, unique: bool = False, name: Optional[str] = None) -> None:
        if name is None:
            name = f"idx__{self.table_name}__{head}"
        uq = "UNIQUE " if unique else ""
        self.conn.execute(
            f"CREATE {uq}INDEX IF NOT EXISTS {qident(name)} ON {qident(self.table_name)}({qident(head)})"
        )
        self.conn.commit()

    @staticmethod
    def from_csv(
        csv_fullpath: str,
        *,
        sep: Optional[str] = None,
        encoding: Optional[str] = None,
        head: Optional[Sequence[str]] = None,
        db_path: str = ":memory:",
        table_name: str = "t_raw",
        batch: int = 100_000,
        progress_every: int = 300_000,
        pragmas_fast: bool = True,
        read_buffer_bytes: int = 4 * 1024 * 1024,
    ) -> "TableSQL":
        """
        CSVファイルからTableSQLを生成（ストリーミング・高速）

        - __src_rownum 列を自動追加（元ファイルの行番号、決定的な重複排除用）
        - head が指定された場合は、その列だけロードしてI/Oを削減
        """
        if encoding is None:
            encoding = TableCSV.guess_encoding(csv_fullpath)
        if sep is None:
            sep = TableCSV.guess_sep(csv_fullpath, encoding=encoding)

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=OFF;")

        # 速い & 壊れにくい（GB級向け）
        if pragmas_fast:
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA temp_store=MEMORY;")
                conn.execute("PRAGMA locking_mode=EXCLUSIVE;")
            except Exception:
                # 環境差分で失敗しても致命的ではない
                pass

        file_size = os.path.getsize(csv_fullpath)
        prog = _progress_new(file_size, progress_every)

        # バイナリで大きめバッファ → TextIOWrapper で csv.reader（高速）
        with open(csv_fullpath, "rb", buffering=read_buffer_bytes) as fb:
            ftxt = io.TextIOWrapper(fb, encoding=encoding, newline="")
            reader = csv.reader(ftxt, delimiter=sep)

            try:
                header_row = next(reader)
            except StopIteration:
                raise RuntimeError("空ファイルです。")

            header_row = [h.replace("\ufeff", "").strip() for h in header_row]
            idx = {h: i for i, h in enumerate(header_row)}

            # 読む列決定
            if head is None:
                read_heads = header_row[:]  # 全列ロード
            else:
                read_heads = list(head)

            missing = [h for h in read_heads if h not in idx]
            if missing:
                raise RuntimeError(
                    "指定headがCSVヘッダに存在しません: "
                    + ", ".join(missing)
                    + f" / sep={sep!r} enc={encoding!r} header_sample={header_row[:10]!r}"
                )

            # テーブル作成（__src_rownum は INTEGER、それ以外は TEXT）
            cols_sql = "__src_rownum INTEGER, " + ", ".join(f"{qident(h)} TEXT" for h in read_heads)
            conn.execute(f"DROP TABLE IF EXISTS {qident(table_name)}")
            conn.execute(f"CREATE TABLE {qident(table_name)} ({cols_sql})")

            ins_cols = "__src_rownum, " + ", ".join(qident(h) for h in read_heads)
            ph = ", ".join(["?"] * (len(read_heads) + 1))
            ins_sql = f"INSERT INTO {qident(table_name)} ({ins_cols}) VALUES ({ph})"

            buf: List[List[Any]] = []
            lines = 0
            bad = 0

            conn.execute("BEGIN")
            for row in reader:
                lines += 1
                try:
                    out = [row[idx[h]] if idx[h] < len(row) else "" for h in read_heads]
                    buf.append([lines] + out)
                except Exception:
                    bad += 1
                    continue

                if len(buf) >= batch:
                    conn.executemany(ins_sql, buf)
                    buf.clear()

                # 進捗（バイト位置で%を出したい場合に利用）
                try:
                    pos = fb.tell()
                except Exception:
                    pos = 0
                _progress_tick(prog, lines=lines, bad=bad, bytes_pos=pos, sep=sep, encoding=encoding, table_name=table_name)

            if buf:
                conn.executemany(ins_sql, buf)
                buf.clear()

            conn.commit()
            _progress_done(prog, lines=lines, bad=bad, table_name=table_name)

        # __src_rownum にインデックス（unique_by/集計が速くなることが多い）
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_src_rownum ON {qident(table_name)}(__src_rownum)")
            conn.commit()
        except Exception:
            pass

        return TableSQL(conn, table_name, owns_conn=True)


# ============================================================
# required_heads（読むべき列）推定
# ============================================================

def _need_cols_from_steps(
    steps: Sequence[Tuple],
    return_heads: Optional[Sequence[str]] = None,
) -> Tuple[List[str], List[str]]:
    """
    baseで読むべき列(required_heads)を推定する。

    ポリシー:
    - return_heads が None の場合は「全列返す」意図なので、required_heads=["*"] とする
      （CSVロード側も head=None で全列ロードする運用に向く）
    - return_heads が指定されている場合は、必要最小限の列だけを required_heads に積む
    - __src_rownum は決定的な重複排除（unique_by）・後続連鎖のため常に必要
    - concat の newhead は派生列なので derived_heads に積む（baseには不要）
    """
    derived: List[str] = []
    required: List[str] = []

    def add_req(h: str) -> None:
        if h not in required:
            required.append(h)

    def add_der(h: str) -> None:
        if h not in derived:
            derived.append(h)

    # 全列返すなら、ベースは "*" 前提にする（高速/単純）
    if return_heads is None:
        # ただし derived_heads は validation や return_heads 指定時に必要になるので計算しておく
        for st in steps:
            if st[0] == "concat":
                add_der(st[2])
        return ["*"], derived

    # return_heads 指定あり：必要最小限
    for st in steps:
        op = st[0]

        if op == "unique_by":
            add_req(st[1])

        elif op == "concat":
            heads, newhead = st[1], st[2]
            for h in heads:
                add_req(h)
            add_der(newhead)

        elif op in ("where_eq", "where_ne", "where_in", "where_between"):
            add_req(st[1])

        elif op == "where_all_eq":
            mapping: Dict[str, Any] = st[1]
            for h in mapping.keys():
                add_req(h)

        else:
            raise ValueError(f"Unknown step op: {op}")

    # return_heads に指定された列も読む（派生列は base 不要）
    for h in return_heads:
        if h in ("__rid", "__src_rownum"):
            continue
        if h in derived:
            continue
        add_req(h)

    add_req("__src_rownum")
    return required, derived


# ============================================================
# Pipeline → SQLPlan（CTE生成）
# ============================================================

def build_pipeline_sql(
    *args: Union["TableSQL", "Pipeline"],
    return_heads: Optional[Sequence[str]] = None,
) -> SQLPlan:
    """
    Pipeline を 1本のSQL（WITH/CTE）へコンパイル（組み立てのみ）

    互換性:
      - build_pipeline_sql(pipeline, return_heads=...)
      - build_pipeline_sql(table, pipeline, return_heads=...)   # table は互換のため受け取るが未使用

    重要な動作:
    - base CTEで rowid を __rid として保持（中間処理用）
    - __src_rownum は「元CSV順の系譜列」として常に保持（決定的な重複排除のため）
    - unique_by() は ROW_NUMBER() で (__src_rownum, __rid) 順に1行のみを選択（決定的）
    - steps を追加順に s1, s2... として適用
    - 最終SELECT:
      * return_heads 指定時: __src_rownum + return_heads（__rid除外）
      * return_heads 未指定時: 全列（__src_rownum, __rid を含む）
    """
    if len(args) == 1 and isinstance(args[0], Pipeline):
        pipeline = args[0]
    elif len(args) == 2 and isinstance(args[1], Pipeline):
        # 旧: (table, pipeline)
        pipeline = args[1]
    else:
        raise TypeError("build_pipeline_sql(pipeline, ...) または build_pipeline_sql(table, pipeline, ...) で呼んでください。")

    steps = pipeline.steps
    required_heads, derived_heads = _need_cols_from_steps(steps, return_heads=return_heads)

    # baseで読む列の決定
    if "*" in required_heads:
        base = f"SELECT rowid AS __rid, * FROM {{table}}"
        base_cols: List[str] = ["*"]
    else:
        # derived を除いた required を base で選ぶ
        base_cols = [h for h in required_heads if h not in derived_heads]
        # __src_rownum を先頭に（ORDER BY / 連鎖のため）
        base_cols_with_rownum = ["__src_rownum"] + [h for h in base_cols if h != "__src_rownum"]
        cols_sql = ", ".join(qident(h) for h in base_cols_with_rownum) if base_cols_with_rownum else "'' AS __dummy"
        base = f"SELECT rowid AS __rid, {cols_sql} FROM {{table}}"

    ctes: List[str] = [f"base AS ({base})"]
    params: List[Any] = []
    prev = "base"

    for i, st in enumerate(steps, start=1):
        name = f"s{i}"
        op = st[0]

        if op == "where_eq":
            head, val = st[1], st[2]
            if val is None:
                sql_i = f"SELECT * FROM {prev} WHERE {qident(head)} IS NULL"
                par_i: Tuple[Any, ...] = ()
            else:
                sql_i = f"SELECT * FROM {prev} WHERE {qident(head)} = ?"
                par_i = (val,)
            ctes.append(f"{name} AS ({sql_i})")
            params.extend(par_i)
            prev = name
            continue

        if op == "where_ne":
            head, val = st[1], st[2]
            if val is None:
                # != None → IS NOT NULL（NULL除外）
                sql_i = f"SELECT * FROM {prev} WHERE {qident(head)} IS NOT NULL"
                par_i = ()
            else:
                # 「!= val のみ」= NULLは除外する（IS NOT NULL AND <> ?）
                sql_i = f"SELECT * FROM {prev} WHERE {qident(head)} IS NOT NULL AND {qident(head)} <> ?"
                par_i = (val,)
            ctes.append(f"{name} AS ({sql_i})")
            params.extend(par_i)
            prev = name
            continue

        if op == "where_all_eq":
            mapping: Dict[str, Any] = st[1]
            parts: List[str] = []
            par: List[Any] = []
            for h, v in mapping.items():
                if v is None:
                    parts.append(f"{qident(h)} IS NULL")
                else:
                    parts.append(f"{qident(h)} = ?")
                    par.append(v)
            where_sql = " AND ".join(parts) if parts else "1=1"
            sql_i = f"SELECT * FROM {prev} WHERE {where_sql}"
            ctes.append(f"{name} AS ({sql_i})")
            params.extend(par)
            prev = name
            continue

        if op == "where_in":
            head, vals = st[1], st[2]
            if not vals:
                sql_i = f"SELECT * FROM {prev} WHERE 1=0"
                par_i = ()
            else:
                ph = ", ".join(["?"] * len(vals))
                sql_i = f"SELECT * FROM {prev} WHERE {qident(head)} IN ({ph})"
                par_i = tuple(vals)
            ctes.append(f"{name} AS ({sql_i})")
            params.extend(par_i)
            prev = name
            continue

        if op == "where_between":
            head, start, end = st[1], st[2], st[3]

            # 日付比較のために DATE() を使用
            # - "YYYY-MM-DD"（ISO）推奨
            # - "YYYY/MM/DD" でも "/"→"-" 置換して DATE() に渡す
            col_txt = f"TRIM(COALESCE(CAST({qident(head)} AS TEXT), ''))"
            col_norm = f"REPLACE({col_txt}, '/', '-')"
            col_date = f"DATE({col_norm})"

            conds: List[str] = []
            par: List[Any] = []

            if start is not None and str(start).strip() != "":
                conds.append(f"{col_date} >= DATE(?)")
                par.append(start)

            if end is not None and str(end).strip() != "":
                conds.append(f"{col_date} <= DATE(?)")
                par.append(end)

            where_sql = " AND ".join(conds) if conds else "1=1"
            sql_i = f"SELECT * FROM {prev} WHERE {where_sql}"
            ctes.append(f"{name} AS ({sql_i})")
            params.extend(par)
            prev = name
            continue

        if op == "concat":
            heads, newhead, sep = st[1], st[2], st[3]
            parts: List[str] = []
            for j, h in enumerate(heads):
                if j > 0 and sep:
                    parts.append(qstr(sep))
                parts.append(f"TRIM(COALESCE(CAST({qident(h)} AS TEXT), ''))")
            expr = " || ".join(parts) if parts else "''"
            sql_i = f"SELECT *, ({expr}) AS {qident(newhead)} FROM {prev}"
            ctes.append(f"{name} AS ({sql_i})")
            prev = name
            continue

        if op == "unique_by":
            head = st[1]
            # __src_rownum（元ファイル順）→ __rid（SQLite rowid）で完全決定
            sql_i = (
                f"SELECT * FROM ("
                f"  SELECT *, ROW_NUMBER() OVER ("
                f"    PARTITION BY {qident(head)} "
                f"    ORDER BY __src_rownum ASC, __rid ASC"
                f"  ) AS __rn "
                f"  FROM {prev}"
                f") WHERE __rn = 1"
            )
            ctes.append(f"{name} AS ({sql_i})")
            prev = name
            continue

        raise ValueError(f"Unknown step op: {op}")

    # 最終SELECT
    if return_heads is not None:
        user_cols = [h for h in return_heads if h not in ("__rid", "__src_rownum")]
        final_cols = ["__src_rownum"] + user_cols
        cols_sql = ", ".join(qident(h) for h in final_cols) if final_cols else "'' AS __dummy"
        final_sql = f"SELECT {cols_sql} FROM {prev}"
        out_heads = list(final_cols)
    else:
        final_sql = f"SELECT * FROM {prev}"
        out_heads = []

    sql = "WITH " + ", ".join(ctes) + " " + final_sql

    # バリデーション（"*"(全列) のときは不要）
    if return_heads is not None and "*" not in required_heads:
        for h in final_cols:
            if h not in base_cols and h not in derived_heads:
                raise ValueError(
                    f"バリデーションエラー: return_heads に指定された列 '{h}' が base から選択されていません。"
                    f" 必要な列: {base_cols}, 派生列: {list(derived_heads)}"
                )

    return SQLPlan(sql=sql, params=tuple(params), required_heads=base_cols, return_heads=out_heads)


# ============================================================
# 実行（1回）
# ============================================================

def apply_plan(
    table: TableSQL,
    plan: SQLPlan,
    *,
    out_table_name: Optional[str] = None,
    drop_if_exists: bool = True,
) -> TableSQL:
    """
    SQLPlanを実行してテーブルを生成（1回のSQL実行で完了）
    """
    if out_table_name is None:
        out_table_name = f"{table.table_name}__step_{int(time.time() * 1000)}"

    src = qident(table.table_name)
    select_sql = plan.sql.format(table=src)
    out_q = qident(out_table_name)

    if drop_if_exists:
        table.conn.execute(f"DROP TABLE IF EXISTS {out_q}")

    table.conn.execute(f"CREATE TABLE {out_q} AS {select_sql}", plan.params)
    table.conn.commit()

    return TableSQL(table.conn, out_table_name, owns_conn=False)


def apply_pipeline(
    table: TableSQL,
    pipeline: Pipeline,
    *,
    out_table_name: Optional[str] = None,
    return_heads: Optional[Sequence[str]] = None,
    debug: bool = False,
) -> TableSQL:
    """
    Pipelineを適用してテーブルを生成
    """
    plan = build_pipeline_sql(table, pipeline, return_heads=return_heads)
    if debug:
        plan.debug_print()
    return apply_plan(table, plan, out_table_name=out_table_name)


# ============================================================
# CSV → SQLite
# ============================================================

def get(
    csv_fullpath: str,
    *,
    head: Optional[Sequence[str]] = None,
    db_path: str = ":memory:",
    table_name: str = "t_raw",
) -> TableSQL:
    """
    CSVファイルを読み込んでTableSQLを生成（簡易関数）
    """
    return TableSQL.from_csv(
        csv_fullpath,
        head=head,
        db_path=db_path,
        table_name=table_name,
    )

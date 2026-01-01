#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
table_sql.py（簡素版）

- Pipeline（規則列）を 1本のSQL（WITH/CTE）にコンパイルする
- apply_plan() で「実行は1回」
- get() でCSV→SQLite（必要列だけロード可能）
- __src_rownum により決定的な重複排除を保証
- デバッグモードでSQL生成内容を確認可能
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
    return "'" + s.replace("'", "''") + "'"


# ============================================================
# SQLPlan（組み立て結果）
# ============================================================

@dataclass(frozen=True)
class SQLPlan:
    sql: str
    params: Tuple[Any, ...]
    required_heads: List[str]   # baseで読むべき列（CSVロード最適化用）
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
        print("【必要列】")
        print(self.required_heads)
        print()
        print("【返却列】")
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
        cur = self.conn.execute(sql, params)
        return cur

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
        CSVファイルからTableSQLを生成
        
        __src_rownum列を自動追加（元ファイルの行番号、決定的な重複排除用）
        
        引数:
            csv_fullpath: CSVファイルパス
            sep: 区切り文字（None=自動推定）
            encoding: エンコーディング（None=自動推定）
            head: 読み込む列名（None=全列）
            db_path: SQLiteデータベースパス（":memory:"=メモリDB）
            table_name: テーブル名
            batch: バッチサイズ（INSERT効率化）
            progress_every: 進捗表示間隔（行数）
            pragmas_fast: 高速化PRAGMA設定（GB級データ向け）
            read_buffer_bytes: 読み込みバッファサイズ
        """
        if encoding is None:
            encoding = TableCSV.guess_encoding(csv_fullpath)
        if sep is None:
            sep = TableCSV.guess_sep(csv_fullpath, encoding=encoding)

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=OFF;")

        # 速い & 壊れにくい（GB級向け）
        if pragmas_fast:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA locking_mode=EXCLUSIVE;")

        file_size = os.path.getsize(csv_fullpath)
        prog = Progress(file_size_bytes=file_size, progress_every_lines=progress_every)

        with open(csv_fullpath, "rb", buffering=read_buffer_bytes) as fb:
            f = io.TextIOWrapper(fb, encoding=encoding, newline="")
            reader = csv.reader(f, delimiter=sep)

            try:
                header_row = next(reader)
            except StopIteration:
                raise RuntimeError("空ファイルです。")

            header_row = [h.replace("\ufeff", "").strip() for h in header_row]
            idx = {h: i for i, h in enumerate(header_row)}

            if head is None:
                head = header_row[:]  # 全列ロード
            head = list(head)

            missing = [h for h in head if h not in idx]
            if missing:
                raise RuntimeError(
                    "指定headがCSVヘッダに存在しません: "
                    + ", ".join(missing)
                    + f" / sep={sep!r} enc={encoding!r} header_sample={header_row[:10]!r}"
                )

            # __src_rownum列を追加（元ファイルの行番号、決定的な重複排除用）
            cols_sql = "__src_rownum INTEGER, " + ", ".join(f"{qident(h)} TEXT" for h in head)
            conn.execute(f"DROP TABLE IF EXISTS {qident(table_name)}")
            conn.execute(f"CREATE TABLE {qident(table_name)} ({cols_sql})")

            ins_cols = "__src_rownum, " + ", ".join(qident(h) for h in head)
            ph = ", ".join(["?"] * (len(head) + 1))
            ins_sql = f"INSERT INTO {qident(table_name)} ({ins_cols}) VALUES ({ph})"

            buf: List[List[Any]] = []
            lines = 0
            bad = 0

            conn.execute("BEGIN")

            for row in reader:
                lines += 1
                try:
                    out = [row[idx[h]] if idx[h] < len(row) else "" for h in head]
                    # 行番号を先頭に追加
                    buf.append([lines] + out)
                except Exception:
                    bad += 1
                    continue

                if len(buf) >= batch:
                    conn.executemany(ins_sql, buf)
                    buf.clear()

                # 進捗表示（バイト位置で%を出す）
                try:
                    pos = fb.tell()
                except Exception:
                    pos = 0
                prog.tick(lines_total=lines, bad_total=bad, bytes_pos=pos, sep=sep, encoding=encoding, table_name=table_name)

            if buf:
                conn.executemany(ins_sql, buf)
                buf.clear()

            conn.commit()
            prog.done(lines_total=lines, bad_total=bad, table_name=table_name)

        # __src_rownum にインデックスを作成（unique_byの高速化、集計の高速化）
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_src_rownum ON {qident(table_name)}(__src_rownum)")
            conn.commit()
        except Exception:
            pass  # インデックス作成失敗は致命的でない

        return TableSQL(conn, table_name, owns_conn=True)


# ============================================================
# Pipeline → SQLPlan（ここが本体：規則を順にCTE化）
# ============================================================

def _need_cols_from_steps(steps: Sequence[Tuple], return_heads: Optional[Sequence[str]] = None) -> Tuple[List[str], List[str]]:
    """
    baseで読むべき列(required_heads)を推定する
    
    注意: unique_by()は全列を保持する必要があるため、unique_by()が含まれる場合は
    全列選択("*")を推奨。ここでは明示的に必要な列のみを返すが、
    呼び出し側で"*"を使うかどうかを判断する。
    
    重要: __src_rownum は決定的な重複排除のために常に必要
    
    戻り値:
      required_heads: 元テーブルから読むべき列（"*"の場合もある）
      derived_heads: 途中で生成される列（baseには不要）
    """
    required: List[str] = []
    derived: List[str] = []
    has_unique = False

    def add_req(h: str) -> None:
        if h not in required:
            required.append(h)

    def add_der(h: str) -> None:
        if h not in derived:
            derived.append(h)

    for st in steps:
        op = st[0]

        if op == "unique_by":
            # unique_by()は全列を保持する必要がある
            # また、__src_rownumを使用して決定的な重複排除を行う
            has_unique = True
            add_req(st[1])

        elif op == "concat":
            heads, newhead = st[1], st[2]
            for h in heads:
                add_req(h)
            add_der(newhead)

        elif op == "where_eq":
            add_req(st[1])

        elif op == "where_all_eq":
            mapping: Dict[str, Any] = st[1]
            for h in mapping.keys():
                add_req(h)

        elif op == "where_in":
            add_req(st[1])

        else:
            raise ValueError(f"Unknown step op: {op}")

    # return_headsが指定されている場合、それらも必要列に追加
    if return_heads is not None:
        for h in return_heads:
            if h not in derived:
                add_req(h)

    # unique_byがある場合は全列選択が必要（__src_rownumを含む）
    if has_unique:
        return ["*"], derived

    # unique_byがなくても、__src_rownumは今後のために保持推奨
    # （パイプラインを連鎖させる場合に備えて）
    add_req("__src_rownum")

    return required, derived


def build_pipeline_sql(p: Pipeline, *, return_heads: Optional[Sequence[str]] = None) -> SQLPlan:
    """
    Pipelineを 1本のSQL（WITH/CTE）へコンパイル（組み立てのみ）

    重要な動作:
    - base CTEで rowid を __rid として保持（中間処理用）
    - __src_rownum は「元CSV順の系譜列」として常に保持（決定的な重複排除のため）
    - unique_by() がある場合は全列を選択（SELECT * FROM {table}）
    - unique_by() は ROW_NUMBER() で (__src_rownum, __rid) 順に1行のみを選択
    - stepsを追加順に s1, s2... として適用
    - 最終SELECT:
      * return_heads 指定時: __src_rownum + return_heads の列を返す（__ridは除外）
      * return_heads 未指定時: 全列を返す（__src_rownum, __ridを含む）
    
    __src_rownum を常に保持する理由:
    - 中間テーブルを後続のパイプラインで使う場合に必要
    - unique_by() が依存する決定的な順序を保証
    
    バリデーション:
    - return_heads で指定された列が base に含まれているか確認
    - 不整合があればビルド時にエラー
    """
    steps = p.steps
    required_heads, derived_heads = _need_cols_from_steps(steps, return_heads=return_heads)

    # baseで読む列の決定
    # 注意: __src_rownum は決定的な重複排除のために必須
    # "*"が含まれる場合は全列選択（__src_rownumも含まれる）
    if "*" in required_heads:
        base = f"SELECT rowid AS __rid, * FROM {{table}}"
        base_cols = []  # 全列なので個別列リストは不要
    else:
        # baseで読む列：required_heads（派生列は除外）
        base_cols = [h for h in required_heads if h not in derived_heads]
        # __src_rownumを明示的に追加（unique_byで使用）
        base_cols_with_rownum = ["__src_rownum"] + [h for h in base_cols if h != "__src_rownum"]
        base_cols_sql = ", ".join(qident(h) for h in base_cols_with_rownum) if base_cols_with_rownum else "'' AS __dummy"
        base = f"SELECT rowid AS __rid, {base_cols_sql} FROM {{table}}"

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

        if op == "concat":
            heads, newhead, sep = st[1], st[2], st[3]
            # TRIM + COALESCE + CAST(TEXT) で安定化
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
            # __src_rownum（元ファイルの行番号）が最小の行＝最初に出現した行
            # 決定的な重複排除を保証
            # ROW_NUMBER()を使用して、(__src_rownum, __rid)の順で絶対的に1行のみを選択
            # これにより、万が一__src_rownumが重複していても確実に1行のみが選択される
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
    # return_headsが指定されている場合：
    # - 出力列は基本的に return_heads を尊重する
    # - ただし __src_rownum は「元CSV順を保持するための系譜列」なので必ず保持する
    #   （後続の unique_by() が ORDER BY __src_rownum に依存するため）
    # - __rid は一時的な内部列なので除外する
    if return_heads is not None:
        user_cols = [h for h in return_heads if h != "__rid" and h != "__src_rownum"]
        
        # 内部列: __src_rownum は常に付与（中間テーブル連鎖の安定性確保）
        final_cols = ["__src_rownum"] + user_cols
        
        cols_sql = ", ".join(qident(h) for h in final_cols) if final_cols else "'' AS __dummy"
        final_sql = f"SELECT {cols_sql} FROM {prev}"
        out_heads = list(final_cols)
    else:
        # return_heads未指定の場合：全列を返す（__src_rownum, __ridも含む）
        # 注意: 中間テーブルを後続のパイプラインで使う場合、__src_rownumが必要
        final_sql = f"SELECT * FROM {prev}"
        out_heads = []

    sql = "WITH " + ", ".join(ctes) + " " + final_sql
    
    # バリデーション: "*"でない場合、return_headsの列がbase_colsに含まれているか確認
    if return_heads is not None and "*" not in required_heads:
        for h in final_cols:
            if h not in base_cols and h not in derived_heads:
                raise ValueError(
                    f"バリデーションエラー: return_heads に指定された列 '{h}' が "
                    f"base から選択されていません。必要な列: {base_cols}, 派生列: {list(derived_heads)}"
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
    
    引数:
        table: 入力テーブル
        plan: 実行するSQLプラン
        out_table_name: 出力テーブル名（省略時は自動生成）
        drop_if_exists: 既存テーブルを削除するか
    
    戻り値:
        生成されたTableSQL（元のconnを共有）
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
    
    引数:
        table: 入力テーブル
        pipeline: 適用するパイプライン
        out_table_name: 出力テーブル名（省略時は自動生成）
        return_heads: 返却する列（省略時は全列）
        debug: Trueの場合、生成されたSQLを表示
    """
    plan = build_pipeline_sql(pipeline, return_heads=return_heads)
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
    
    引数:
        csv_fullpath: CSVファイルパス
        head: 読み込む列名（None=全列）
        db_path: SQLiteデータベースパス
        table_name: テーブル名
    
    戻り値:
        TableSQL（__src_rownum列を含む）
    """
    return TableSQL.from_csv(
        csv_fullpath,
        head=head,
        db_path=db_path,
        table_name=table_name,
    )
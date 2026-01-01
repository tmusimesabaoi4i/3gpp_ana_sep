#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
normalization.py

【仕様確定版】
- 正規化は normal(raw) を独立させて 1 回だけ適用する。
- 以後は SQL適用(table_sql.apply_plan) を繰り返す。

高速化（GB級対応）:
- 出力INSERTは1トランザクションでまとめる（commitは最後に1回）
- batch を大きめに（デフォルト 100k）

重要: __src_rownum の扱い
- raw テーブルの __src_rownum 列をそのまま t_norm に保持
- CSV元ファイルの順序を保つため、__src_rownum でソート
- t_norm にもインデックスを作成（unique_by の高速化）
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence

from progress import Progress
from table_sql import TableSQL, qident


class Normalization:
    """デフォルト正規化関数群"""

    @staticmethod
    def data_normalization_method(values: List[str]) -> List[str]:
        """
        データ正規化（全列対象）
        - 前後空白除去
        - None相当は空文字へ
        - タブ→半角スペース
        - 連続する空白を1つの半角スペースに圧縮
        """
        out: List[str] = []
        for v in values:
            s = "" if v is None else str(v)
            s = s.strip()
            s = s.replace("\t", " ")
            s = re.sub(r"[ ]{2,}", " ", s)
            out.append(s)
        return out

    @staticmethod
    def zero_one_normalization_method(values: List[str]) -> List[str]:
        """
        0/1正規化（対象列のみ）
        - Yes系 -> "1"
        - No系/空 -> "0"
        """
        yes = {"1", "yes", "y", "true", "t"}
        no = {"0", "no", "n", "false", "f", ""}
        out: List[str] = []
        for v in values:
            s = "" if v is None else str(v)
            s = s.strip().lower()
            if s in yes:
                out.append("1")
            elif s in no:
                out.append("0")
            else:
                out.append("0")
        return out


def _auto_detect_01_cols(cols: Sequence[str]) -> List[str]:
    """
    0/1列の自動推定（列名規則ベース）
    - 2G/3G/4G/5G
    - *_FLAG
    - ESS_/LICD_/DECL_/ESS_TO_ で始まる
    - Ess_To_Standard（特別扱い）
    """
    out: List[str] = []
    for c in cols:
        # 特別扱い: Ess_To_Standard
        if c == "Ess_To_Standard":
            out.append(c)
            continue
        if c in ("2G", "3G", "4G", "5G"):
            out.append(c)
            continue
        if c.endswith("_FLAG"):
            out.append(c)
            continue
        if c.startswith(("ESS_", "LICD_", "DECL_", "ESS_TO_")):
            out.append(c)
            continue
    return out


@dataclass
class Normalizer:
    """
    テーブル正規化クラス

    - data_norm: 全列に適用する正規化関数 (list[str] -> list[str])
    - norm01: 0/1対象列に適用する正規化関数 (list[str] -> list[str])
    - normalize_01_cols: 明示的な0/1列指定（Noneなら自動推定）
    
    注意: __src_rownum は正規化対象外（そのまま保持される内部列）
    """
    data_norm: Callable[[List[str]], List[str]] = Normalization.data_normalization_method
    norm01: Callable[[List[str]], List[str]] = Normalization.zero_one_normalization_method
    normalize_01_cols: Optional[List[str]] = None

    def apply(
        self,
        raw: TableSQL,
        *,
        out_table_name: str = "t_norm",
        batch: int = 100_000,
        progress_every: int = 300_000,
        drop_if_exists: bool = True,
    ) -> TableSQL:
        """
        raw(TableSQL) -> 正規化済みTableSQL を生成する（正規化はここで1回だけ）。
        
        重要な動作:
        - __src_rownum 列は正規化せずそのまま保持（決定的な重複排除用）
        - 読み込みは __src_rownum でソート（CSV元ファイルの順序を保つ）
        - 出力テーブルにも __src_rownum のインデックスを作成
        """
        cols = raw.columns()
        if not cols:
            raise RuntimeError("正規化対象テーブルに列がありません。")

        # __src_rownumを除外して処理（そのままコピーする）
        cols_to_normalize = [c for c in cols if c != "__src_rownum"]
        has_src_rownum = "__src_rownum" in cols

        if self.normalize_01_cols is None:
            cols01 = _auto_detect_01_cols(cols_to_normalize)
        else:
            cols01 = [c for c in self.normalize_01_cols if c in cols_to_normalize]
        cols01_set = set(cols01)
        idxs = [cols_to_normalize.index(c) for c in cols01] if cols01 else []

        ddl_cols = []
        # __src_rownumを先頭に追加
        if has_src_rownum:
            ddl_cols.append(f"{qident('__src_rownum')} INTEGER")
        for c in cols_to_normalize:
            ddl_cols.append(f"{qident(c)} INTEGER" if c in cols01_set else f"{qident(c)} TEXT")
        ddl = ", ".join(ddl_cols)

        out_q = qident(out_table_name)
        if drop_if_exists:
            raw.conn.execute(f"DROP TABLE IF EXISTS {out_q}")
        raw.conn.execute(f"CREATE TABLE {out_q} ({ddl})")

        # INSERT列リストの構築
        ins_cols_list = []
        if has_src_rownum:
            ins_cols_list.append("__src_rownum")
        ins_cols_list.extend(cols_to_normalize)
        ins_cols = ", ".join(qident(c) for c in ins_cols_list)
        ph = ", ".join(["?"] * len(ins_cols_list))
        ins_sql = f"INSERT INTO {out_q} ({ins_cols}) VALUES ({ph})"

        # 正規化は file_size 不明なので lines/s 表示になる
        prog = Progress(file_size_bytes=0, progress_every_lines=progress_every)

        src_q = qident(raw.table_name)
        sel_cols = ", ".join(qident(c) for c in cols)
        # 重要: CSV元ファイルの順序を保つため __src_rownum でソート
        if has_src_rownum:
            sel_sql = f"SELECT {sel_cols} FROM {src_q} ORDER BY __src_rownum"
        else:
            sel_sql = f"SELECT {sel_cols} FROM {src_q}"

        buf: List[List[object]] = []
        lines = 0
        bad = 0

        raw.conn.execute("BEGIN")

        for row in raw.iterquery(sel_sql):
            lines += 1
            try:
                # __src_rownumとその他の列を分離
                src_rownum_val = None
                values_dict = {}
                for i, c in enumerate(cols):
                    if c == "__src_rownum":
                        src_rownum_val = row[i]
                    else:
                        values_dict[c] = row[i]
                
                # 正規化対象列のみを処理
                values = ["" if values_dict.get(c) is None else str(values_dict.get(c)) for c in cols_to_normalize]
                values = self.data_norm(values)

                if idxs:
                    sub = [values[i] for i in idxs]
                    sub = self.norm01(sub)
                    for k, i in enumerate(idxs):
                        values[i] = sub[k]

                out_row: List[object] = []
                # __src_rownumを先頭に追加
                if has_src_rownum:
                    out_row.append(src_rownum_val)
                
                for c, v in zip(cols_to_normalize, values):
                    if c in cols01_set:
                        try:
                            out_row.append(int(v))
                        except Exception:
                            out_row.append(0)
                    else:
                        out_row.append(v)

                buf.append(out_row)
            except Exception:
                bad += 1
                continue

            if len(buf) >= batch:
                raw.conn.executemany(ins_sql, buf)
                buf.clear()

            prog.tick(lines_total=lines, bad_total=bad, bytes_pos=0, sep="?", encoding="?", table_name=out_table_name)

        if buf:
            raw.conn.executemany(ins_sql, buf)
            buf.clear()

        raw.conn.commit()
        prog.done(lines_total=lines, bad_total=bad, table_name=out_table_name)

        # __src_rownum にインデックスを作成（unique_byの高速化）
        if has_src_rownum:
            try:
                raw.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{out_table_name}_src_rownum ON {out_q}(__src_rownum)")
                raw.conn.commit()
            except Exception:
                pass  # インデックス作成失敗は致命的でない

        return TableSQL(raw.conn, out_table_name, owns_conn=False)


def normal(raw: TableSQL, normalizer: Optional[Normalizer] = None, *, out_table_name: str = "t_norm") -> TableSQL:
    """
    rawを正規化して返す（正規化はここで1回だけ）。
    
    - __src_rownum 列を保持
    - 0/1列を自動検出して正規化（Ess_To_Standard を含む）
    - CSV元ファイルの順序を保つ
    """
    nz = normalizer or Normalizer()
    return nz.apply(raw, out_table_name=out_table_name)

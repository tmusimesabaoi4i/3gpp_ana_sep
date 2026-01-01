#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
normalization.py（自動推定なし / 欠損=-1 / yes=1,no=0 / 日付はYYYY-MM-DDへ）

追加要件（今回）
- IPRD_SIGNATURE_DATE / Reflected_Date / PBPA_APP_DATE は
  "YYYY-MM-DD HH:MM:SS" 等で入るため、"YYYY-MM-DD" のみに正規化する

共通要件（維持）
- 欠損（データが存在しない）:
  - INTEGER列: -1
  - TEXT列: "-1"
- フラグ列（BOOL01_COLS_EXPLICIT）:
  - yes系 -> 1
  - no系  -> 0
  - 欠損/不明 -> -1
- COMP_LEGAL_NAME: 「,」「.」除去
- 全列共通: trim/タブ→空白/連続空白圧縮/改行→空白
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, List, Optional

from progress import Progress
from table_sql import TableSQL, qident


# ============================================================
# 欠損（エラー）表現
# ============================================================
ERROR_INT = -1
ERROR_TEXT = "-1"

# TEXT列でも欠損扱いにするトークン（完全一致・小文字）
_MISSING_TOKENS = {"", "-1", "error", "err", "unknown", "na", "n/a", "nan", "null", "none", "?"}


# ============================================================
# 列型（明示）
# ============================================================
INT_COLS_EXPLICIT = {
    "Explicitely_Disclosed",
    "Normalized_Patent",
    "2G",
    "3G",
    "4G",
    "5G",
    "DECL_IS_PROP_FLAG",
    "LICD_DEC_PREP_TO_GRANT_FLAG",
    "LICD_REC_CONDI_FLAG",
    "Ess_To_Standard",
    "Ess_To_Project",
}

# 0/1フラグ列（yes/no系を 1/0 に、欠損は -1）
BOOL01_COLS_EXPLICIT = {
    "Explicitely_Disclosed",
    "Normalized_Patent",
    "2G",
    "3G",
    "4G",
    "5G",
    "DECL_IS_PROP_FLAG",
    "LICD_DEC_PREP_TO_GRANT_FLAG",
    "LICD_REC_CONDI_FLAG",
    "Ess_To_Standard",
    "Ess_To_Project",
}

# 今回：日付列は "YYYY-MM-DD" を保持したいので TEXT に強制する（INT指定より優先）
DATE_ONLY_COLS = {"IPRD_SIGNATURE_DATE", "Reflected_Date", "PBPA_APP_DATE"}
FORCE_TEXT_COLS = set(DATE_ONLY_COLS)

# 列特化
COL_COMP_LEGAL_NAME = "COMP_LEGAL_NAME"


def _is_missing_text(v: str) -> bool:
    s = "" if v is None else str(v)
    return s.strip().lower() in _MISSING_TOKENS


def _date_only(v: str) -> str:
    """
    "YYYY-MM-DD HH:MM:SS" 等 → "YYYY-MM-DD"
    - 既に YYYY-MM-DD ならそのまま
    - YYYY/MM/DD, YYYY.MM.DD, YYYY年M月D日 も吸収（できる範囲で）
    - 欠損/不明は "" を返す（後段で "-1" に統一）
    """
    if v is None:
        return ""
    s = str(v).strip()
    if _is_missing_text(s):
        return ""

    # 先頭トークン（空白 or T 区切り）を日付部として扱う
    s = re.split(r"[ T]", s, 1)[0].strip()

    # 日本語表記をざっくり吸収（YYYY年MM月DD日）
    s = s.replace("年", "-").replace("月", "-").replace("日", "")
    s = s.replace("/", "-").replace(".", "-")

    # YYYYMMDD
    if re.fullmatch(r"\d{8}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

    # YYYY-M-D / YYYY-MM-DD など → ゼロパディングして YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return f"{y}-{int(mo):02d}-{int(d):02d}"

    # 想定外はそのまま返す（後工程で確認できるように）
    return s


class Normalization:
    @staticmethod
    def data_normalization_method(values: List[str]) -> List[str]:
        """
        全列共通の文字列正規化
        - None -> ""
        - 改行(\r\n/\r/\n) はスペースへ（単語結合を防ぐ）
        - trim
        - タブ→スペース
        - 連続スペース圧縮
        """
        out: List[str] = []
        for v in values:
            s = "" if v is None else str(v)
            s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
            s = s.strip()
            s = s.replace("\t", " ")
            s = re.sub(r"[ ]{2,}", " ", s)
            out.append(s)
        return out

    @staticmethod
    def yn01e_to_int(v: str) -> int:
        """
        yes/no/欠損（error等）を 1/0/-1 に正規化する（int）
        - yes系 -> 1
        - no系  -> 0
        - 空/None/error/unknown/その他 -> -1
        """
        s = "" if v is None else str(v)
        s = s.strip().lower()

        yes = {"1", "yes", "y", "true", "t", "on"}
        no = {"0", "no", "n", "false", "f", "off"}

        if s in yes:
            return 1
        if s in no:
            return 0
        if s in _MISSING_TOKENS:
            return ERROR_INT
        return ERROR_INT

    @staticmethod
    def parse_int_or_error(v: str) -> int:
        """
        INTEGER列（フラグ以外）を int にする。
        - 欠損/不正 -> -1
        """
        if v is None:
            return ERROR_INT
        s = str(v).strip()
        if s == "":
            return ERROR_INT
        if s.lower() in _MISSING_TOKENS:
            return ERROR_INT
        try:
            return int(s)
        except Exception:
            return ERROR_INT


@dataclass
class Normalizer:
    data_norm: Callable[[List[str]], List[str]] = Normalization.data_normalization_method
    normalize_01_cols: Optional[List[str]] = None  # 明示追加したい場合のみ

    def apply(
        self,
        raw: TableSQL,
        *,
        out_table_name: str = "t_norm",
        batch: int = 100_000,
        progress_every: int = 300_000,
        drop_if_exists: bool = True,
    ) -> TableSQL:
        cols = raw.columns()
        if not cols:
            raise RuntimeError("正規化対象テーブルに列がありません。")

        cols_to_normalize = [c for c in cols if c != "__src_rownum"]
        has_src_rownum = "__src_rownum" in cols

        # INTEGER列（ユーザ指定）※force-text は除外
        cols_int_set = {c for c in cols_to_normalize if (c in INT_COLS_EXPLICIT and c not in FORCE_TEXT_COLS)}

        # 0/1フラグ列（ユーザ指定）※integer で持つ列のみを対象にする
        cols_bool01_set = {c for c in cols_to_normalize if c in BOOL01_COLS_EXPLICIT and c in cols_int_set}

        # 明示追加（任意）
        if self.normalize_01_cols:
            for c in self.normalize_01_cols:
                if c in cols_int_set:
                    cols_bool01_set.add(c)

        # COMP_LEGAL_NAME の位置（TEXT列）
        try:
            idx_comp = cols_to_normalize.index(COL_COMP_LEGAL_NAME)
        except ValueError:
            idx_comp = -1

        # 日付列の位置（存在する列だけ）
        date_idxs = []
        for dc in DATE_ONLY_COLS:
            if dc in cols_to_normalize:
                date_idxs.append(cols_to_normalize.index(dc))

        # DDL：INTEGER列のみ INTEGER、それ以外 TEXT
        ddl_cols = []
        if has_src_rownum:
            ddl_cols.append(f"{qident('__src_rownum')} INTEGER")
        for c in cols_to_normalize:
            if c in cols_int_set:
                ddl_cols.append(f"{qident(c)} INTEGER")
            else:
                ddl_cols.append(f"{qident(c)} TEXT")

        out_q = qident(out_table_name)
        if drop_if_exists:
            raw.conn.execute(f"DROP TABLE IF EXISTS {out_q}")
        raw.conn.execute(f"CREATE TABLE {out_q} ({', '.join(ddl_cols)})")

        # INSERT
        ins_cols_list = (["__src_rownum"] if has_src_rownum else []) + cols_to_normalize
        ins_cols = ", ".join(qident(c) for c in ins_cols_list)
        ph = ", ".join(["?"] * len(ins_cols_list))
        ins_sql = f"INSERT INTO {out_q} ({ins_cols}) VALUES ({ph})"

        prog = Progress(file_size_bytes=0, progress_every_lines=progress_every)

        src_q = qident(raw.table_name)
        sel_cols = ", ".join(qident(c) for c in cols)
        sel_sql = f"SELECT {sel_cols} FROM {src_q}"
        if has_src_rownum:
            sel_sql += " ORDER BY __src_rownum"

        buf: List[List[object]] = []
        lines = 0
        bad = 0

        raw.conn.execute("BEGIN")

        for row in raw.iterquery(sel_sql):
            lines += 1
            try:
                src_rownum_val = None
                values_dict = {}
                for i, c in enumerate(cols):
                    if c == "__src_rownum":
                        src_rownum_val = row[i]
                    else:
                        values_dict[c] = row[i]

                # 全列を文字列として正規化（空は ""）
                values = ["" if values_dict.get(c) is None else str(values_dict.get(c)) for c in cols_to_normalize]
                values = self.data_norm(values)

                # 列特化: COMP_LEGAL_NAME の ',' '.' を除去
                if idx_comp >= 0:
                    s = values[idx_comp] or ""
                    s = s.replace(",", "").replace(".", "")
                    s = s.strip()
                    s = re.sub(r"[ ]{2,}", " ", s)
                    values[idx_comp] = s

                # 日付列: "YYYY-MM-DD" のみに正規化（欠損は "" のまま）
                for di in date_idxs:
                    values[di] = _date_only(values[di])

                # 出力行を組み立て
                out_row: List[object] = []
                if has_src_rownum:
                    out_row.append(src_rownum_val)

                for c, v in zip(cols_to_normalize, values):
                    if c in cols_int_set:
                        # INTEGER列
                        if c in cols_bool01_set:
                            out_row.append(Normalization.yn01e_to_int(v))
                        else:
                            out_row.append(Normalization.parse_int_or_error(v))
                    else:
                        # TEXT列: 欠損は "-1"
                        out_row.append(ERROR_TEXT if _is_missing_text(v) else v)

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

        # __src_rownum にインデックス
        if has_src_rownum:
            try:
                raw.conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{out_table_name}_src_rownum ON {out_q}(__src_rownum)"
                )
                raw.conn.commit()
            except Exception:
                pass

        return TableSQL(raw.conn, out_table_name, owns_conn=False)


def normal(raw: TableSQL, normalizer: Optional[Normalizer] = None, *, out_table_name: str = "t_norm") -> TableSQL:
    """rawを正規化して返す（正規化はここで1回だけ）。"""
    nz = normalizer or Normalizer()
    return nz.apply(raw, out_table_name=out_table_name)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
4つの分析スクリプト（会社別FD / JP会社別FD / TS/TR別FD / JP TS/TR別FD）を
「CSV取得(get)・正規化(normal)は1回だけ」に統合した実行用スクリプト。

- 入力: ISLD-export.csv
- 共有処理: CSV読み込み → 正規化(t_norm)
- その後、以下を順に実行してCSVを書き出す:
  A) 全体: COMP_LEGAL_NAME の度数分布（ユニーク化前/後）
  B) JP限定: COMP_LEGAL_NAME の度数分布（ユニーク化前/後）
  C) 全体: TSTRNUM (= 3GPP_Type + "_" + TGPP_NUMBER) の度数分布（ユニーク化前/後）
  D) JP限定: TSTRNUM の度数分布（ユニーク化前/後）

注意:
- 中間テーブル名はジョブごとにprefixを付け、同一DB上でも衝突しにくいようにしています。
- 出力CSV名(prefix)は元の4ファイルと同じにしています。
"""

from __future__ import annotations

from table_sql import get, apply_pipeline
from table_rule import Pipeline
from normalization import normal
from table_al import TableAL


# ============================================================
# 設定（入出力）
# ============================================================
CSV_PATH = "../../ISLD-export/ISLD-export.csv"
DB_PATH = "work.sqlite"

GEN_COLS = ("3G", "4G", "5G")  # 世代フラグ列

# 4スクリプトで必要な列の「和集合」だけを読み込む（I/O最小化）
HEADS_ALL = [
    "IPRD_ID",
    "COMP_LEGAL_NAME",
    "Ess_To_Standard",
    "3GPP_Type",
    "TGPP_NUMBER",
    "Country_Of_Registration",
    "3G",
    "4G",
    "5G",
]

# JP判定
JP_VALUE = "JP JAPAN"

# TS/TR 連結列（技術仕様番号）
CONNECT_HEADS = ["3GPP_Type", "TGPP_NUMBER"]
NEW_HEAD_TSTRNUM = "TSTRNUM"
SEP_TSTRNUM = "_"

# 出力prefix（元ファイルに合わせる）
OUT_PREFIX_COMP_ALL = "fd"
OUT_PREFIX_COMP_JP = "fd_jp"
OUT_PREFIX_TSTR_ALL = "fd_tstr"
OUT_PREFIX_TSTR_JP = "fd_jp_tstr"


# ============================================================
# 共通ヘルパ
# ============================================================
def _build_filtered_table(
    raw_norm,
    *,
    gen_col: str,
    only_jp: bool,
    out_table_name: str,
    return_heads: list[str],
):
    """
    フィルタ済みテーブル（ユニーク化前）を作る。

    条件（AND）:
      - gen_col == 1
      - Ess_To_Standard == 1
      - (only_jp=True の場合) Country_Of_Registration == JP_VALUE
    """
    pipeline = Pipeline()
    pipeline.where_eq(gen_col, 1)
    pipeline.where_eq("Ess_To_Standard", 1)
    if only_jp:
        pipeline.where_eq("Country_Of_Registration", JP_VALUE)

    return apply_pipeline(raw_norm, pipeline, out_table_name=out_table_name, return_heads=return_heads)


def _build_unique_by_iprd(
    tbl,
    *,
    out_table_name: str,
    return_heads: list[str] | None = None,
):
    """
    IPRD_ID でユニーク化したテーブル（ユニーク化後）を作る。
    同一 IPRD_ID は「最初の行（rowid最小）」を採用。
    """
    tbl.create_index("IPRD_ID")  # 速くなることが多い

    pipeline = Pipeline()
    pipeline.unique_by("IPRD_ID")
    return apply_pipeline(tbl, pipeline, out_table_name=out_table_name, return_heads=return_heads)


def _save_frequency_csv(
    al: TableAL,
    *,
    tbl_non_unique,
    tbl_unique,
    freq_head: str,
    out_prefix: str,
    gen_col: str,
):
    """
    指定列(freq_head)の度数分布を（ユニーク化前/後）で保存する。
    """
    labels_nu, counts_nu = al.frequency_distribution(tbl_non_unique, freq_head)
    labels_uq, counts_uq = al.frequency_distribution(tbl_unique, freq_head)

    al.save_as_file((labels_nu, counts_nu), f"{out_prefix}_{gen_col}_non_unique.csv")
    al.save_as_file((labels_uq, counts_uq), f"{out_prefix}_{gen_col}.csv")


# ============================================================
# ジョブA/B: 会社別（COMP_LEGAL_NAME）FD
# ============================================================
def run_company_fd(raw_norm, *, only_jp: bool, out_prefix: str, table_tag: str, al: TableAL) -> None:
    """
    会社名（COMP_LEGAL_NAME）の度数分布を作る（ユニーク化前/後）。

    - ユニーク化キー: IPRD_ID
    """
    # 後段（unique_by / frequency_distribution）に必要な最小限の列だけ残す
    return_heads = ["IPRD_ID", "COMP_LEGAL_NAME"]

    for gen_col in GEN_COLS:
        tbl_filtered = _build_filtered_table(
            raw_norm,
            gen_col=gen_col,
            only_jp=only_jp,
            out_table_name=f"t_{table_tag}_flt_{gen_col}",
            return_heads=return_heads,
        )
        tbl_unique = _build_unique_by_iprd(
            tbl_filtered,
            out_table_name=f"t_{table_tag}_uq_{gen_col}",
            return_heads=return_heads,
        )
        _save_frequency_csv(
            al,
            tbl_non_unique=tbl_filtered,
            tbl_unique=tbl_unique,
            freq_head="COMP_LEGAL_NAME",
            out_prefix=out_prefix,
            gen_col=gen_col,
        )


# ============================================================
# ジョブC/D: TS/TR別（TSTRNUM）FD
# ============================================================
def _build_connect_tstrnum(tbl_filtered, *, out_table_name: str):
    """
    連結列を作成する（3GPP_Type + "_" + TGPP_NUMBER -> TSTRNUM）
    例: "TS" + "_" + "36.331" -> "TS_36.331"

    返却テーブルは後段に必要な最小限の列のみ:
      - IPRD_ID（ユニーク化キー）
      - TSTRNUM（度数分布対象）
    """
    pipeline = Pipeline()
    pipeline.concat(CONNECT_HEADS, NEW_HEAD_TSTRNUM, SEP_TSTRNUM)
    return apply_pipeline(
        tbl_filtered,
        pipeline,
        out_table_name=out_table_name,
        return_heads=["IPRD_ID", NEW_HEAD_TSTRNUM],
    )


def run_tstrnum_fd(raw_norm, *, only_jp: bool, out_prefix: str, table_tag: str, al: TableAL) -> None:
    """
    技術仕様番号（TSTRNUM = 3GPP_Type + "_" + TGPP_NUMBER）の度数分布を作る（ユニーク化前/後）。

    - ユニーク化キー: IPRD_ID
    """
    # 連結作成(pipeline.concat)に必要な最小限の列だけ残す
    return_heads_for_concat = ["IPRD_ID", "3GPP_Type", "TGPP_NUMBER"]

    for gen_col in GEN_COLS:
        tbl_filtered = _build_filtered_table(
            raw_norm,
            gen_col=gen_col,
            only_jp=only_jp,
            out_table_name=f"t_{table_tag}_flt_{gen_col}",
            return_heads=return_heads_for_concat,
        )
        tbl_connect = _build_connect_tstrnum(
            tbl_filtered,
            out_table_name=f"t_{table_tag}_cn_{gen_col}",
        )
        tbl_unique = _build_unique_by_iprd(
            tbl_connect,
            out_table_name=f"t_{table_tag}_uq_{gen_col}",
            return_heads=["IPRD_ID", NEW_HEAD_TSTRNUM],
        )
        _save_frequency_csv(
            al,
            tbl_non_unique=tbl_connect,
            tbl_unique=tbl_unique,
            freq_head=NEW_HEAD_TSTRNUM,
            out_prefix=out_prefix,
            gen_col=gen_col,
        )


# ============================================================
# main（get + normal は1回だけ）
# ============================================================
def main() -> None:
    """
    実行順:
      1) CSV読み込み（必要列のみ）
      2) 正規化（1回だけ）
      3) 4ジョブを順次実行してCSV出力
    """
    al = TableAL()

    raw = get(CSV_PATH, db_path=DB_PATH, head=HEADS_ALL)
    try:
        # ----------------------------------------------------
        # 1) 正規化（全ジョブで共有 / 1回のみ）
        # ----------------------------------------------------
        raw_norm = normal(raw, out_table_name="t_norm")

        # ----------------------------------------------------
        # 2) 会社別FD（全体 / JP限定）
        # ----------------------------------------------------
        run_company_fd(raw_norm, only_jp=False, out_prefix=OUT_PREFIX_COMP_ALL, table_tag="cmp_all", al=al)
        run_company_fd(raw_norm, only_jp=True, out_prefix=OUT_PREFIX_COMP_JP, table_tag="cmp_jp", al=al)

        # ----------------------------------------------------
        # 3) TS/TR別FD（全体 / JP限定）
        # ----------------------------------------------------
        run_tstrnum_fd(raw_norm, only_jp=False, out_prefix=OUT_PREFIX_TSTR_ALL, table_tag="tstr_all", al=al)
        run_tstrnum_fd(raw_norm, only_jp=True, out_prefix=OUT_PREFIX_TSTR_JP, table_tag="tstr_jp", al=al)
    finally:
        raw.close()


if __name__ == "__main__":
    main()

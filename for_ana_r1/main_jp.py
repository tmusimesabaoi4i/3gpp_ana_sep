#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from table_sql import get, apply_pipeline
from table_rule import Pipeline
from normalization import normal
from table_al import TableAL


# ============================================================
# 設定
# ============================================================
CSV_PATH = "../ISLD-export/ISLD-export.csv"
DB_PATH = "work.sqlite"

GEN_COLS = ("3G", "4G", "5G")
HEADS = ["IPRD_ID", "COMP_LEGAL_NAME", "Ess_To_Standard", "Country_Of_Registration", "3G", "4G", "5G"]

JP_VALUE = "JP JAPAN"
OUT_PREFIX = "fd_jp"


# ============================================================
# 処理（共通）
# ============================================================
def build_filtered_table_jp(raw_norm, gen_col: str):
    """
    指定世代(gen_col)について、日本(JP)登録のみの分析対象テーブル（ユニーク化前）を作る。

    条件（AND）:
      - gen_col == 1
      - Ess_To_Standard == 1
      - Country_Of_Registration == "JP JAPAN"
    """
    pipeline = Pipeline()
    pipeline.where_eq(gen_col, 1)
    pipeline.where_eq("Ess_To_Standard", 1)
    pipeline.where_eq("Country_Of_Registration", JP_VALUE)

    return apply_pipeline(raw_norm, pipeline, out_table_name=f"t_flt_jp_{gen_col}", return_heads=HEADS)


def build_unique_table(filtered_tbl, gen_col: str):
    """IPRD_IDでユニーク化（最初の行を採用）"""
    filtered_tbl.create_index("IPRD_ID")  # 速くなることが多い

    pipeline = Pipeline()
    pipeline.unique_by("IPRD_ID")
    return apply_pipeline(filtered_tbl, pipeline, out_table_name=f"t_uq_jp_{gen_col}")


def save_frequency(al: TableAL, tbl_non_unique, tbl_unique, gen_col: str):
    """COMP_LEGAL_NAME の度数分布を（ユニーク化前/後）で保存"""
    labels_nu, counts_nu = al.frequency_distribution(tbl_non_unique, "COMP_LEGAL_NAME")
    labels_uq, counts_uq = al.frequency_distribution(tbl_unique, "COMP_LEGAL_NAME")

    al.save_as_file((labels_nu, counts_nu), f"{OUT_PREFIX}_{gen_col}_non_unique.csv")
    al.save_as_file((labels_uq, counts_uq), f"{OUT_PREFIX}_{gen_col}.csv")


def run_one_generation_jp(raw_norm, gen_col: str, al: TableAL) -> None:
    """
    1世代分の処理（JP限定）:
      1) フィルタ（gen & Ess & JP）
      2) ユニーク化（IPRD_ID）
      3) 度数分布保存
    """
    tbl_filtered = build_filtered_table_jp(raw_norm, gen_col)
    tbl_unique = build_unique_table(tbl_filtered, gen_col)
    save_frequency(al, tbl_filtered, tbl_unique, gen_col)


# ============================================================
# main
# ============================================================
def main() -> None:
    al = TableAL()

    raw = get(CSV_PATH, db_path=DB_PATH, head=HEADS)
    try:
        # 正規化は1回だけ
        raw_norm = normal(raw, out_table_name="t_norm")

        # 3G / 4G / 5G をまとめて実行
        for gen_col in GEN_COLS:
            run_one_generation_jp(raw_norm, gen_col, al)
    finally:
        raw.close()


if __name__ == "__main__":
    main()

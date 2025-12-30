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
CSV_PATH = "../../ISLD-export/ISLD-export.csv"
DB_PATH = "work.sqlite"

GEN_COLS = ("3G", "4G", "5G")

# CSVから読み込む列（効率化のため必要最小限）
HEADS = ["IPRD_ID", "Ess_To_Standard", "3GPP_Type", "TGPP_NUMBER", "Country_Of_Registration", "3G", "4G", "5G"]

# 連結設定: 3GPP_Type + "_" + TGPP_NUMBER -> TSTRNUM
# 例: "TS" + "_" + "36.331" -> "TS_36.331"
CONNECT_HEADS = ["3GPP_Type", "TGPP_NUMBER"]
NEW_HEAD = "TSTRNUM"  # 連結後の新しい列名（技術仕様番号）
SEP = "_"

JP_VALUE = "JP JAPAN"
OUT_PREFIX = "fd_tstr"

# 中間テーブルで保持する列（効率化と安定性のため明示的に指定）
# - IPRD_ID: ユニーク化のキー（宣言単位）
# - TSTRNUM: 度数分布の対象（技術仕様番号）
MINIMAL_HEADS = ["IPRD_ID", NEW_HEAD]

# ============================================================
# 処理（共通）
# ============================================================
def build_filtered_table(raw_norm, gen_col: str):
    """
    指定世代(gen_col)について分析対象テーブル（ユニーク化前）を作る。

    条件（AND）:
      - gen_col == 1
      - Ess_To_Standard == 1

    出力:
      - フィルタ済みテーブル（ユニーク化前）
    """
    pipeline = Pipeline()
    pipeline.where_eq(gen_col, 1)
    pipeline.where_eq("Ess_To_Standard", 1)

    return apply_pipeline(raw_norm, pipeline, out_table_name=f"t_flt_{gen_col}", return_heads=HEADS)


def build_connect_table(filtered_tbl, gen_col: str, return_heads=None):
    """
    連結列を作成する（3GPP_Type + "_" + TGPP_NUMBER -> TSTRNUM）
    
    例: "TS" + "_" + "36.331" -> "TS_36.331"
    
    引数:
        filtered_tbl: フィルタ済みテーブル
        gen_col: 世代列名（"3G", "4G", "5G"）
        return_heads: 出力する列（Noneの場合は最小限の列を自動選択）
    
    戻り値: 連結列を含むテーブル
    
    注意: return_heads を明示的に指定することで、
    - IPRD_ID の欠落を防ぐ（unique_by で必要）
    - 不要な列を削除してI/O効率を向上
    """
    if return_heads is None:
        # デフォルト: 後続処理に必要な最小限の列
        # - IPRD_ID: ユニーク化のキー
        # - TSTRNUM: 度数分布の対象（新規作成される列）
        return_heads = MINIMAL_HEADS
    
    pipeline = Pipeline()
    pipeline.concat(CONNECT_HEADS, NEW_HEAD, SEP)
    return apply_pipeline(
        filtered_tbl, 
        pipeline, 
        out_table_name=f"t_cn_jp_{gen_col}",
        return_heads=return_heads
    )

def build_unique_table(filtered_tbl, gen_col: str):
    """IPRD_IDでユニーク化（最初の行を採用）"""
    filtered_tbl.create_index("IPRD_ID")  # 速くなることが多い

    pipeline = Pipeline()
    pipeline.unique_by("IPRD_ID")
    return apply_pipeline(filtered_tbl, pipeline, out_table_name=f"t_uq_jp_{gen_col}")

def save_frequency(al: TableAL, tbl_non_unique, tbl_unique, gen_col: str):
    """COMP_LEGAL_NAME の度数分布を（ユニーク化前/後）で保存"""
    labels_nu, counts_nu = al.frequency_distribution(tbl_non_unique, NEW_HEAD)
    labels_uq, counts_uq = al.frequency_distribution(tbl_unique, NEW_HEAD)

    al.save_as_file((labels_nu, counts_nu), f"{OUT_PREFIX}_{gen_col}_non_unique.csv")
    al.save_as_file((labels_uq, counts_uq), f"{OUT_PREFIX}_{gen_col}.csv")


def run_one_generation_jp(raw_norm, gen_col: str, al: TableAL) -> None:
    """
    1世代分の処理（日本企業の標準必須特許を対象）
    
    処理フロー:
      1) フィルタ: 指定世代 & 標準必須 & 日本登録のレコードを抽出
         → t_flt_jp_{gen_col}
      
      2) 連結: 技術仕様番号を作成（3GPP_Type + "_" + TGPP_NUMBER）
         → t_cn_jp_{gen_col}
         例: "TS_36.331", "TR_38.912"
      
      3) ユニーク化: IPRD_ID（宣言単位）で重複排除
         → t_uq_jp_{gen_col}
         同じIPRD_IDが複数の技術仕様に宣言されている場合、最初の1つのみ採用
      
      4) 度数分布: TSTRNUM（技術仕様番号）の出現頻度を集計・保存
         - ユニーク化前: 全宣言を対象
         - ユニーク化後: IPRD_ID単位で重複排除後
    
    引数:
        raw_norm: 正規化済みテーブル
        gen_col: 世代列名（"3G", "4G", "5G"）
        al: TableAL インスタンス
    """
    tbl_filtered = build_filtered_table(raw_norm, gen_col)
    tbl_connect = build_connect_table(tbl_filtered, gen_col)
    tbl_unique = build_unique_table(tbl_connect, gen_col)
    save_frequency(al, tbl_connect, tbl_unique, gen_col)


# ============================================================
# main
# ============================================================
def main() -> None:
    """
    メイン処理: 日本企業の標準必須特許について、技術仕様番号の度数分布を生成
    
    処理概要:
      1) CSV読み込み（必要な列のみ）
      2) データ正規化（0/1列の統一、空白処理など）
      3) 各世代（3G/4G/5G）について:
         - 日本登録の標準必須特許を抽出
         - 技術仕様番号（TSTRNUM）を生成
         - IPRD_ID単位で重複排除
         - 度数分布を計算・保存
    
    出力ファイル:
      - fd_jp_tstr_3G_non_unique.csv / fd_jp_tstr_3G.csv
      - fd_jp_tstr_4G_non_unique.csv / fd_jp_tstr_4G.csv
      - fd_jp_tstr_5G_non_unique.csv / fd_jp_tstr_5G.csv
    """
    al = TableAL()

    raw = get(CSV_PATH, db_path=DB_PATH, head=HEADS)
    try:
        # 正規化は1回だけ（全世代で共有）
        raw_norm = normal(raw, out_table_name="t_norm")

        # 3G / 4G / 5G をまとめて実行
        for gen_col in GEN_COLS:
            run_one_generation_jp(raw_norm, gen_col, al)
    finally:
        raw.close()


if __name__ == "__main__":
    main()

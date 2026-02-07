#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from openpyxl import Workbook

# ----------------------------------------------------------------------
# ローカルライブラリパス（../std）を追加
# ----------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
LIB_DIR = (SCRIPT_DIR / ".." / "std").resolve()
sys.path.insert(0, str(LIB_DIR))

from table_sql import get, apply_pipeline
from table_rule import Pipeline
from normalization import normal
from table_al import TableAL


# ============================================================
# 入出力設定
# ============================================================
SOURCE_CSV = Path("../../ISLD-export/ISLD-export.csv")
SQLITE_DB = Path("work.sqlite")

OUT_DIR = Path("out_isld_fd_5g_only")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FD = OUT_DIR / "fd_global"
OUT_FD.mkdir(parents=True, exist_ok=True)

# ★ 5Gのみ
GEN_FLAGS = ("5G",)
GEN_SUFFIX = {"5G": "5g"}

SCOPE_ALL = "all"
SCOPE_JP = "jp"
JP_COUNTRY_VALUE = "JP JAPAN"

# 会社FDで使う unique キー
UNIQ_KEYS = ("IPRD_REFERENCE", "IPRD_ID", "DIPG_ID", "DIPG_PATF_ID")

# ============================================================
# SOURCE_COLUMNS（必要列）
# - 入力CSV読み込み負荷を下げるため、今回の処理に必要な列だけ
# ============================================================
SOURCE_COLUMNS = [
    "COMP_LEGAL_NAME",
    "Ess_To_Standard",
    "Ess_To_Project",
    "DIPG_PATF_ID",
    "Normalized_Patent",
    "Country_Of_Registration",
    "5G",
    "IPRD_REFERENCE",
    "IPRD_ID",
    "DIPG_ID",
]


# ============================================================
# Excel: シート名生成（31文字制限、禁則文字、重複）
# ============================================================
_INVALID_SHEET_CHARS = set(r'[]:*?/\\')

def _sanitize_sheet_title(title: str) -> str:
    s = "".join("_" if ch in _INVALID_SHEET_CHARS else ch for ch in title)
    s = s.strip()
    return s or "sheet"

def _make_unique_sheet_title(desired: str, used: set[str]) -> str:
    base = _sanitize_sheet_title(desired)[:31]
    if base not in used:
        used.add(base)
        return base
    n = 1
    while True:
        suffix = f"_{n}"
        cut = 31 - len(suffix)
        cand = (base[:cut] + suffix)[:31]
        if cand not in used:
            used.add(cand)
            return cand
        n += 1


def write_fd_to_workbook(
    wb: Workbook,
    sheet_csv_name: str,
    labels: list[Any],
    counts: list[Any],
    *,
    used_titles: set[str]
) -> None:
    title = _make_unique_sheet_title(sheet_csv_name, used_titles)
    ws = wb.create_sheet(title=title)
    ws.append(["COMP_LEGAL_NAME", "count"])
    for lab, cnt in zip(labels or [], counts or []):
        ws.append([lab, cnt])


# ============================================================
# (Ｂ) 会社FD（CSV + Excel）
#   - 抽出条件は一切入れない（全体データから）
#   - 5Gのみ（GEN_FLAGS=("5G",)）
# ============================================================
def is_jp(scope: str) -> bool:
    return scope == SCOPE_JP


def make_base_filtered_table(norm_tbl, *, scope: str, gen_flag: str, out_table: str, return_cols: list[str]):
    """
    条件（AND）:
      - gen_flag == 1
      - Ess_To_Standard == 1
      - Ess_To_Project == 1
      - DIPG_PATF_ID != -1
      - Normalized_Patent == 1
      - scope == "jp" の場合: Country_Of_Registration == "JP JAPAN"
    """
    p = Pipeline()
    p.where_eq(gen_flag, 1)
    p.where_eq("Ess_To_Standard", 1)
    p.where_eq("Ess_To_Project", 1)
    p.where_ne("DIPG_PATF_ID", -1)
    p.where_eq("Normalized_Patent", 1)
    if is_jp(scope):
        p.where_eq("Country_Of_Registration", JP_COUNTRY_VALUE)

    return apply_pipeline(norm_tbl, p, out_table_name=out_table, return_heads=return_cols)


def make_unique_by_key(tbl, *, uniq_key: str, out_table: str, return_cols: list[str]):
    try:
        tbl.create_index(uniq_key)
    except Exception:
        pass
    p = Pipeline().unique_by(uniq_key)
    return apply_pipeline(tbl, p, out_table_name=out_table, return_heads=return_cols)


def fd_filename(scope: str, gen_suffix: str, uniq_key: str) -> str:
    return f"fd_cmp__{scope}__{gen_suffix}__uniq-by_{uniq_key}.csv"


def run_global_company_fd_5g_only(norm_tbl, *, al: TableAL) -> None:
    cols_needed = ["COMP_LEGAL_NAME"] + list(UNIQ_KEYS) + [
        "Ess_To_Standard", "Ess_To_Project", "DIPG_PATF_ID", "Normalized_Patent",
        "Country_Of_Registration", "5G"
    ]

    # 5G用Excel（1本）
    wb = Workbook(write_only=True)
    used_titles: set[str] = set()

    for scope in (SCOPE_ALL, SCOPE_JP):
        for gen_flag in GEN_FLAGS:  # ← 5Gのみ
            gen_suffix = GEN_SUFFIX[gen_flag]

            # 1) 全体データに共通フィルタだけ
            t_filtered = make_base_filtered_table(
                norm_tbl,
                scope=scope,
                gen_flag=gen_flag,
                out_table=f"t_global__{scope}__flt__{gen_suffix}",
                return_cols=cols_needed,
            )

            # 2) uniqueキーごとに unique → 会社FD
            for uniq_key in UNIQ_KEYS:
                t_uq = make_unique_by_key(
                    t_filtered,
                    uniq_key=uniq_key,
                    out_table=f"t_global__{scope}__uq_{uniq_key}__{gen_suffix}",
                    return_cols=["COMP_LEGAL_NAME", uniq_key],
                )
                labels, counts = al.frequency_distribution(t_uq, "COMP_LEGAL_NAME")

                # CSV
                csv_name = fd_filename(scope, gen_suffix, uniq_key)
                out_csv = OUT_FD / csv_name
                al.save_as_file((labels, counts), str(out_csv))

                # Excel sheet（シート名＝CSV名、ただしExcel制限で短縮/重複回避）
                write_fd_to_workbook(
                    wb,
                    csv_name,
                    labels,
                    counts,
                    used_titles=used_titles,
                )

    # Excel保存（5Gのみなので1本）
    out_xlsx = OUT_FD / "fd_global__5g.xlsx"
    wb.save(out_xlsx)


# ============================================================
# main
# ============================================================
def main() -> None:
    al = TableAL()

    raw = get(str(SOURCE_CSV), db_path=str(SQLITE_DB), head=SOURCE_COLUMNS)
    try:
        norm_tbl = normal(raw, out_table_name="t_norm")
        run_global_company_fd_5g_only(norm_tbl, al=al)
    finally:
        raw.close()


if __name__ == "__main__":
    main()

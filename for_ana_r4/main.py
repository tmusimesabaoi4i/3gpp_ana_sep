#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# make_base_filtered_tableを新しくした

"""
ISLD 統合分析スクリプト（get/normal は 1 回だけ）

要点
- DIPG_PATF_ID でユニーク化
- TS/TR番号（TSTRNUM=3GPP_Type + "_" + TGPP_NUMBER）でFDを作成
- fd_tno_<scope>_<gen>_uq の上位N件（labels）を allowlist に自動格納
- allowlist を用いて「特定TS/TRのみ」の会社FD（cmp）も作成

実行順（重要）
1) run_tstrnum_fd() を実行して allowlist を自動生成
2) run_company_fd_for_selected_tstr() が allowlist を使ってTS/TR限定の会社FDを生成
"""


from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Iterable

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

# 入力CSV側の世代フラグ列 / 命名用キー
GEN_FLAGS = ("3G", "4G", "5G")
GEN_SUFFIX = {"3G": "3g", "4G": "4g", "5G": "5g"}

# 必要列の「和集合」だけ読む（I/O最小化）
SOURCE_COLUMNS = [
    "IPRD_ID",
    "IPRD_SIGNATURE_DATE",
    "COMP_LEGAL_NAME",
    "Ess_To_Standard",
    "3GPP_Type",
    "TGPP_NUMBER",
    "Country_Of_Registration",
    "Ess_To_Project",
    "DIPG_PATF_ID",
    "Normalized_Patent",
    "3G",
    "4G",
    "5G",
]

# scope
SCOPE_ALL = "all"
SCOPE_JP = "jp"

# JP判定
JP_COUNTRY_VALUE = "JP JAPAN"

# TS/TR連結列（技術仕様番号）
TSTRNUM_PART_COLS = ("3GPP_Type", "TGPP_NUMBER")
TSTRNUM_COL = "TSTRNUM"
TSTRNUM_SEP = "_"

# allowlist を適用する世代（必要なら絞る：例 ("5G",)）
ALLOWLIST_TARGET_GENS = ("3G", "4G", "5G")

# fd_tno_*_uq の「上位N」を allowlist に入れる
TOP_N_TSTRNUM = 10


# ============================================================
# allowlist（3G/4G/5G × ALL/JP の 6 通り）
# - ここは固定値を入れる場所でもあるが、今回は「上位Nで自動上書き」運用
# ============================================================
TSTR_ALLOWLIST_BY_GEN_SCOPE: dict[tuple[str, str], list[str]] = {
    (g, SCOPE_ALL): []
    for g in GEN_FLAGS
}
TSTR_ALLOWLIST_BY_GEN_SCOPE.update({
    (g, SCOPE_JP): []
    for g in GEN_FLAGS
})


# ============================================================
# 命名ヘルパ
# ============================================================
def fd_filename(dim: str, scope: str, gen_suffix: str, uniq_tag: str, *, tag: str | None = None) -> str:
    """度数分布CSVの保存ファイル名を組み立てる。"""
    base = f"fd_{dim}_{scope}_{gen_suffix}_{uniq_tag}"
    return f"{base}_{tag}.csv" if tag else f"{base}.csv"


def tmp_table_name(dim: str, scope: str, step: str, gen_suffix: str, *, tag: str | None = None) -> str:
    """中間テーブル名（SQLite等で衝突しにくい）。"""
    base = f"t_{dim}_{scope}_{step}_{gen_suffix}"
    return f"{base}_{tag}" if tag else base


def is_jp(scope: str) -> bool:
    return scope == SCOPE_JP


# ============================================================
# allowlist 取得/更新
# ============================================================
def allowlist_ref(gen_flag: str, scope: str) -> list[str]:
    """
    (gen_flag, scope) に対応する allowlist の「参照」を返す。
    参照を返すため、ref[:] = ... で同一オブジェクトのまま更新できる。
    """
    if gen_flag not in GEN_FLAGS:
        return []
    if scope not in (SCOPE_ALL, SCOPE_JP):
        return []
    return TSTR_ALLOWLIST_BY_GEN_SCOPE.get((gen_flag, scope), [])


def allowlist_value(gen_flag: str, scope: str) -> list[str]:
    """読み取り用途（実体は参照と同じ）。"""
    return allowlist_ref(gen_flag, scope)


def _to_int(x: Any) -> int:
    """countsがstr/floatでも安全に数値化する。"""
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0


def top_labels_by_count(labels: list[str], counts: list[Any], top_n: int) -> list[str]:
    """
    labels/counts から count 降順で上位 top_n の labels を返す。
    """
    pairs = [(lab, _to_int(cnt)) for lab, cnt in zip(labels or [], counts or []) if lab is not None]
    pairs.sort(key=lambda t: t[1], reverse=True)
    return [lab for lab, _cnt in pairs[:top_n]]


def refresh_allowlist_from_fd(gen_flag: str, scope: str, labels_uq: list[str], counts_uq: list[Any], top_n: int) -> None:
    """
    fd_tno_<scope>_<gen>_uq の結果（labels_uq, counts_uq）から
    上位 top_n の labels を抽出して、対応 allowlist を上書き更新する。
    """
    ref = allowlist_ref(gen_flag, scope)
    if ref is None:
        return
    ref[:] = top_labels_by_count(labels_uq, counts_uq, top_n)


# ============================================================
# テーブル生成（フィルタ/連結/ユニーク）
# ============================================================
def make_base_filtered_table(
    norm_tbl,
    *,
    scope: str,
    gen_flag: str,
    out_table: str,
    return_cols: list[str],
):
    """
    共通の基本フィルタを適用したテーブル（ユニーク化前）を作る。

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
    # p.where_between("IPRD_SIGNATURE_DATE", "2017-01-01", "2025-12-15")
    p.where_eq("Ess_To_Standard", 1)
    p.where_eq("Ess_To_Project", 1)
    p.where_ne("DIPG_PATF_ID", -1)
    p.where_eq("Normalized_Patent", 1)
    if is_jp(scope):
        p.where_eq("Country_Of_Registration", JP_COUNTRY_VALUE)

    return apply_pipeline(norm_tbl, p, out_table_name=out_table, return_heads=return_cols)


def make_unique_by_patf_id(tbl, *, out_table: str, return_cols: list[str] | None = None):
    """DIPG_PATF_ID でユニーク化したテーブル（ユニーク化後）を作る。"""
    tbl.create_index("DIPG_PATF_ID")  # 効くことが多い
    p = Pipeline().unique_by("DIPG_PATF_ID")
    return apply_pipeline(tbl, p, out_table_name=out_table, return_heads=return_cols)


def add_tstrnum_col(tbl, *, out_table: str, return_cols: list[str]):
    """TSTRNUM（3GPP_Type + '_' + TGPP_NUMBER）を生成する。"""
    p = Pipeline().concat(TSTRNUM_PART_COLS, TSTRNUM_COL, TSTRNUM_SEP)
    return apply_pipeline(tbl, p, out_table_name=out_table, return_heads=return_cols)


def filter_by_tstr_allowlist(tbl_with_tstr, *, allowlist: list[str], out_table: str, return_cols: list[str]):
    """
    TSTRNUM が allowlist に含まれる行だけに絞り込む。
    allowlist が空なら「絞り込み無し」（安全側）として元テーブルをそのまま返す。
    """
    if not allowlist:
        return tbl_with_tstr

    p = Pipeline().where_in(TSTRNUM_COL, allowlist)
    return apply_pipeline(tbl_with_tstr, p, out_table_name=out_table, return_heads=return_cols)


# ============================================================
# 保存（度数分布）
# ============================================================
def save_fd_csv(
    al: TableAL,
    *,
    labels_nu: list[str],
    counts_nu: list[Any],
    labels_uq: list[str],
    counts_uq: list[Any],
    dim: str,
    scope: str,
    gen_suffix: str,
    tag: str | None = None,
):
    """度数分布（labels/counts）を（ユニーク前/後）でCSV保存する。"""
    al.save_as_file((labels_nu, counts_nu), fd_filename(dim, scope, gen_suffix, "nu", tag=tag))
    al.save_as_file((labels_uq, counts_uq), fd_filename(dim, scope, gen_suffix, "uq", tag=tag))


# ============================================================
# ジョブA: 会社別FD（cmp）
# ============================================================
def run_company_fd(norm_tbl, *, scope: str, al: TableAL) -> None:
    """cmp（COMP_LEGAL_NAME）の度数分布を作る（3g/4g/5gをまとめて実行）。"""
    dim = "cmp"
    cols_needed = ["DIPG_PATF_ID", "COMP_LEGAL_NAME"]

    for gen_flag in GEN_FLAGS:
        gen_suffix = GEN_SUFFIX[gen_flag]

        t_filtered = make_base_filtered_table(
            norm_tbl,
            scope=scope,
            gen_flag=gen_flag,
            out_table=tmp_table_name(dim, scope, "flt", gen_suffix),
            return_cols=cols_needed,
        )

        t_unique = make_unique_by_patf_id(
            t_filtered,
            out_table=tmp_table_name(dim, scope, "uq", gen_suffix),
            return_cols=cols_needed,
        )

        labels_nu, counts_nu = al.frequency_distribution(t_filtered, "COMP_LEGAL_NAME")
        labels_uq, counts_uq = al.frequency_distribution(t_unique,  "COMP_LEGAL_NAME")

        save_fd_csv(
            al,
            labels_nu=labels_nu, counts_nu=counts_nu,
            labels_uq=labels_uq, counts_uq=counts_uq,
            dim=dim, scope=scope, gen_suffix=gen_suffix,
        )


# ============================================================
# ジョブB: TS/TR番号別FD（tno） + 上位Nラベルを allowlist に格納
# ============================================================
def run_tstrnum_fd(norm_tbl, *, scope: str, al: TableAL) -> None:
    """
    tno（TSTRNUM）の度数分布を作る（3g/4g/5gをまとめて実行）。

    追加:
    - fd_tno_<scope>_<gen>_uq の「上位Nラベル（TSTRNUM）」を allowlist に格納する。
    """
    dim = "tno"
    cols_for_concat = ["DIPG_PATF_ID", "3GPP_Type", "TGPP_NUMBER"]

    for gen_flag in GEN_FLAGS:
        gen_suffix = GEN_SUFFIX[gen_flag]

        # 1) 基本フィルタ
        t_filtered = make_base_filtered_table(
            norm_tbl,
            scope=scope,
            gen_flag=gen_flag,
            out_table=tmp_table_name(dim, scope, "flt", gen_suffix),
            return_cols=cols_for_concat,
        )

        # 2) TSTRNUM生成（concat）
        t_with_tstr = add_tstrnum_col(
            t_filtered,
            out_table=tmp_table_name(dim, scope, "cat", gen_suffix),
            return_cols=["DIPG_PATF_ID", TSTRNUM_COL],
        )

        # 3) DIPG_PATF_IDでユニーク化
        t_unique = make_unique_by_patf_id(
            t_with_tstr,
            out_table=tmp_table_name(dim, scope, "uq", gen_suffix),
            return_cols=["DIPG_PATF_ID", TSTRNUM_COL],
        )

        # 4) FD（ユニーク前/後）
        labels_nu, counts_nu = al.frequency_distribution(t_with_tstr, TSTRNUM_COL)
        labels_uq, counts_uq = al.frequency_distribution(t_unique,    TSTRNUM_COL)

        save_fd_csv(
            al,
            labels_nu=labels_nu, counts_nu=counts_nu,
            labels_uq=labels_uq, counts_uq=counts_uq,
            dim=dim, scope=scope, gen_suffix=gen_suffix,
        )

        # 5) ★上位Nラベルを allowlist に反映（labels側のみ）
        refresh_allowlist_from_fd(gen_flag, scope, labels_uq, counts_uq, TOP_N_TSTRNUM)


# ============================================================
# ジョブC: 会社別FD（cmp）を「特定TS/TR番号だけ」に限定して集計
# ============================================================
def run_company_fd_for_selected_tstr(norm_tbl, *, scope: str, al: TableAL) -> None:
    """
    cmp（COMP_LEGAL_NAME）の度数分布を作るが、先に TSTRNUM allowlist で行を絞り込む。

    allowlist は run_tstrnum_fd() により自動生成されたものを使用する。

    フロー:
      1) 基本フィルタ
      2) TSTRNUM生成
      3) allowlist 絞り込み（where_in）
      4) DIPG_PATF_IDでユニーク化
      5) 会社名FDを保存（tag="tstr" を付与）
    """
    dim = "cmp"
    tag = "tstr"

    cols_needed = ["DIPG_PATF_ID", "COMP_LEGAL_NAME", "3GPP_Type", "TGPP_NUMBER"]

    for gen_flag in GEN_FLAGS:
        if gen_flag not in ALLOWLIST_TARGET_GENS:
            continue

        gen_suffix = GEN_SUFFIX[gen_flag]
        allowlist = allowlist_value(gen_flag, scope)

        # 1) 基本フィルタ
        t_filtered = make_base_filtered_table(
            norm_tbl,
            scope=scope,
            gen_flag=gen_flag,
            out_table=tmp_table_name(dim, scope, "flt", gen_suffix, tag=tag),
            return_cols=cols_needed,
        )

        # 2) TSTRNUM生成
        t_with_tstr = add_tstrnum_col(
            t_filtered,
            out_table=tmp_table_name(dim, scope, "cat", gen_suffix, tag=tag),
            return_cols=["DIPG_PATF_ID", "COMP_LEGAL_NAME", TSTRNUM_COL],
        )

        # 3) allowlist 絞り込み（空なら絞り込み無し）
        t_selected = filter_by_tstr_allowlist(
            t_with_tstr,
            allowlist=allowlist,
            out_table=tmp_table_name(dim, scope, "sel", gen_suffix, tag=tag),
            return_cols=["DIPG_PATF_ID", "COMP_LEGAL_NAME", TSTRNUM_COL],
        )

        # 4) DIPG_PATF_IDでユニーク化
        t_unique = make_unique_by_patf_id(
            t_selected,
            out_table=tmp_table_name(dim, scope, "uq", gen_suffix, tag=tag),
            return_cols=["DIPG_PATF_ID", "COMP_LEGAL_NAME"],
        )

        # 5) FD
        labels_nu, counts_nu = al.frequency_distribution(t_selected, "COMP_LEGAL_NAME")
        labels_uq, counts_uq = al.frequency_distribution(t_unique,  "COMP_LEGAL_NAME")

        save_fd_csv(
            al,
            labels_nu=labels_nu, counts_nu=counts_nu,
            labels_uq=labels_uq, counts_uq=counts_uq,
            dim=dim, scope=scope, gen_suffix=gen_suffix, tag=tag,
        )


# ============================================================
# main（get + normal は1回だけ）
# ============================================================
def main() -> None:
    al = TableAL()

    raw = get(str(SOURCE_CSV), db_path=str(SQLITE_DB), head=SOURCE_COLUMNS)
    try:
        norm_tbl = normal(raw, out_table_name="t_norm")

        # A) 会社別FD（通常）
        run_company_fd(norm_tbl, scope=SCOPE_ALL, al=al)
        run_company_fd(norm_tbl, scope=SCOPE_JP,  al=al)

        # B) TS/TR番号別FD（通常）＋ allowlist 自動生成（上位N）
        run_tstrnum_fd(norm_tbl, scope=SCOPE_ALL, al=al)
        run_tstrnum_fd(norm_tbl, scope=SCOPE_JP,  al=al)

        # C) 会社別FD（特定TS/TR番号のみ：allowlistを使用）
        run_company_fd_for_selected_tstr(norm_tbl, scope=SCOPE_ALL, al=al)
        run_company_fd_for_selected_tstr(norm_tbl, scope=SCOPE_JP,  al=al)

    finally:
        raw.close()


if __name__ == "__main__":
    main()
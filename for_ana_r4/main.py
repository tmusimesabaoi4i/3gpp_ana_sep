#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# DIPG_PATF_IDでユニーク化

"""
統合分析スクリプト（get/normal は 1 回だけ）

追加仕様（今回の変更点）
- fd_tno_<scope>_<gen>_uq（= TSTRNUM のユニーク後FD）について
  「上位10件のラベル（TSTRNUM）」を自動抽出し、
  6通り（3G/4G/5G × ALL/JP）の allowlist（TSTR_ALLOWLIST_MAP）へ格納する。
  ※「ラベル側ね」＝ counts ではなく labels を入れる。

動作順序（重要）
1) run_tstrnum_fd() を実行して fd_tno_* を生成 + allowlist を自動生成
2) run_company_fd_for_selected_tstr() が allowlist を使って TS/TR限定の会社FDを生成
"""

from __future__ import annotations

import sys
import os

# このファイルのパスを基準に、../std をパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
std_path = os.path.abspath(os.path.join(current_dir, '..', 'std'))
sys.path.insert(0, std_path)

from table_sql import get, apply_pipeline
from table_rule import Pipeline
from normalization import normal
from table_al import TableAL

# ============================================================
# 入出力設定
# ============================================================
CSV_PATH = "../../ISLD-export/ISLD-export.csv"
DB_PATH = "work.sqlite"

GEN_COLS = ("3G", "4G", "5G")                     # 入力CSV側の世代フラグ列
GEN_KEY  = {"3G": "3g", "4G": "4g", "5G": "5g"}   # 命名用キー

# 必要列の「和集合」だけ読み込む（I/O最小化）
HEADS_ALL = [
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

# JP判定
JP_VALUE = "JP JAPAN"

# TS/TR 連結列（技術仕様番号）
TSTR_CONNECT_HEADS = ["3GPP_Type", "TGPP_NUMBER"]
TSTR_COL = "TSTRNUM"
TSTR_SEP = "_"

# scope 値（固定）
SCOPE_ALL = "all"
SCOPE_JP  = "jp"


# ============================================================
# TS/TR 限定（allowlist）設定：3G/4G/5G × (ALL/JP) の6通り
# ============================================================
# ここは「固定値を入れる場所」でもあるが、
# 今回は「fd_tno_*_uq の上位10ラベルで自動的に上書きする」運用にする。
#
# - 文字列は必ず "TS_38.331" のような TSTRNUM 形式。
# - 空リストなら「絞り込み無し」になるが、今回の自動生成で基本は埋まる。

# --- 3G（ALL/JP）---
TSTR_ALLOWLIST_ALL_3G: list[str] = []
TSTR_ALLOWLIST_JP_3G:  list[str] = []

# --- 4G（ALL/JP）---
TSTR_ALLOWLIST_ALL_4G: list[str] = []
TSTR_ALLOWLIST_JP_4G:  list[str] = []

# --- 5G（ALL/JP）---
TSTR_ALLOWLIST_ALL_5G: list[str] = []
TSTR_ALLOWLIST_JP_5G:  list[str] = []

# 内部マップ（6通りを確実に返せるようにする）
TSTR_ALLOWLIST_MAP: dict[tuple[str, str], list[str]] = {
    ("3G", SCOPE_ALL): TSTR_ALLOWLIST_ALL_3G,
    ("3G", SCOPE_JP):  TSTR_ALLOWLIST_JP_3G,
    ("4G", SCOPE_ALL): TSTR_ALLOWLIST_ALL_4G,
    ("4G", SCOPE_JP):  TSTR_ALLOWLIST_JP_4G,
    ("5G", SCOPE_ALL): TSTR_ALLOWLIST_ALL_5G,
    ("5G", SCOPE_JP):  TSTR_ALLOWLIST_JP_5G,
}

# allowlist を適用する世代（必要なら絞る：例 ("5G",)）
TSTR_FILTER_GEN_COLS = ("3G", "4G", "5G")

# fd_tno_*_uq の「上位N」を allowlist に入れるN
TOP_N_TSTRNUM = 10


# ============================================================
# 命名ヘルパ
# ============================================================
def build_fd_filename(dim: str, scope: str, gen: str, uniq: str, *, tag: str | None = None) -> str:
    """度数分布CSVの保存ファイル名を組み立てる。"""
    base = f"fd_{dim}_{scope}_{gen}_{uniq}"
    return f"{base}_{tag}.csv" if tag else f"{base}.csv"


def build_table_name(dim: str, scope: str, step: str, gen: str, *, tag: str | None = None) -> str:
    """中間テーブル名を組み立てる（SQLite等で衝突しにくい）。"""
    base = f"t_{dim}_{scope}_{step}_{gen}"
    return f"{base}_{tag}" if tag else base


def is_jp_scope(scope: str) -> bool:
    return scope == SCOPE_JP


# ============================================================
# allowlist 取得/更新（6通り対応）
# ============================================================
def get_tstr_allowlist_ref(gen_col: str, scope: str) -> list[str]:
    """
    gen_col（"3G"/"4G"/"5G"）と scope（"all"/"jp"）に応じて
    allowlist の「参照（listオブジェクト）」を返す。

    - 参照を返すので、呼び出し側で allowlist[:] = ... のように更新できる
    - 想定外入力は空リスト（新規のダミー）を返す（落とさない）
    """
    if gen_col not in GEN_COLS:
        return []
    if scope not in (SCOPE_ALL, SCOPE_JP):
        return []
    return TSTR_ALLOWLIST_MAP.get((gen_col, scope), [])


def get_tstr_allowlist(gen_col: str, scope: str) -> list[str]:
    """参照ではなく値として使う（読み取り用途）。"""
    return get_tstr_allowlist_ref(gen_col, scope)


def _to_int(x) -> int:
    """countsがstrでも安全に数値化する。"""
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0


def pick_top_labels(labels: list[str], counts: list, top_n: int) -> list[str]:
    """
    labels/counts から count 降順で上位 top_n の labels を返す。
    - labels が None/空でも落ちない
    """
    pairs = [(lab, _to_int(cnt)) for lab, cnt in zip(labels or [], counts or []) if lab is not None]
    pairs.sort(key=lambda t: t[1], reverse=True)
    return [lab for lab, _cnt in pairs[:top_n]]


def update_tstr_allowlist_from_fd(gen_col: str, scope: str, labels_uq: list[str], counts_uq: list, top_n: int) -> None:
    """
    fd_tno_<scope>_<gen>_uq の結果（labels_uq, counts_uq）から
    上位 top_n の labels を抽出して、対応する allowlist に格納する（上書き）。
    """
    ref = get_tstr_allowlist_ref(gen_col, scope)
    if ref is None:
        return
    top_labels = pick_top_labels(labels_uq, counts_uq, top_n)
    # 参照先リストを「同一オブジェクトのまま」更新する（マップ参照を壊さない）
    ref[:] = top_labels


# ============================================================
# 共有処理（読み込み/正規化）
# ============================================================
def load_source_table():
    """CSVを必要列だけ読み込む（get）。"""
    return get(CSV_PATH, db_path=DB_PATH, head=HEADS_ALL)


def normalize_table(raw):
    """正規化（normal）。"""
    return normal(raw, out_table_name="t_norm")


# ============================================================
# テーブル生成（フィルタ/連結/ユニーク）
# ============================================================
def build_filtered_table(raw_norm, *, scope: str, gen_col: str, out_table: str, return_heads: list[str]):
    """
    共通の基本フィルタを適用したテーブル（ユニーク化前）を作る。

    条件（AND）:
      - gen_col == 1
      - Ess_To_Standard == 1
      - (scope == "jp") の場合: Country_Of_Registration == "JP JAPAN"
    """
    p = Pipeline()
    p.where_eq(gen_col, 1)
    p.where_eq("Ess_To_Standard", 1)
    p.where_eq("Ess_To_Project", 1)
    p.where_ne("DIPG_PATF_ID", -1)
    p.where_eq("Normalized_Patent", 1)
    if is_jp_scope(scope):
        p.where_eq("Country_Of_Registration", JP_VALUE)

    return apply_pipeline(raw_norm, p, out_table_name=out_table, return_heads=return_heads)


def build_unique_by_DIPG_PATF_ID(tbl, *, out_table: str, return_heads: list[str] | None = None):
    """DIPG_PATF_IDでユニーク化したテーブル（ユニーク化後）を作る。"""
    tbl.create_index("DIPG_PATF_ID")  # 速くなることが多い
    p = Pipeline()
    p.unique_by("DIPG_PATF_ID")
    return apply_pipeline(tbl, p, out_table_name=out_table, return_heads=return_heads)


def build_tstrnum_table(tbl, *, out_table: str, return_heads: list[str]):
    """TSTRNUMを作る（3GPP_Type + '_' + TGPP_NUMBER）。"""
    p = Pipeline()
    p.concat(TSTR_CONNECT_HEADS, TSTR_COL, TSTR_SEP)
    return apply_pipeline(tbl, p, out_table_name=out_table, return_heads=return_heads)


def build_tstrnum_allowlist_table(tbl_with_tstr, *, allowlist: list[str], out_table: str, return_heads: list[str]):
    """
    TSTRNUMが allowlist に含まれる行だけに絞り込む。

    注意:
    - allowlist が空の場合は「絞り込み無し」（安全側）として、元テーブルをそのまま返す。
    """
    if not allowlist:
        return tbl_with_tstr

    p = Pipeline()
    p.where_in(TSTR_COL, allowlist)
    return apply_pipeline(tbl_with_tstr, p, out_table_name=out_table, return_heads=return_heads)


# ============================================================
# 保存（度数分布）
# ============================================================
def save_frequency_distribution_csv(
    al: TableAL,
    *,
    labels_nu: list[str],
    counts_nu: list,
    labels_uq: list[str],
    counts_uq: list,
    dim: str,
    scope: str,
    gen: str,
    tag: str | None = None,
):
    """度数分布（labels/counts）を（ユニーク前/後）でCSV保存する。"""
    al.save_as_file((labels_nu, counts_nu), build_fd_filename(dim, scope, gen, "nu", tag=tag))
    al.save_as_file((labels_uq, counts_uq), build_fd_filename(dim, scope, gen, "uq", tag=tag))


# ============================================================
# ジョブA: 会社別FD（cmp）
# ============================================================
def run_company_fd(raw_norm, *, scope: str, al: TableAL) -> None:
    """cmp（COMP_LEGAL_NAME）の度数分布を作る（3g/4g/5gをまとめて実行）。"""
    dim = "cmp"
    heads_needed = ["DIPG_PATF_ID", "COMP_LEGAL_NAME"]

    for gen_col in GEN_COLS:
        gen = GEN_KEY[gen_col]

        t_filtered = build_filtered_table(
            raw_norm,
            scope=scope,
            gen_col=gen_col,
            out_table=build_table_name(dim, scope, "flt", gen),
            return_heads=heads_needed,
        )

        t_unique = build_unique_by_DIPG_PATF_ID(
            t_filtered,
            out_table=build_table_name(dim, scope, "uq", gen),
            return_heads=heads_needed,
        )

        labels_nu, counts_nu = al.frequency_distribution(t_filtered, "COMP_LEGAL_NAME")
        labels_uq, counts_uq = al.frequency_distribution(t_unique,  "COMP_LEGAL_NAME")

        save_frequency_distribution_csv(
            al,
            labels_nu=labels_nu, counts_nu=counts_nu,
            labels_uq=labels_uq, counts_uq=counts_uq,
            dim=dim, scope=scope, gen=gen,
        )


# ============================================================
# ジョブB: TS/TR番号別FD（tno） + 上位10ラベルを allowlist に格納
# ============================================================
def run_tstrnum_fd(raw_norm, *, scope: str, al: TableAL) -> None:
    """
    tno（TSTRNUM）の度数分布を作る（3g/4g/5gをまとめて実行）。

    追加:
    - fd_tno_<scope>_<gen>_uq の「上位10ラベル（TSTRNUM）」を
      TSTR_ALLOWLIST_MAP[(gen_col, scope)] に格納する。
    """
    dim = "tno"
    heads_for_concat = ["DIPG_PATF_ID", "3GPP_Type", "TGPP_NUMBER"]

    for gen_col in GEN_COLS:
        gen = GEN_KEY[gen_col]

        # まず基本フィルタ
        t_filtered = build_filtered_table(
            raw_norm,
            scope=scope,
            gen_col=gen_col,
            out_table=build_table_name(dim, scope, "flt", gen),
            return_heads=heads_for_concat,
        )

        # TSTRNUM生成（concat）
        t_with_tstr = build_tstrnum_table(
            t_filtered,
            out_table=build_table_name(dim, scope, "cat", gen),
            return_heads=["DIPG_PATF_ID", TSTR_COL],
        )

        # DIPG_PATF_IDでユニーク化
        t_unique = build_unique_by_DIPG_PATF_ID(
            t_with_tstr,
            out_table=build_table_name(dim, scope, "uq", gen),
            return_heads=["DIPG_PATF_ID", TSTR_COL],
        )

        # FD（ユニーク前/後）
        labels_nu, counts_nu = al.frequency_distribution(t_with_tstr, TSTR_COL)
        labels_uq, counts_uq = al.frequency_distribution(t_unique,    TSTR_COL)

        save_frequency_distribution_csv(
            al,
            labels_nu=labels_nu, counts_nu=counts_nu,
            labels_uq=labels_uq, counts_uq=counts_uq,
            dim=dim, scope=scope, gen=gen,
        )

        # ★ ここが今回の要件：
        # fd_tno_<scope>_<gen>_uq の上位10ラベルを allowlist に入れる
        update_tstr_allowlist_from_fd(gen_col, scope, labels_uq, counts_uq, TOP_N_TSTRNUM)


# ============================================================
# ジョブC: 会社別FD（cmp）を「特定TS/TR番号だけ」に限定して集計
# ============================================================
def run_company_fd_for_selected_tstr(raw_norm, *, scope: str, al: TableAL) -> None:
    """
    cmp（COMP_LEGAL_NAME）の度数分布を作るが、先に TSTRNUM allowlist で行を絞り込む。

    allowlist は run_tstrnum_fd() により自動生成された
    TSTR_ALLOWLIST_MAP[(gen_col, scope)] を使用する。

    フロー:
      1) 基本フィルタ（gen=1 & ess=1 [& jp]）
      2) TSTRNUM生成（concat）
      3) allowlist で絞り込み（where_in）
      4) DIPG_PATF_IDでユニーク化
      5) 会社名FDを保存（通常版と区別するため tag="tstr" を付与）
    """
    dim = "cmp"
    tag = "tstr"

    # concat するので、Type/Number も必要
    heads_needed = ["DIPG_PATF_ID", "COMP_LEGAL_NAME", "3GPP_Type", "TGPP_NUMBER"]

    for gen_col in GEN_COLS:
        if gen_col not in TSTR_FILTER_GEN_COLS:
            continue

        gen = GEN_KEY[gen_col]
        allowlist = get_tstr_allowlist(gen_col, scope)

        # 1) 基本フィルタ
        t_filtered = build_filtered_table(
            raw_norm,
            scope=scope,
            gen_col=gen_col,
            out_table=build_table_name(dim, scope, "flt", gen, tag=tag),
            return_heads=heads_needed,
        )

        # 2) TSTRNUM生成（concat）
        t_with_tstr = build_tstrnum_table(
            t_filtered,
            out_table=build_table_name(dim, scope, "cat", gen, tag=tag),
            return_heads=["DIPG_PATF_ID", "COMP_LEGAL_NAME", TSTR_COL],
        )

        # 3) allowlist で絞り込み（空なら絞り込み無し）
        t_selected = build_tstrnum_allowlist_table(
            t_with_tstr,
            allowlist=allowlist,
            out_table=build_table_name(dim, scope, "sel", gen, tag=tag),
            return_heads=["DIPG_PATF_ID", "COMP_LEGAL_NAME", TSTR_COL],
        )

        # 4) DIPG_PATF_IDでユニーク化
        t_unique = build_unique_by_DIPG_PATF_ID(
            t_selected,
            out_table=build_table_name(dim, scope, "uq", gen, tag=tag),
            return_heads=["DIPG_PATF_ID", "COMP_LEGAL_NAME"],
        )

        # 5) 会社名FD
        labels_nu, counts_nu = al.frequency_distribution(t_selected, "COMP_LEGAL_NAME")
        labels_uq, counts_uq = al.frequency_distribution(t_unique,  "COMP_LEGAL_NAME")

        save_frequency_distribution_csv(
            al,
            labels_nu=labels_nu, counts_nu=counts_nu,
            labels_uq=labels_uq, counts_uq=counts_uq,
            dim=dim, scope=scope, gen=gen, tag=tag,
        )


# ============================================================
# main（get + normal は1回だけ）
# ============================================================
def main() -> None:
    al = TableAL()
    raw = get(CSV_PATH, db_path=DB_PATH, head=HEADS_ALL)
    try:
        raw_norm = normal(raw, out_table_name="t_norm")

        # A) 会社別FD（通常）
        run_company_fd(raw_norm, scope=SCOPE_ALL, al=al)
        run_company_fd(raw_norm, scope=SCOPE_JP,  al=al)

        # B) TS/TR番号別FD（通常）＋ allowlist 自動生成（上位10）
        run_tstrnum_fd(raw_norm, scope=SCOPE_ALL, al=al)
        run_tstrnum_fd(raw_norm, scope=SCOPE_JP,  al=al)

        # C) 会社別FD（特定TS/TR番号のみ：allowlistを使用）
        run_company_fd_for_selected_tstr(raw_norm, scope=SCOPE_ALL, al=al)
        run_company_fd_for_selected_tstr(raw_norm, scope=SCOPE_JP,  al=al)
    finally:
        raw.close()


if __name__ == "__main__":
    main()

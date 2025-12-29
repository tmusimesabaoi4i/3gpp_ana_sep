from table_sql import get, build_filter_sql, build_unique_sql, apply_plan
from table_rule import Rule
from normalization import normal
from table_al import TableAL

# ID COMP N1 N2 N3 GE GO GAというtableがcsvにあります。
# ID COMP N1 N2 N3 GEのみを読み込み、
# GEが1のみにフィルタし、
# N1 N2 N3を_で連結し、N4を返し、
# IDでユニーク化して、
# COMPで度数分布を作成し、
# N4で度数分布を作成するサンプルを書いて

# ============================================================
# 目的:
# - CSVから必要列だけ読み込み（高速化）
# - 正規化は1回だけ
# - GE==1 にフィルタ
# - N1/N2/N3 を "_" で連結して派生列 N4 を作る
# - IDでユニーク化
# - COMP と N4 の度数分布を作って保存
# ============================================================

CSV_PATH = "input.csv"
DB_PATH = "work.sqlite"

# 必要列だけロード（GB級でも効く）
HEADS = ["ID", "COMP", "N1", "N2", "N3", "GE"]

al = TableAL()

# 1) raw作成（必要列のみ）
raw = get(CSV_PATH, db_path=DB_PATH, head=HEADS)

try:
    # 2) 正規化は1回だけ
    t_norm = normal(raw, out_table_name="t_norm")

    # 3) GE==1 で抽出しつつ、N4 = N1_N2_N3 を生成（1回のSQLでやる＝速い）
    r = Rule(return_heads=["ID", "COMP", "N4"])
    r.add_filter_eq("GE", 1)
    r.add_derive_concat(["N1", "N2", "N3"], "N4", "_")

    t_filtered = apply_plan(t_norm, build_filter_sql(r), out_table_name="t_filtered")

    # 4) IDでユニーク化（速くするため index）
    t_filtered.create_index("ID")
    t_unique = apply_plan(
        t_filtered,
        build_unique_sql(Rule(unique_head="ID", return_heads=["ID", "COMP", "N4"])),
        out_table_name="t_unique",
    )

    # 5) 度数分布（COMP / N4）
    comp_labels, comp_counts = al.frequency_distribution(t_unique, "COMP")
    n4_labels, n4_counts = al.frequency_distribution(t_unique, "N4")

    # 6) 保存
    al.save_as_file((comp_labels, comp_counts), "fd_COMP.csv")
    al.save_as_file((n4_labels, n4_counts), "fd_N4.csv")

finally:
    raw.close()
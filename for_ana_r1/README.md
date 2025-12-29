# 3GPP標準必須特許（SEP）分析ツール

## 概要

このプロジェクトは、3GPP標準必須特許（Standard Essential Patents）のデータを分析するためのPythonツールです。大規模CSVデータ（GB級）を効率的に処理し、企業別・技術仕様別の特許度数分布を生成します。

### 主な特徴

- ✅ **高速処理**: SQLiteとCTE（Common Table Expression）を活用した効率的なデータ処理
- ✅ **決定的な重複排除**: `__src_rownum`による再現可能な結果
- ✅ **パイプライン処理**: フィルタ、連結、ユニーク化などの操作を連鎖可能
- ✅ **GB級データ対応**: ストリーミング処理と進捗表示機能
- ✅ **自動正規化**: 0/1列の自動検出と正規化

## プロジェクト構成

```
for_ana_r1/
├── README.md                    # このファイル
│
├── main.py                      # メイン分析スクリプト（全企業）
├── main_jp.py                   # 日本企業限定の分析
├── main_ts_tr_jp.py            # 日本企業の技術仕様別分析
│
├── table_sql.py                 # SQLパイプライン処理（コア）
├── table_rule.py                # Pipeline定義（where, unique_by, concat等）
├── normalization.py             # データ正規化
├── table_csv.py                 # CSV前処理（区切り文字・エンコーディング推定）
├── table_al.py                  # 分析ユーティリティ（度数分布）
├── progress.py                  # 進捗表示
│
└── work.sqlite                  # 作業用データベース（自動生成）
```

## インストール

### 必要な環境

- Python 3.7以上
- 標準ライブラリのみ（外部依存なし）

### セットアップ

```bash
# リポジトリをクローン
git clone <repository-url>
cd for_ana_r1

# CSVデータを配置
# ../ISLD-export/ISLD-export.csv
```

## 使い方

### 1. 全企業の分析（main.py）

```bash
python main.py
```

**処理内容**:
- 3G/4G/5Gごとに標準必須特許を抽出
- 企業名（COMP_LEGAL_NAME）の度数分布を生成
- ユニーク化前後の結果を出力

**出力ファイル**:
- `fd_3G_non_unique.csv` / `fd_3G.csv`
- `fd_4G_non_unique.csv` / `fd_4G.csv`
- `fd_5G_non_unique.csv` / `fd_5G.csv`

### 2. 日本企業限定の分析（main_jp.py）

```bash
python main_jp.py
```

**処理内容**:
- 日本登録（Country_Of_Registration == "JP JAPAN"）の特許のみを抽出
- 企業名の度数分布を生成

**出力ファイル**:
- `fd_jp_3G_non_unique.csv` / `fd_jp_3G.csv`
- `fd_jp_4G_non_unique.csv` / `fd_jp_4G.csv`
- `fd_jp_5G_non_unique.csv` / `fd_jp_5G.csv`

### 3. 技術仕様別の分析（main_ts_tr_jp.py）

```bash
python main_ts_tr_jp.py
```

**処理内容**:
- 技術仕様番号（TSTRNUM = 3GPP_Type + "_" + TGPP_NUMBER）を生成
  - 例: "TS_36.331", "TR_38.912"
- 技術仕様ごとの特許数を集計

**出力ファイル**:
- `fd_jp_tstr_3G_non_unique.csv` / `fd_jp_tstr_3G.csv`
- `fd_jp_tstr_4G_non_unique.csv` / `fd_jp_tstr_4G.csv`
- `fd_jp_tstr_5G_non_unique.csv` / `fd_jp_tstr_5G.csv`

## アーキテクチャ

### パイプライン処理フロー

```
1. CSV読み込み
   ↓ (table_sql.get)
   
2. SQLiteテーブル化 + __src_rownum付与
   ↓ (TableSQL.from_csv)
   
3. データ正規化 (0/1列の統一、空白除去)
   ↓ (normalization.normal)
   
4. パイプライン処理
   ├─ where_eq()        : フィルタ（=条件）
   ├─ where_all_eq()    : フィルタ（AND条件）
   ├─ where_in()        : フィルタ（IN条件）
   ├─ concat()          : 列の連結
   └─ unique_by()       : 重複排除
   ↓ (apply_pipeline)
   
5. 度数分布の計算・出力
   └─ (TableAL.frequency_distribution)
```

### 重要な実装詳細

#### 1. 決定的な重複排除

`__src_rownum`列により、同じデータで実行するたびに同じ結果が得られます。

```python
# CSV読み込み時に自動付与
__src_rownum = 1, 2, 3, ...  # 元ファイルの行番号

# unique_by()で使用
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY key_col 
    ORDER BY __src_rownum ASC, __rid ASC
  ) AS __rn
  FROM table
) WHERE __rn = 1
```

#### 2. Pipeline API

```python
from table_rule import Pipeline
from table_sql import apply_pipeline

# パイプライン定義
pipeline = Pipeline()
pipeline.where_eq("Ess_To_Standard", 1)     # 標準必須特許のみ
pipeline.where_eq("3G", 1)                  # 3G世代のみ
pipeline.concat(["Type", "Number"], "ID")   # 列の連結
pipeline.unique_by("IPRD_ID")               # 重複排除

# 実行（1回のSQLで完了）
result = apply_pipeline(table, pipeline, return_heads=["IPRD_ID", "ID"])
```

#### 3. 効率的な列選択

`return_heads`パラメータで必要な列のみを保持し、I/O効率を向上：

```python
# フィルタ後は必要な列のみ
apply_pipeline(table, pipeline, return_heads=["IPRD_ID", "Type", "Number"])

# 注: __src_rownum は自動的に保持される（重複排除に必要）
```

## データスキーマ

### 入力CSV（ISLD-export.csv）

主要な列：
- `IPRD_ID`: IPR宣言ID（ユニーク化のキー）
- `COMP_LEGAL_NAME`: 企業名
- `Ess_To_Standard`: 標準必須フラグ（0/1）
- `Country_Of_Registration`: 登録国
- `3GPP_Type`: 技術仕様タイプ（TS/TRなど）
- `TGPP_NUMBER`: 技術仕様番号
- `3G`, `4G`, `5G`: 世代フラグ（0/1）

### 中間テーブル

すべてのテーブルに`__src_rownum`列が自動付与されます：

```sql
CREATE TABLE t_raw (
  __src_rownum INTEGER,  -- 元ファイルの行番号（自動付与）
  IPRD_ID TEXT,
  COMP_LEGAL_NAME TEXT,
  ...
);

CREATE INDEX idx_t_raw_src_rownum ON t_raw(__src_rownum);
```

### 出力CSV

度数分布ファイルの形式：

```csv
label,count
COMPANY_A,150
COMPANY_B,120
COMPANY_C,80
...
```

## 設定のカスタマイズ

### CSV パス

各メインスクリプトで変更可能：

```python
CSV_PATH = "../ISLD-export/ISLD-export.csv"  # 入力CSVファイル
DB_PATH = "work.sqlite"                       # 作業用DB
```

### 読み込む列

効率化のため、必要な列のみを指定：

```python
HEADS = ["IPRD_ID", "COMP_LEGAL_NAME", "Ess_To_Standard", "3G", "4G", "5G"]
```

### フィルタ条件

Pipeline APIで柔軟に変更可能：

```python
pipeline = Pipeline()
pipeline.where_eq("Ess_To_Standard", 1)           # 標準必須特許
pipeline.where_eq("Country_Of_Registration", "US UNITED STATES")  # 米国
pipeline.where_in("3G", [1])                      # 3Gのみ
```

## パフォーマンス

### GB級データの処理

- **CSV読み込み**: バッチINSERT（デフォルト100k行）とWALモードで高速化
- **正規化**: トランザクション内で一括処理
- **パイプライン**: CTE（WITH句）により1回のSQLで完結
- **インデックス**: 主要列に自動作成

### 進捗表示

```
[progress] 1,500,000 lines (100MB) | 50.5% | 5.2MB/s | bad=12 | ...
```

## デバッグ

### SQL確認

```python
# デバッグモードでSQL表示
apply_pipeline(table, pipeline, debug=True)
```

出力例：
```
================================================================================
【生成されたSQL】
================================================================================
WITH base AS (SELECT rowid AS __rid, * FROM "t_norm"),
s1 AS (SELECT * FROM base WHERE "Ess_To_Standard" = ?),
s2 AS (SELECT * FROM s1 WHERE "3G" = ?)
SELECT "__src_rownum", "IPRD_ID", "COMP_LEGAL_NAME" FROM s2

【パラメータ】
(1, 1)
...
```

### 中間テーブル確認

```bash
sqlite3 work.sqlite
```

```sql
-- テーブル一覧
.tables

-- スキーマ確認
PRAGMA table_info('t_flt_3G');

-- データサンプル
SELECT * FROM t_flt_3G LIMIT 10;
```

## トラブルシューティング

### エラー: no such column: __src_rownum

原因: 古いデータベースファイルを使用している

解決策:
```bash
# データベースを削除して再実行
rm work.sqlite
python main.py
```

### エラー: UnicodeDecodeError

原因: CSVエンコーディングの推定失敗

解決策:
```python
# table_csv.py のフォールバックリストを調整
ENCODING_FALLBACKS = ["utf-8-sig", "utf-8", "cp932", "shift_jis", "latin1"]
```

### メモリ不足

解決策:
```python
# バッチサイズを調整（table_sql.py）
TableSQL.from_csv(csv_path, batch=50_000)  # デフォルト: 100_000

# 正規化のバッチサイズも調整（normalization.py）
normal(raw, batch=50_000)
```

## 今後の拡張

### パテントファミリー単位の集計

現在は宣言単位（IPRD_ID）ですが、パテントファミリー単位に変更可能：

```python
# HEADS に追加
HEADS = ["IPRD_ID", "DIPG_PATF_ID", ...]

# unique_by を変更
pipeline.unique_by("DIPG_PATF_ID")  # パテントファミリー単位
```

### 新しいパイプライン操作

`table_rule.py`にメソッドを追加：

```python
def where_like(self, head: str, pattern: str) -> "Pipeline":
    """LIKE条件"""
    self.steps.append(("where_like", head, pattern))
    return self
```

対応するSQL生成を`table_sql.py`に実装。

## ライセンス

（ライセンス情報を記載）

## 貢献

（貢献ガイドラインを記載）

## 連絡先

（連絡先情報を記載）


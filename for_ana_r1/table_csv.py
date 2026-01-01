#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
table_csv.py

CSV/TSV入力ファイルの前処理ユーティリティ:
- 区切り文字(sep)推定
- 文字コード(encoding)推定（フォールバック）

※巨大ファイルでも、先頭の小さなサンプルだけで推定する。
"""

from __future__ import annotations

from typing import Optional


class TableCSV:
    """CSV/TSVの推定ユーティリティ"""

    SEP_CANDIDATES = ["\t", ";", ",", "|"]
    ENCODING_FALLBACKS = ["utf-8-sig", "utf-8", "cp932", "latin1"]

    @staticmethod
    def guess_sep(csv_fullpath: str, *, encoding: Optional[str] = None, sample_bytes: int = 8192) -> str:
        """区切り文字推定（同点はタブ優先）"""
        enc = encoding or "utf-8-sig"
        with open(csv_fullpath, "rb") as fb:
            raw = fb.read(sample_bytes)

        try:
            sample = raw.decode(enc, errors="strict")
        except Exception:
            sample = raw.decode("utf-8-sig", errors="replace")

        counts = {c: sample.count(c) for c in TableCSV.SEP_CANDIDATES}
        best = max(TableCSV.SEP_CANDIDATES, key=lambda c: (counts[c], 1 if c == "\t" else 0))
        return best

    @staticmethod
    def guess_encoding(csv_fullpath: str, *, preferred: Optional[str] = None) -> str:
        """文字コード推定（preferred → utf-8-sig → utf-8 → cp932 → latin1）"""
        tries = []
        if preferred:
            tries.append(preferred)
        for e in TableCSV.ENCODING_FALLBACKS:
            if e not in tries:
                tries.append(e)

        with open(csv_fullpath, "rb") as fb:
            raw = fb.read(8192)

        last_err = None
        for enc in tries:
            try:
                raw.decode(enc, errors="strict")
                return enc
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"文字コード推定に失敗しました。試行={tries} 最終エラー={last_err!r}")

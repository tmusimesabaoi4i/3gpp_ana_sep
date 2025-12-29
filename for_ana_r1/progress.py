#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
progress.py

巨大CSV/TSVのストリーミング処理向けの進捗表示ユーティリティ。

- 標準エラー (stderr) へ一定間隔で進捗を出力する。
- 進捗は「行数」をトリガにしつつ、可能ならファイル位置(バイト)から進捗率(%)を推定する。

注意:
- TextIOWrapper.tell() は環境/使い方によって例外になることがあるため、
  呼び出し側は「バイト位置」を可能ならバイナリファイル側の tell() で渡すことを推奨。
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass


def _fmt_rate_bytes(bytes_delta: int, sec_delta: float) -> str:
    """速度表記（MB/s）"""
    if sec_delta <= 0:
        return "0.00MB/s"
    mb = bytes_delta / (1024.0 * 1024.0)
    return f"{mb / sec_delta:.2f}MB/s"


def _fmt_rate_lines(lines_delta: int, sec_delta: float) -> str:
    """速度表記（lines/s）"""
    if sec_delta <= 0:
        return "0.0l/s"
    return f"{lines_delta / sec_delta:,.1f}l/s"


@dataclass
class Progress:
    """進捗表示クラス"""

    file_size_bytes: int
    progress_every_lines: int = 300_000

    def __post_init__(self) -> None:
        self._t0 = time.time()
        self._t_last = self._t0
        self._b_last = 0
        self._l_last = 0

    def tick(
        self,
        lines_total: int,
        bad_total: int,
        bytes_pos: int,
        *,
        sep: str,
        encoding: str,
        table_name: str,
    ) -> None:
        """必要なタイミングで進捗ログを出す。"""
        if self.progress_every_lines <= 0:
            return
        if lines_total <= 0:
            return
        if lines_total % self.progress_every_lines != 0:
            return

        now = time.time()
        dt = now - self._t_last
        db = max(0, bytes_pos - self._b_last)
        dl = max(0, lines_total - self._l_last)

        if self.file_size_bytes > 0:
            pct = min(100.0, max(0.0, (bytes_pos / self.file_size_bytes) * 100.0))
            pct_s = f"{pct:6.2f}%"
            rate = _fmt_rate_bytes(db, dt)
        else:
            pct_s = "  n/a "
            rate = _fmt_rate_lines(dl, dt)

        elapsed = now - self._t0

        sys.stderr.write(
            f"[LOAD {pct_s}] lines={lines_total:,} bad={bad_total:,} "
            f"rate={rate} elapsed={elapsed:.1f}s sep={sep!r} enc={encoding!r} table={table_name}\n"
        )
        sys.stderr.flush()

        self._t_last = now
        self._b_last = bytes_pos
        self._l_last = lines_total

    def done(self, lines_total: int, bad_total: int, *, table_name: str) -> None:
        """完了ログ"""
        elapsed = time.time() - self._t0
        sys.stderr.write(
            f"[LOAD DONE] lines={lines_total:,} bad={bad_total:,} elapsed={elapsed:.1f}s table={table_name}\n"
        )
        sys.stderr.flush()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
table_al.py

データ分析クラス（可視化なし）:
- frequency_distribution(table, head) -> (labels, counts)
- save_as_file((labels, counts), filename)
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import List, Sequence, Tuple, Union

from table_sql import TableSQL, qident


@dataclass
class TableAL:
    """分析ユーティリティ"""

    def frequency_distribution(
        self,
        table: TableSQL,
        head: Union[str, Sequence[str]],
        *,
        order_by_count_desc: bool = True,
        null_as_empty: bool = True,
        sep: str = "|",
    ) -> Tuple[List[str], List[int]]:
        """tableからheadの度数分布を作成する。"""
        if isinstance(head, str):
            label_expr = qident(head)
            group_expr = qident(head)
        else:
            heads = list(head)
            if not heads:
                raise ValueError("head が空です。")
            parts = [f"COALESCE({qident(h)}, '')" for h in heads] if null_as_empty else [qident(h) for h in heads]
            label_expr = (" || " + repr(sep) + " || ").join(parts)
            group_expr = ", ".join(qident(h) for h in heads)

        src = qident(table.table_name)
        order = "cnt DESC" if order_by_count_desc else "label ASC"
        sql = f"SELECT {label_expr} AS label, COUNT(*) AS cnt FROM {src} GROUP BY {group_expr} ORDER BY {order}"

        labels: List[str] = []
        counts: List[int] = []
        for label, cnt in table.iterquery(sql):
            labels.append("" if label is None else str(label))
            counts.append(int(cnt))
        return labels, counts

    def save_as_file(
        self,
        data: Tuple[List[str], List[int]],
        filename: str,
        *,
        label_header: str = "label",
        count_header: str = "count",
        encoding: str = "utf-8-sig",
    ) -> None:
        """data=(labels, counts) をCSVとして保存する。"""
        labels, counts = data
        if len(labels) != len(counts):
            raise ValueError("labels と counts の長さが一致しません。")

        with open(filename, "w", encoding=encoding, newline="") as f:
            w = csv.writer(f)
            w.writerow([label_header, count_header])
            for l, c in zip(labels, counts):
                w.writerow([l, c])

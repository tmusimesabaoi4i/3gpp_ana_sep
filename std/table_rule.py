#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
table_rule.py（簡素版）

Pipeline に「規則（regulation）」を追加するだけ。
SQL生成は table_sql.build_pipeline_sql() が行う。

実装した規則:
1) unique_by(head)                 : headで一意化（元ファイルの最初の行を採用、決定的）
2) concat(heads, newhead, sep)     : headsをsepで連結しnewhead列を生成
3) where_eq(head, val)             : head = val（val=Noneの場合は IS NULL）
4) where_ne(head, val)             : head != val（val=Noneの場合は IS NOT NULL）
                                    ※val指定時は「NULLも!=とみなす」ため head IS NULL OR head <> val
5) where_all_eq({h1:v1,...})       : h1=v1 AND h2=v2 ...
6) where_in(head, [v1,v2,...])     : head IN (v1,v2,...)（OR条件の実用形）
7) where_between(head, start, end) : headの日付が start<=head<=end の行のみ抽出（YYYY-MM-DD推奨）

※ 追加順が実行順（CTEで順番を固定）
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence, Tuple


@dataclass
class Pipeline:
    """
    規則の配列（追加順が実行順）
    steps: list[tuple] で保持（クラス増殖を避ける）

    注意: unique_by()が含まれる場合、全列が保持される
    """
    steps: List[Tuple] = field(default_factory=list)

    def unique_by(self, head: str) -> "Pipeline":
        """
        指定された列(head)で一意化する
        重複がある場合、元ファイルで最初に出現した行を採用（決定的）
        全列が保持される
        """
        self.steps.append(("unique_by", head))
        return self

    def concat(self, heads: Sequence[str], newhead: str, sep: str = "_") -> "Pipeline":
        """
        複数列(heads)を指定された区切り文字(sep)で連結し、新しい列(newhead)を生成
        """
        self.steps.append(("concat", tuple(heads), newhead, sep))
        return self

    def where_eq(self, head: str, val: Any) -> "Pipeline":
        """
        単一条件フィルタ: head = val
        valがNoneの場合は head IS NULL
        """
        self.steps.append(("where_eq", head, val))
        return self

    def where_ne(self, head: str, val: Any) -> "Pipeline":
        """
        単一条件フィルタ: head != val
        - valがNoneの場合は head IS NOT NULL
        - valが指定された場合は「NULLも!=とみなす」ため、head IS NULL OR head <> val
          （SQLのNULL比較は UNKNOWN になるため、直感に合わせて NULL を包含）
        """
        self.steps.append(("where_ne", head, val))
        return self

    def where_all_eq(self, mapping: Dict[str, Any]) -> "Pipeline":
        """
        AND条件フィルタ: h1=v1 AND h2=v2 AND ...
        mappingの各キー・バリューペアが条件となる
        """
        self.steps.append(("where_all_eq", dict(mapping)))
        return self

    def where_in(self, head: str, vals: Sequence[Any]) -> "Pipeline":
        """
        OR条件フィルタ: head IN (v1, v2, ...)
        実用的なOR条件の表現
        """
        self.steps.append(("where_in", head, tuple(vals)))
        return self

    def where_between(self, head: str, start: str, end: str) -> "Pipeline":
        """
        日付範囲フィルタ: start <= head <= end
        - start/end は "YYYY-MM-DD" 推奨
        - start または end が空文字の場合、その境界は無視（片側だけの範囲指定）
        """
        self.steps.append(("where_between", head, start, end))
        return self

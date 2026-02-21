from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flink_types import Collector

def process_element(line: str, out: Collector) -> None:
    """Noop — intentionally does nothing."""
    pass

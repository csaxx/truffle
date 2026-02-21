# Type stubs for Java objects passed into Python via GraalPy polyglot interop.
# This file is for IDE type-checking only; it is NOT loaded at runtime.

from typing import Protocol


class Collector(Protocol):
    """Mirrors Flink's org.apache.flink.util.Collector<T> for T=str."""
    def collect(self, value: str) -> None: ...

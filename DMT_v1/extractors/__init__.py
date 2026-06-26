# Base extractor interface for all source types.
# Co-authored with CoCo
"""extractors — Base interface for data extraction from source systems.

Each source type (MySQL, Teradata, etc.) implements this interface so the
orchestrator can call extract() uniformly regardless of the underlying engine.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ExtractionResult:
    """Outcome of an extraction step."""
    files: list[Path] = field(default_factory=list)
    row_count: int = 0
    watermark_to: str | None = None
    file_format: str = "parquet"
    engine: str = "unknown"
    skipped: bool = False
    skip_reason: str | None = None


class BaseExtractor(ABC):
    """Abstract base for source data extraction."""

    @abstractmethod
    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        """Full extraction of the entire table."""
        ...

    @abstractmethod
    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Incremental extraction based on watermark/cursor."""
        ...

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Return the source type identifier: mysql | teradata | postgres."""
        ...

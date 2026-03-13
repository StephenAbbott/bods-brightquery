"""Output writer for BODS statements.

Supports two output formats:
    - JSON: A single JSON array of all statements (for small datasets).
    - JSONL: One JSON object per line (for large datasets, streaming-friendly).
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


class BODSWriter:
    """Writes BODS statements to JSON or JSONL files."""

    def __init__(self, output_path: str | Path, format: str = "json"):
        self.output_path = str(output_path)
        self.format = format.lower()
        self._statements: list[dict] = []
        self._count = 0

        if self.format not in ("json", "jsonl"):
            raise ValueError(f"Unsupported output format: {format}. Use 'json' or 'jsonl'.")

    @property
    def is_stdout(self) -> bool:
        return self.output_path == "-"

    def write_statements(self, statements: list[dict]) -> None:
        if not statements:
            return

        if self.format == "jsonl":
            self._write_jsonl(statements)
        else:
            self._statements.extend(statements)

        self._count += len(statements)

    def finalize(self) -> None:
        if self.format == "json":
            self._write_json(self._statements)
            self._statements = []

        logger.info(
            "Wrote %d BODS statements to %s (%s format)",
            self._count,
            self.output_path,
            self.format,
        )

    def _write_jsonl(self, statements: list[dict]) -> None:
        if self.is_stdout:
            for stmt in statements:
                sys.stdout.write(json.dumps(stmt, ensure_ascii=False, default=str) + "\n")
            sys.stdout.flush()
        else:
            with open(self.output_path, "a", encoding="utf-8") as f:
                for stmt in statements:
                    f.write(json.dumps(stmt, ensure_ascii=False, default=str) + "\n")

    def _write_json(self, statements: list[dict]) -> None:
        if self.is_stdout:
            json.dump(statements, sys.stdout, indent=2, ensure_ascii=False, default=str)
            sys.stdout.write("\n")
            sys.stdout.flush()
        else:
            with open(self.output_path, "w", encoding="utf-8") as f:
                json.dump(statements, f, indent=2, ensure_ascii=False, default=str)
                f.write("\n")

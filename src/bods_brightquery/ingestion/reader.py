"""JSONL streaming reader for BrightQuery Senzing data files."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator

from bods_brightquery.ingestion.models import BQOrganization, BQPerson

logger = logging.getLogger(__name__)


class BrightQueryReader:
    """Reads BrightQuery Senzing JSONL files as streaming iterators."""

    def read_organizations(self, filepath: Path | str) -> Iterator[BQOrganization]:
        """Read organizations from a JSONL file."""
        count = 0
        errors = 0
        for record in self._read_jsonl(filepath):
            try:
                org = BQOrganization.from_senzing_record(record)
                if org is not None:
                    count += 1
                    yield org
                else:
                    errors += 1
            except Exception as e:
                errors += 1
                if errors <= 10:
                    logger.warning("Error parsing organization record: %s", e)

        logger.info(
            "Read %d organizations from %s (%d errors)", count, filepath, errors
        )

    def read_people(self, filepath: Path | str) -> Iterator[BQPerson]:
        """Read people-business records from a JSONL file."""
        count = 0
        errors = 0
        for record in self._read_jsonl(filepath):
            try:
                person = BQPerson.from_senzing_record(record)
                if person is not None:
                    count += 1
                    yield person
                else:
                    errors += 1
            except Exception as e:
                errors += 1
                if errors <= 10:
                    logger.warning("Error parsing person record: %s", e)

        logger.info(
            "Read %d people from %s (%d errors)", count, filepath, errors
        )

    def _read_jsonl(self, filepath: Path | str) -> Iterator[dict]:
        """Read a JSONL file, yielding one parsed dict per line."""
        filepath = Path(filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    if line_num <= 10:
                        logger.warning(
                            "Invalid JSON at %s:%d: %s", filepath.name, line_num, e
                        )

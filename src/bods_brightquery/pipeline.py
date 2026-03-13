"""Pipeline orchestrator for transforming BrightQuery data to BODS."""

from __future__ import annotations

import logging
from pathlib import Path

from bods_brightquery.config import PublisherConfig
from bods_brightquery.ingestion.reader import BrightQueryReader
from bods_brightquery.output.writer import BODSWriter
from bods_brightquery.transform.entities import transform_organization
from bods_brightquery.transform.identifiers import entity_record_id, person_record_id
from bods_brightquery.transform.persons import transform_person
from bods_brightquery.transform.relationships import transform_contact_relationship

logger = logging.getLogger(__name__)


class BODSPipeline:
    """Orchestrates the transformation of BrightQuery data to BODS format.

    Usage:
        config = PublisherConfig(output_path="output.jsonl")
        pipeline = BODSPipeline(config)
        pipeline.process_organizations("~/Desktop/Organization/bq_org_000.json")
        pipeline.process_people("~/Desktop/PeopleBusiness/bq_people_000.json")
        pipeline.finalize()
    """

    def __init__(self, config: PublisherConfig):
        self.config = config
        self.writer = BODSWriter(config.output_path, config.output_format)
        self._emitted_record_ids: set[str] = set()
        self._reader = BrightQueryReader()

    @property
    def statement_count(self) -> int:
        return self.writer._count

    def process_organizations(self, path: str | Path) -> int:
        """Process organization JSONL file(s), emitting entity statements.

        Args:
            path: Path to a JSONL file or directory of JSONL files.

        Returns:
            Number of entity statements generated.
        """
        count = 0

        for filepath in self._resolve_files(path):
            for org in self._reader.read_organizations(filepath):
                rec_id = entity_record_id(org.bq_id)
                if rec_id in self._emitted_record_ids:
                    continue

                stmt = transform_organization(org, self.config)
                self.writer.write_statements([stmt])
                self._emitted_record_ids.add(rec_id)
                count += 1

                if count % 50000 == 0:
                    logger.info("Processed %d organization entities", count)

        logger.info("Generated %d entity statements from organizations", count)
        return count

    def process_people(self, path: str | Path) -> int:
        """Process people-business JSONL file(s), emitting person + relationship statements.

        Args:
            path: Path to a JSONL file or directory of JSONL files.

        Returns:
            Number of statements generated (person + relationship).
        """
        count = 0

        for filepath in self._resolve_files(path):
            for person in self._reader.read_people(filepath):
                statements = self._transform_person_with_relationship(person)
                if statements:
                    self.writer.write_statements(statements)
                    count += len(statements)

                if count % 50000 == 0 and count > 0:
                    logger.info("Processed %d person/relationship statements", count)

        logger.info("Generated %d statements from people data", count)
        return count

    def finalize(self) -> None:
        """Finalize the output (flush buffers, close files)."""
        self.writer.finalize()
        logger.info(
            "Pipeline complete: %d total statements, %d unique records tracked",
            self.writer._count,
            len(self._emitted_record_ids),
        )

    def _transform_person_with_relationship(self, person) -> list[dict]:
        """Transform a person into person statement + relationship statement."""
        if not person.org_bq_id:
            logger.debug(
                "Skipping person %s: no organization link", person.record_id
            )
            return []

        statements: list[dict] = []

        # Entity record ID for the organization
        ent_rec_id = entity_record_id(person.org_bq_id)

        # Person record ID
        per_rec_id = person_record_id(person.record_id, person.org_bq_id)

        # Person statement (deduplicate)
        if per_rec_id not in self._emitted_record_ids:
            person_stmt = transform_person(person, self.config)
            # Set declarationSubject to the entity
            person_stmt["declarationSubject"] = ent_rec_id
            statements.append(person_stmt)
            self._emitted_record_ids.add(per_rec_id)

        # Relationship statement
        rel_stmt = transform_contact_relationship(
            ent_rec_id, per_rec_id, person.role, self.config
        )
        statements.append(rel_stmt)

        return statements

    def _resolve_files(self, path: str | Path) -> list[Path]:
        """Resolve a path to a list of JSONL files.

        If path is a file, return it as a single-element list.
        If path is a directory, glob for *.json files sorted by name.
        """
        path = Path(path)
        if path.is_file():
            return [path]
        elif path.is_dir():
            files = sorted(path.glob("*.json"))
            logger.info("Found %d JSON files in %s", len(files), path)
            return files
        else:
            logger.warning("Path does not exist: %s", path)
            return []

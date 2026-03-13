"""Integration tests for the BODS pipeline."""

import json

import pytest

from bods_brightquery.config import PublisherConfig
from bods_brightquery.pipeline import BODSPipeline

from .conftest import (
    SAMPLE_ORG_RECORD,
    SAMPLE_ORG_SIMPLE,
    SAMPLE_PERSON_FULL,
    SAMPLE_PERSON_NULL_NAME,
    write_jsonl,
)


def _make_config(tmp_path, fmt="json") -> PublisherConfig:
    return PublisherConfig(
        publication_date="2026-03-13",
        output_path=str(tmp_path / f"output.{fmt}"),
        output_format=fmt,
    )


class TestPipelineOrganizations:
    def test_process_single_file(self, tmp_path):
        org_file = tmp_path / "orgs.json"
        write_jsonl([SAMPLE_ORG_SIMPLE, SAMPLE_ORG_RECORD], org_file)

        config = _make_config(tmp_path)
        pipeline = BODSPipeline(config)
        count = pipeline.process_organizations(org_file)
        pipeline.finalize()

        assert count == 2

        with open(config.output_path) as f:
            statements = json.load(f)

        assert len(statements) == 2
        assert all(s["recordType"] == "entity" for s in statements)

    def test_deduplication(self, tmp_path):
        org_file = tmp_path / "orgs.json"
        # Same org twice
        write_jsonl([SAMPLE_ORG_SIMPLE, SAMPLE_ORG_SIMPLE], org_file)

        config = _make_config(tmp_path)
        pipeline = BODSPipeline(config)
        count = pipeline.process_organizations(org_file)
        pipeline.finalize()

        assert count == 1

    def test_process_directory(self, tmp_path):
        org_dir = tmp_path / "orgs"
        org_dir.mkdir()
        write_jsonl([SAMPLE_ORG_SIMPLE], org_dir / "part1.json")
        write_jsonl([SAMPLE_ORG_RECORD], org_dir / "part2.json")

        config = _make_config(tmp_path)
        pipeline = BODSPipeline(config)
        count = pipeline.process_organizations(org_dir)
        pipeline.finalize()

        assert count == 2


class TestPipelinePeople:
    def test_process_people(self, tmp_path):
        people_file = tmp_path / "people.json"
        write_jsonl([SAMPLE_PERSON_FULL], people_file)

        config = _make_config(tmp_path)
        pipeline = BODSPipeline(config)
        count = pipeline.process_people(people_file)
        pipeline.finalize()

        # 1 person + 1 relationship = 2 statements
        assert count == 2

        with open(config.output_path) as f:
            statements = json.load(f)

        types = [s["recordType"] for s in statements]
        assert "person" in types
        assert "relationship" in types

    def test_anonymous_person(self, tmp_path):
        people_file = tmp_path / "people.json"
        write_jsonl([SAMPLE_PERSON_NULL_NAME], people_file)

        config = _make_config(tmp_path)
        pipeline = BODSPipeline(config)
        count = pipeline.process_people(people_file)
        pipeline.finalize()

        with open(config.output_path) as f:
            statements = json.load(f)

        person_stmts = [s for s in statements if s["recordType"] == "person"]
        assert person_stmts[0]["recordDetails"]["personType"] == "anonymousPerson"


class TestPipelineEndToEnd:
    def test_full_pipeline(self, tmp_path):
        org_file = tmp_path / "orgs.json"
        people_file = tmp_path / "people.json"

        write_jsonl([SAMPLE_ORG_SIMPLE], org_file)
        write_jsonl([SAMPLE_PERSON_FULL], people_file)

        config = _make_config(tmp_path)
        pipeline = BODSPipeline(config)
        pipeline.process_organizations(org_file)
        pipeline.process_people(people_file)
        pipeline.finalize()

        with open(config.output_path) as f:
            statements = json.load(f)

        # 1 entity + 1 person + 1 relationship = 3
        assert len(statements) == 3

        types = {s["recordType"] for s in statements}
        assert types == {"entity", "person", "relationship"}

    def test_jsonl_output(self, tmp_path):
        org_file = tmp_path / "orgs.json"
        write_jsonl([SAMPLE_ORG_SIMPLE], org_file)

        config = _make_config(tmp_path, fmt="jsonl")
        pipeline = BODSPipeline(config)
        pipeline.process_organizations(org_file)
        pipeline.finalize()

        with open(config.output_path) as f:
            lines = [json.loads(line) for line in f if line.strip()]

        assert len(lines) == 1
        assert lines[0]["recordType"] == "entity"

    def test_statement_ids_are_deterministic(self, tmp_path):
        """Running the pipeline twice produces identical statement IDs."""
        org_file = tmp_path / "orgs.json"
        write_jsonl([SAMPLE_ORG_SIMPLE], org_file)

        ids = []
        for i in range(2):
            out = tmp_path / f"output{i}.json"
            config = PublisherConfig(
                publication_date="2026-03-13",
                output_path=str(out),
                output_format="json",
            )
            pipeline = BODSPipeline(config)
            pipeline.process_organizations(org_file)
            pipeline.finalize()

            with open(out) as f:
                stmts = json.load(f)
            ids.append([s["statementId"] for s in stmts])

        assert ids[0] == ids[1]

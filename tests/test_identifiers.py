"""Tests for identifier generation."""

from bods_brightquery.transform.identifiers import (
    build_entity_identifiers,
    entity_record_id,
    generate_statement_id,
    person_record_id,
    relationship_record_id,
)


class TestRecordIds:
    def test_entity_record_id(self):
        assert entity_record_id("100001813958") == "brightquery-100001813958"

    def test_person_record_id(self):
        result = person_record_id("8880744527", "100000038057")
        assert result == "brightquery-person-8880744527-100000038057"

    def test_relationship_record_id(self):
        result = relationship_record_id("brightquery-100001", "brightquery-person-200001-100001")
        assert result == "brightquery-100001-rel-brightquery-person-200001-100001"


class TestStatementIds:
    def test_deterministic(self):
        """Same inputs always produce the same statement ID."""
        id1 = generate_statement_id("rec-1", "2026-01-01", "new")
        id2 = generate_statement_id("rec-1", "2026-01-01", "new")
        assert id1 == id2

    def test_different_inputs_different_ids(self):
        id1 = generate_statement_id("rec-1", "2026-01-01", "new")
        id2 = generate_statement_id("rec-2", "2026-01-01", "new")
        assert id1 != id2

    def test_uuid_format(self):
        result = generate_statement_id("rec-1", "2026-01-01")
        # UUID v5 format: 8-4-4-4-12 hex digits
        parts = result.split("-")
        assert len(parts) == 5
        assert len(result) == 36


class TestEntityIdentifiers:
    def test_bq_id_only(self):
        ids = build_entity_identifiers("100001")
        assert len(ids) == 1
        assert ids[0]["id"] == "100001"
        assert ids[0]["schemeName"] == "BrightQuery"

    def test_with_lei(self):
        ids = build_entity_identifiers("100001", lei_number="5493001KJTIIGC8Y1R12")
        assert len(ids) == 2
        lei = ids[1]
        assert lei["id"] == "5493001KJTIIGC8Y1R12"
        assert lei["scheme"] == "XI-LEI"

    def test_with_npi(self):
        ids = build_entity_identifiers("100001", npi_number="1376289876")
        assert len(ids) == 2
        npi = ids[1]
        assert npi["id"] == "1376289876"
        assert "NPI" in npi["schemeName"]

    def test_with_other_ids(self):
        other_ids = {
            "TICKER": "CHTR",
            "CIK": "0001091667",
            "SAM_UEI": "D872MLL5ENG3",
        }
        ids = build_entity_identifiers("100001", other_ids=other_ids)
        # BQ_ID + 3 other IDs
        assert len(ids) == 4

        id_map = {i["schemeName"]: i["id"] for i in ids}
        assert id_map["Stock Ticker"] == "CHTR"
        assert id_map["U.S. Securities and Exchange Commission"] == "0001091667"
        assert id_map["SAM.gov Unique Entity Identifier"] == "D872MLL5ENG3"

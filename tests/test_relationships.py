"""Tests for relationship statement transformation."""

from bods_brightquery.config import PublisherConfig
from bods_brightquery.transform.relationships import transform_contact_relationship


def _make_config() -> PublisherConfig:
    return PublisherConfig(publication_date="2026-03-13")


class TestTransformContactRelationship:
    def test_basic_structure(self):
        stmt = transform_contact_relationship(
            entity_rec_id="brightquery-100001",
            person_rec_id="brightquery-person-200001-100001",
            role="Contact",
            config=_make_config(),
        )

        assert stmt["recordType"] == "relationship"
        assert stmt["recordStatus"] == "new"
        assert stmt["declarationSubject"] == "brightquery-100001"

    def test_subject_and_interested_party(self):
        stmt = transform_contact_relationship(
            entity_rec_id="brightquery-100001",
            person_rec_id="brightquery-person-200001-100001",
            role="Contact",
            config=_make_config(),
        )
        details = stmt["recordDetails"]

        assert details["subject"] == "brightquery-100001"
        assert details["interestedParty"] == "brightquery-person-200001-100001"

    def test_interest_type(self):
        stmt = transform_contact_relationship(
            entity_rec_id="brightquery-100001",
            person_rec_id="brightquery-person-200001-100001",
            role="Contact",
            config=_make_config(),
        )
        interests = stmt["recordDetails"]["interests"]

        assert len(interests) == 1
        interest = interests[0]
        assert interest["type"] == "otherInfluenceOrControl"
        assert interest["directOrIndirect"] == "unknown"
        assert interest["beneficialOwnershipOrControl"] is False

    def test_role_in_details(self):
        stmt = transform_contact_relationship(
            entity_rec_id="brightquery-100001",
            person_rec_id="brightquery-person-200001-100001",
            role="Contact",
            config=_make_config(),
        )
        interest = stmt["recordDetails"]["interests"][0]
        assert "Contact" in interest["details"]

    def test_record_id(self):
        stmt = transform_contact_relationship(
            entity_rec_id="brightquery-100001",
            person_rec_id="brightquery-person-200001-100001",
            role="Contact",
            config=_make_config(),
        )
        assert stmt["recordId"] == "brightquery-100001-rel-brightquery-person-200001-100001"

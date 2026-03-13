"""Tests for person statement transformation."""

from bods_brightquery.config import PublisherConfig
from bods_brightquery.ingestion.models import BQPerson
from bods_brightquery.transform.persons import transform_person

from .conftest import SAMPLE_PERSON_FULL, SAMPLE_PERSON_NULL_NAME


def _make_config() -> PublisherConfig:
    return PublisherConfig(publication_date="2026-03-13")


class TestTransformPerson:
    def test_known_person(self):
        person = BQPerson.from_senzing_record(SAMPLE_PERSON_FULL)
        stmt = transform_person(person, _make_config())

        assert stmt["recordType"] == "person"
        assert stmt["recordStatus"] == "new"
        details = stmt["recordDetails"]
        assert details["personType"] == "knownPerson"

    def test_person_names(self):
        person = BQPerson.from_senzing_record(SAMPLE_PERSON_FULL)
        stmt = transform_person(person, _make_config())
        names = stmt["recordDetails"]["names"]

        assert len(names) == 1
        name = names[0]
        assert name["type"] == "individual"
        assert name["fullName"] == "Rosemary Debutts"
        assert name["givenName"] == "Rosemary"
        assert name["familyName"] == "Debutts"

    def test_person_address(self):
        person = BQPerson.from_senzing_record(SAMPLE_PERSON_FULL)
        stmt = transform_person(person, _make_config())
        addresses = stmt["recordDetails"]["addresses"]

        assert len(addresses) == 1
        assert addresses[0]["type"] == "residence"
        assert addresses[0]["country"]["code"] == "US"

    def test_anonymous_person(self):
        person = BQPerson.from_senzing_record(SAMPLE_PERSON_NULL_NAME)
        stmt = transform_person(person, _make_config())
        details = stmt["recordDetails"]

        assert details["personType"] == "anonymousPerson"
        assert "unspecifiedPersonDetails" in details
        assert "names" not in details

    def test_record_id_includes_org(self):
        person = BQPerson.from_senzing_record(SAMPLE_PERSON_FULL)
        stmt = transform_person(person, _make_config())

        assert "100000038057" in stmt["recordId"]
        assert "8880744527" in stmt["recordId"]

    def test_publication_details(self):
        person = BQPerson.from_senzing_record(SAMPLE_PERSON_FULL)
        stmt = transform_person(person, _make_config())

        assert stmt["publicationDetails"]["bodsVersion"] == "0.4"

"""Tests for entity statement transformation."""

from bods_brightquery.config import PublisherConfig
from bods_brightquery.ingestion.models import BQOrganization
from bods_brightquery.transform.entities import transform_organization

from .conftest import SAMPLE_ORG_RECORD, SAMPLE_ORG_SIMPLE


def _make_config() -> PublisherConfig:
    return PublisherConfig(publication_date="2026-03-13")


class TestTransformOrganization:
    def test_basic_structure(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_SIMPLE)
        stmt = transform_organization(org, _make_config())

        assert stmt["recordType"] == "entity"
        assert stmt["recordStatus"] == "new"
        assert stmt["recordId"] == "brightquery-100003806145"
        assert stmt["declarationSubject"] == "brightquery-100003806145"

    def test_record_details(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_SIMPLE)
        stmt = transform_organization(org, _make_config())
        details = stmt["recordDetails"]

        assert details["isComponent"] is False
        assert details["entityType"] == {"type": "registeredEntity"}
        assert details["name"] == "GWT SERVICES INC"
        assert details["jurisdiction"]["code"] == "US"

    def test_address(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_SIMPLE)
        stmt = transform_organization(org, _make_config())
        addresses = stmt["recordDetails"]["addresses"]

        assert len(addresses) == 1
        addr = addresses[0]
        assert addr["type"] == "registered"
        assert "Springfield" in addr["address"]
        assert addr["postCode"] == "62712"

    def test_rich_identifiers(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_RECORD)
        stmt = transform_organization(org, _make_config())
        identifiers = stmt["recordDetails"]["identifiers"]

        # BQ_ID + TICKER + CIK + PERMID + SAM_UEI + SAM_CAGE + CAPIQ = 7
        assert len(identifiers) == 7

        id_map = {i.get("schemeName", ""): i["id"] for i in identifiers}
        assert id_map["BrightQuery"] == "100001813958"
        assert id_map["Stock Ticker"] == "CHTR"
        assert id_map["U.S. Securities and Exchange Commission"] == "0001091667"

    def test_alternate_names(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_RECORD)
        stmt = transform_organization(org, _make_config())
        details = stmt["recordDetails"]

        assert details["name"] == "CHARTER COMMUNICATIONS INC"
        assert "alternateNames" in details
        assert "CHARTER COMMUNICATIONS, CORP" in details["alternateNames"]

    def test_website_as_uri(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_RECORD)
        stmt = transform_organization(org, _make_config())

        assert stmt["recordDetails"]["uri"] == "https://corporate.charter.com"

    def test_publication_details(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_SIMPLE)
        stmt = transform_organization(org, _make_config())

        pub = stmt["publicationDetails"]
        assert pub["bodsVersion"] == "0.4"
        assert pub["publicationDate"] == "2026-03-13"
        assert pub["publisher"]["name"] == "BODS BrightQuery Pipeline"

    def test_source(self):
        org = BQOrganization.from_senzing_record(SAMPLE_ORG_SIMPLE)
        stmt = transform_organization(org, _make_config())

        source = stmt["source"]
        assert "officialRegister" in source["type"]
        assert "BrightQuery" in source["description"]

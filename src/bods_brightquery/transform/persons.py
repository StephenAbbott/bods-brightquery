"""Transform BrightQuery people-business records into BODS person statements."""

from __future__ import annotations

from bods_brightquery.config import PublisherConfig
from bods_brightquery.ingestion.models import BQPerson
from bods_brightquery.transform.identifiers import (
    generate_statement_id,
    person_record_id,
)
from bods_brightquery.utils.statements import (
    build_publication_details,
    build_source,
    clean_statement,
)


def build_person_names(person: BQPerson) -> list[dict]:
    """Build BODS name objects from a BrightQuery person."""
    if not person.has_name:
        return []

    name: dict = {"type": "individual"}

    if person.full_name:
        name["fullName"] = _title_case(person.full_name)

    if person.first_name:
        name["givenName"] = _title_case(person.first_name)
    if person.last_name:
        name["familyName"] = _title_case(person.last_name)

    # Build fullName from parts if not provided
    if not name.get("fullName") and (person.first_name or person.last_name):
        parts = [person.first_name, person.middle_name, person.last_name]
        name["fullName"] = " ".join(_title_case(p) for p in parts if p)

    return [name]


def build_person_addresses(person: BQPerson) -> list[dict]:
    """Build BODS address objects from a BrightQuery person."""
    # BQ people data only has state and country
    if not person.state and not person.country:
        return []

    address: dict = {"type": "residence"}

    parts = [p for p in [person.state, person.country] if p]
    if parts:
        address["address"] = ", ".join(parts)

    if person.country:
        address["country"] = {
            "code": "US" if person.country == "USA" else person.country,
            "name": "United States of America" if person.country == "USA" else person.country,
        }

    return [address]


def transform_person(person: BQPerson, config: PublisherConfig) -> dict:
    """Transform a BrightQuery person into a BODS person statement."""
    rec_id = person_record_id(person.record_id, person.org_bq_id or "unknown")

    # Determine person type
    person_type = "knownPerson" if person.has_name else "anonymousPerson"

    statement = {
        "statementId": generate_statement_id(rec_id, config.publication_date),
        "declarationSubject": None,  # Set by pipeline (entity record ID)
        "statementDate": config.publication_date,
        "recordId": rec_id,
        "recordType": "person",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "personType": person_type,
            "names": build_person_names(person) or None,
            "addresses": build_person_addresses(person) or None,
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source(config),
    }

    if not person.has_name:
        statement["recordDetails"]["unspecifiedPersonDetails"] = {
            "reason": "informationUnknownToPublisher",
            "description": "Person name not available in BrightQuery data",
        }

    return clean_statement(statement)


def _title_case(s: str) -> str:
    """Convert a string to title case, handling names properly."""
    if not s:
        return s
    # Only convert if the string is all uppercase
    if s == s.upper() and len(s) > 1:
        return s.title()
    return s

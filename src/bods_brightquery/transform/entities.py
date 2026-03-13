"""Transform BrightQuery organizations into BODS entity statements."""

from __future__ import annotations

from bods_brightquery.config import PublisherConfig
from bods_brightquery.ingestion.models import BQOrganization
from bods_brightquery.transform.identifiers import (
    build_entity_identifiers,
    entity_record_id,
    generate_statement_id,
)
from bods_brightquery.utils.statements import (
    build_publication_details,
    build_source,
    clean_statement,
)

# US jurisdiction for all BrightQuery entities
US_JURISDICTION = {"code": "US", "name": "United States of America"}


def build_entity_addresses(org: BQOrganization) -> list[dict]:
    """Build BODS address objects from a BrightQuery organization."""
    if not org.address_line1 and not org.city:
        return []

    address: dict = {"type": "registered"}

    # Build address string
    address["address"] = org.full_address

    if org.postal_code:
        address["postCode"] = org.postal_code

    address["country"] = US_JURISDICTION

    return [address]


def transform_organization(org: BQOrganization, config: PublisherConfig) -> dict:
    """Transform a BrightQuery organization into a BODS entity statement."""
    record_id = entity_record_id(org.bq_id)

    # Build names list
    names = []
    if org.name:
        names.append(org.name)
    if org.legal_name and org.legal_name != org.name:
        names.append(org.legal_name)

    # Build identifiers
    identifiers = build_entity_identifiers(
        bq_id=org.bq_id,
        lei_number=org.lei_number,
        npi_number=org.npi_number,
        other_ids=org.other_ids,
    )

    statement = {
        "statementId": generate_statement_id(record_id, config.publication_date),
        "declarationSubject": record_id,
        "statementDate": config.publication_date,
        "recordId": record_id,
        "recordType": "entity",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": org.name or org.legal_name,
            "alternateNames": names[1:] if len(names) > 1 else None,
            "jurisdiction": US_JURISDICTION,
            "identifiers": identifiers if identifiers else None,
            "addresses": build_entity_addresses(org) or None,
            "uri": org.website,
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source(config),
    }

    return clean_statement(statement)

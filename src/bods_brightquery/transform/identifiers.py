"""Record and statement ID generation for BrightQuery BODS pipeline."""

from __future__ import annotations

import uuid

# Unique namespace for BrightQuery BODS statement IDs (UUID v5)
BRIGHTQUERY_NAMESPACE = uuid.UUID("f3a1b7c2-9d4e-5f6a-8b7c-1d2e3f4a5b6c")


def entity_record_id(bq_id: str) -> str:
    """Generate a BODS record ID for a BrightQuery organization."""
    return f"brightquery-{bq_id}"


def person_record_id(bq_person_id: str, org_bq_id: str) -> str:
    """Generate a BODS record ID for a BrightQuery person.

    Includes org_bq_id because the same person may appear as a contact
    at multiple organizations, and each association is a distinct record.
    """
    return f"brightquery-person-{bq_person_id}-{org_bq_id}"


def relationship_record_id(subject_record_id: str, interested_party_record_id: str) -> str:
    """Generate a BODS record ID for a relationship."""
    return f"{subject_record_id}-rel-{interested_party_record_id}"


def generate_statement_id(
    record_id: str,
    statement_date: str,
    record_status: str = "new",
) -> str:
    """Generate a deterministic BODS statement ID using UUID v5.

    Same inputs always produce the same statement ID, ensuring
    reproducibility across pipeline runs.
    """
    name = f"{record_id}:{statement_date}:{record_status}"
    return str(uuid.uuid5(BRIGHTQUERY_NAMESPACE, name))


def build_entity_identifiers(
    bq_id: str,
    lei_number: str | None = None,
    npi_number: str | None = None,
    other_ids: dict[str, str] | None = None,
) -> list[dict]:
    """Build BODS identifier objects from BrightQuery data.

    Maps BrightQuery identifiers to appropriate BODS identifier schemes.
    """
    identifiers: list[dict] = []

    # BQ_ID as the primary identifier
    identifiers.append({
        "id": bq_id,
        "schemeName": "BrightQuery",
    })

    # LEI Number
    if lei_number:
        identifiers.append({
            "id": lei_number,
            "scheme": "XI-LEI",
            "schemeName": "Legal Entity Identifier",
        })

    # NPI Number (US healthcare)
    if npi_number:
        identifiers.append({
            "id": npi_number,
            "schemeName": "National Provider Identifier (NPI)",
        })

    if not other_ids:
        return identifiers

    # Map OTHER_ID_TYPE values to BODS identifiers
    for id_type, id_value in other_ids.items():
        if id_type == "CIK":
            identifiers.append({
                "id": id_value,
                "scheme": "US-SEC",
                "schemeName": "U.S. Securities and Exchange Commission",
            })
        elif id_type == "TICKER":
            identifiers.append({
                "id": id_value,
                "schemeName": "Stock Ticker",
            })
        elif id_type == "ISIN":
            identifiers.append({
                "id": id_value,
                "schemeName": "International Securities Identification Number",
            })
        elif id_type == "PERMID":
            identifiers.append({
                "id": id_value,
                "schemeName": "Refinitiv PermID",
            })
        elif id_type == "OPEN_FIGI":
            identifiers.append({
                "id": id_value,
                "schemeName": "OpenFIGI",
            })
        elif id_type == "SAM_UEI":
            identifiers.append({
                "id": id_value,
                "schemeName": "SAM.gov Unique Entity Identifier",
            })
        elif id_type == "SAM_CAGE":
            identifiers.append({
                "id": id_value,
                "schemeName": "SAM.gov CAGE Code",
            })
        elif id_type == "CAPIQ":
            identifiers.append({
                "id": id_value,
                "schemeName": "S&P Capital IQ",
            })
        elif id_type == "PITCHBOOK_ID":
            identifiers.append({
                "id": id_value,
                "schemeName": "PitchBook",
            })
        elif id_type == "CRUNCHBASE_URL":
            # Store as URI-based identifier
            identifiers.append({
                "id": id_value,
                "schemeName": "Crunchbase",
            })

    return identifiers

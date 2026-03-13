"""Transform BrightQuery person-organization contacts into BODS relationship statements."""

from __future__ import annotations

from bods_brightquery.config import PublisherConfig
from bods_brightquery.transform.identifiers import (
    generate_statement_id,
    relationship_record_id,
)
from bods_brightquery.utils.statements import (
    build_publication_details,
    build_source,
    clean_statement,
)


def transform_contact_relationship(
    entity_rec_id: str,
    person_rec_id: str,
    role: str,
    config: PublisherConfig,
) -> dict:
    """Transform a BrightQuery person-org contact into a BODS relationship statement.

    Since BrightQuery data only indicates "Contact" relationships without
    specifying ownership or control details, we use otherInfluenceOrControl
    as the interest type and set beneficialOwnershipOrControl to false.
    """
    rel_rec_id = relationship_record_id(entity_rec_id, person_rec_id)

    statement = {
        "statementId": generate_statement_id(rel_rec_id, config.publication_date),
        "declarationSubject": entity_rec_id,
        "statementDate": config.publication_date,
        "recordId": rel_rec_id,
        "recordType": "relationship",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "subject": entity_rec_id,
            "interestedParty": person_rec_id,
            "interests": [
                {
                    "type": "otherInfluenceOrControl",
                    "directOrIndirect": "unknown",
                    "beneficialOwnershipOrControl": False,
                    "details": f"BrightQuery role: {role}",
                }
            ],
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source(config),
    }

    return clean_statement(statement)

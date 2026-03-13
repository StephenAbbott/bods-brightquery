"""Data models for BrightQuery Senzing JSON records."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class BQOrganization:
    """A BrightQuery organization record parsed from Senzing JSON."""

    bq_id: str
    name: str | None = None
    legal_name: str | None = None
    address_line1: str | None = None
    address_line2: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    website: str | None = None
    linkedin: str | None = None
    npi_number: str | None = None
    lei_number: str | None = None
    placekey: str | None = None
    other_ids: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_senzing_record(cls, record: dict) -> BQOrganization | None:
        """Parse a Senzing JSON record into a BQOrganization.

        The record has a FEATURES array where each element is a dict
        containing one or more related fields.
        """
        record_id = record.get("RECORD_ID")
        if not record_id:
            return None

        features = record.get("FEATURES", [])

        name = None
        legal_name = None
        addr_line1 = None
        addr_line2 = None
        city = None
        state = None
        postal_code = None
        country = None
        lat = None
        lon = None
        website = None
        linkedin = None
        npi = None
        lei = None
        placekey = None
        bq_id = str(record_id)
        other_ids: dict[str, str] = {}

        for feat in features:
            if not isinstance(feat, dict) or not feat:
                continue

            # Names
            if "NAME_ORG" in feat:
                name_type = feat.get("NAME_TYPE", "PRIMARY")
                if name_type == "PRIMARY":
                    name = feat["NAME_ORG"]
                elif name_type == "LEGAL":
                    legal_name = feat["NAME_ORG"]

            # Address
            if "ADDR_LINE1" in feat:
                addr_line1 = feat.get("ADDR_LINE1")
                addr_line2 = feat.get("ADDR_LINE2")
                city = feat.get("ADDR_CITY")
                state = feat.get("ADDR_STATE")
                postal_code = feat.get("ADDR_POSTAL_CODE")
                country = feat.get("ADDR_COUNTRY")

            # Geo
            if "GEO_LATITUDE" in feat:
                lat = str(feat["GEO_LATITUDE"])
                lon = str(feat.get("GEO_LONGITUDE"))

            # Website / LinkedIn
            if "WEBSITE_ADDRESS" in feat:
                website = feat["WEBSITE_ADDRESS"]
            if "LINKEDIN" in feat:
                linkedin = feat["LINKEDIN"]

            # Specific identifiers
            if "NPI_NUMBER" in feat:
                npi = feat["NPI_NUMBER"]
            if "LEI_NUMBER" in feat:
                lei = feat["LEI_NUMBER"]

            # BQ_ID
            if "BQ_ID" in feat:
                bq_id = str(feat["BQ_ID"])

            # Placekey
            if "PLACEKEY" in feat:
                placekey = feat["PLACEKEY"]

            # OTHER_ID identifiers
            if "OTHER_ID_TYPE" in feat and "OTHER_ID_NUMBER" in feat:
                other_ids[feat["OTHER_ID_TYPE"]] = str(feat["OTHER_ID_NUMBER"])

        return cls(
            bq_id=bq_id,
            name=name,
            legal_name=legal_name,
            address_line1=addr_line1,
            address_line2=addr_line2,
            city=city,
            state=state,
            postal_code=postal_code,
            country=country,
            latitude=lat,
            longitude=lon,
            website=website,
            linkedin=linkedin,
            npi_number=npi,
            lei_number=lei,
            placekey=placekey,
            other_ids=other_ids,
        )

    @property
    def full_address(self) -> str | None:
        """Build a single-line address string."""
        parts = [
            self.address_line1,
            self.address_line2,
            self.city,
            self.state,
            self.postal_code,
            self.country,
        ]
        filtered = [p for p in parts if p]
        return ", ".join(filtered) if filtered else None


@dataclass
class BQPerson:
    """A BrightQuery people-business record parsed from Senzing JSON."""

    record_id: str
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    middle_name: str | None = None
    state: str | None = None
    country: str | None = None
    org_bq_id: str | None = None
    linkedin: str | None = None
    role: str = "Contact"

    @classmethod
    def from_senzing_record(cls, record: dict) -> BQPerson | None:
        """Parse a Senzing JSON record into a BQPerson."""
        record_id = record.get("RECORD_ID")
        if not record_id:
            return None

        features = record.get("FEATURES", [])

        full_name = None
        first_name = None
        last_name = None
        middle_name = None
        state = None
        country = None
        org_bq_id = None
        linkedin = None
        role = "Contact"

        for feat in features:
            if not isinstance(feat, dict) or not feat:
                continue

            # Full name
            if "NAME_FULL" in feat:
                full_name = feat["NAME_FULL"]

            # Parsed name parts
            if "NAME_FIRST" in feat:
                first_name = feat["NAME_FIRST"]
            if "NAME_LAST" in feat:
                last_name = feat["NAME_LAST"]
            if "NAME_MIDDLE" in feat:
                middle_name = feat["NAME_MIDDLE"]

            # Address (state/country only in people data)
            if "ADDR_STATE" in feat:
                state = feat["ADDR_STATE"]
            if "ADDR_COUNTRY" in feat:
                country = feat["ADDR_COUNTRY"]

            # Organization link
            if "GROUP_ASSN_ID_TYPE" in feat and feat.get("GROUP_ASSN_ID_TYPE") == "BQ_ID":
                org_bq_id = str(feat["GROUP_ASSN_ID_NUMBER"])

            # Relationship pointer (backup for org link)
            if "REL_POINTER_KEY" in feat:
                if org_bq_id is None:
                    org_bq_id = str(feat["REL_POINTER_KEY"])
                if "REL_POINTER_ROLE" in feat:
                    role = feat["REL_POINTER_ROLE"]

            # LinkedIn
            if "LINKEDIN" in feat:
                linkedin = feat["LINKEDIN"]

        return cls(
            record_id=str(record_id),
            full_name=full_name,
            first_name=first_name,
            last_name=last_name,
            middle_name=middle_name,
            state=state,
            country=country,
            org_bq_id=org_bq_id,
            linkedin=linkedin,
            role=role,
        )

    @property
    def has_name(self) -> bool:
        """Check if the person has any name data."""
        return bool(self.full_name or self.first_name or self.last_name)

"""Shared test fixtures for the bods-brightquery test suite."""

import json

import pytest


# Sample BrightQuery Senzing organization record
SAMPLE_ORG_RECORD = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "100001813958",
    "bq_dataset": "COMPANY",
    "FEATURES": [
        {"NAME_ORG": "CHARTER COMMUNICATIONS INC", "NAME_TYPE": "PRIMARY"},
        {"NAME_ORG": "CHARTER COMMUNICATIONS, CORP", "NAME_TYPE": "LEGAL"},
        {"RECORD_TYPE": "ORGANIZATION"},
        {
            "ADDR_CITY": "Charlotte",
            "ADDR_COUNTRY": "USA",
            "ADDR_LINE1": "7820 Crescent Executive Dr",
            "ADDR_POSTAL_CODE": "28217",
            "ADDR_STATE": "NC",
            "ADDR_TYPE": "BUSINESS",
        },
        {"GEO_LATITUDE": "35.1441", "GEO_LONGITUDE": "-80.91678"},
        {"REL_ANCHOR_DOMAIN": "BQ", "REL_ANCHOR_KEY": 100001813958},
        {"WEBSITE_ADDRESS": "https://corporate.charter.com"},
        {"LINKEDIN": "https://www.linkedin.com/company/charter-communications/"},
        {"OTHER_ID_NUMBER": "CHTR", "OTHER_ID_TYPE": "TICKER"},
        {"OTHER_ID_NUMBER": "0001091667", "OTHER_ID_TYPE": "CIK"},
        {"OTHER_ID_NUMBER": "4295915163", "OTHER_ID_TYPE": "PERMID"},
        {"OTHER_ID_NUMBER": "D872MLL5ENG3", "OTHER_ID_TYPE": "SAM_UEI"},
        {"OTHER_ID_NUMBER": "47PP3", "OTHER_ID_TYPE": "SAM_CAGE"},
        {"OTHER_ID_NUMBER": "39481778", "OTHER_ID_TYPE": "CAPIQ"},
        {"BQ_ID": "100001813958"},
        {"PLACEKEY": "14thhfx462@8gf-dv8-75z"},
    ],
}

# Simple org without many identifiers
SAMPLE_ORG_SIMPLE = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "100003806145",
    "bq_dataset": "COMPANY",
    "FEATURES": [
        {"NAME_ORG": "GWT SERVICES INC", "NAME_TYPE": "PRIMARY"},
        {"RECORD_TYPE": "ORGANIZATION"},
        {
            "ADDR_CITY": "Springfield",
            "ADDR_COUNTRY": "USA",
            "ADDR_LINE1": "6017 Horseview Dr",
            "ADDR_POSTAL_CODE": "62712",
            "ADDR_STATE": "IL",
            "ADDR_TYPE": "BUSINESS",
        },
        {"GEO_LATITUDE": "39.713070000000002", "GEO_LONGITUDE": "-89.629059999999996"},
        {"REL_ANCHOR_DOMAIN": "BQ", "REL_ANCHOR_KEY": 100003806145},
        {"BQ_ID": "100003806145"},
        {"PLACEKEY": "1l2fxickzo@5pk-8h9-hkf"},
    ],
}

# Sample person with full name
SAMPLE_PERSON_FULL = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "8880744527",
    "bq_dataset": "PEOPLE_BUSINESS",
    "FEATURES": [
        {"NAME_FULL": "ROSEMARY DEBUTTS"},
        {"NAME_FIRST": "ROSEMARY", "NAME_LAST": "DEBUTTS"},
        {"RECORD_TYPE": "PERSON"},
        {"ADDR_COUNTRY": "USA", "ADDR_STATE": "DC"},
        {"GROUP_ASSN_ID_NUMBER": "100000038057", "GROUP_ASSN_ID_TYPE": "BQ_ID"},
        {
            "REL_POINTER_DOMAIN": "BQ",
            "REL_POINTER_KEY": 100000038057,
            "REL_POINTER_ROLE": "Contact",
        },
        {"LINKEDIN": "linkedin.com/in/rosemary-debutts-33265726"},
    ],
}

# Sample person with null name (anonymous)
SAMPLE_PERSON_NULL_NAME = {
    "DATA_SOURCE": "BRIGHTQUERY",
    "RECORD_ID": "8859358678",
    "bq_dataset": "PEOPLE_BUSINESS",
    "FEATURES": [
        {"NAME_FULL": None},
        {},
        {"RECORD_TYPE": "PERSON"},
        {"ADDR_STATE": "LA"},
        {"GROUP_ASSN_ID_NUMBER": "100000353702", "GROUP_ASSN_ID_TYPE": "BQ_ID"},
        {
            "REL_POINTER_DOMAIN": "BQ",
            "REL_POINTER_KEY": 100000353702,
            "REL_POINTER_ROLE": "Contact",
        },
        {"LINKEDIN": "https://www.linkedin.com/in/shane-smith-1017608"},
    ],
}


def write_jsonl(records: list[dict], filepath) -> None:
    """Write records as JSONL to a file."""
    with open(filepath, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

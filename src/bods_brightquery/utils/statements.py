"""Builders for BODS statement envelope fields."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bods_brightquery.config import PublisherConfig


def build_publication_details(config: PublisherConfig) -> dict:
    """Build the BODS publicationDetails object."""
    details: dict = {
        "publicationDate": config.publication_date,
        "bodsVersion": "0.4",
        "publisher": {
            "name": config.publisher_name,
        },
    }
    if config.publisher_uri:
        details["publisher"]["uri"] = config.publisher_uri
    if config.license_url:
        details["license"] = config.license_url
    return details


def build_source(config: PublisherConfig) -> dict:
    """Build a BODS source object for BrightQuery/OpenData.org data."""
    source: dict = {
        "type": ["officialRegister"],
        "description": (
            "BrightQuery OpenData.org - data sourced from U.S. government "
            "agencies and verified regulatory filings"
        ),
        "url": "https://opendata.org",
        "assertedBy": [
            {
                "name": "BrightQuery / OpenData.org",
                "uri": "https://opendata.org",
            }
        ],
    }
    if config.retrieved_at:
        source["retrievedAt"] = config.retrieved_at
    return source


def clean_statement(statement: dict) -> dict:
    """Remove None values and empty collections from a BODS statement."""
    cleaned = {}
    for key, value in statement.items():
        if value is None:
            continue
        if isinstance(value, dict):
            nested = clean_statement(value)
            if nested:
                cleaned[key] = nested
        elif isinstance(value, list):
            cleaned_list = []
            for item in value:
                if isinstance(item, dict):
                    nested_item = clean_statement(item)
                    if nested_item:
                        cleaned_list.append(nested_item)
                elif item is not None:
                    cleaned_list.append(item)
            if cleaned_list:
                cleaned[key] = cleaned_list
        else:
            cleaned[key] = value
    return cleaned

"""Publisher configuration for the BODS BrightQuery pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field

from bods_brightquery.utils.dates import current_date_iso, current_datetime_iso


@dataclass
class PublisherConfig:
    """Configuration for the BODS publication metadata."""

    publisher_name: str = "BODS BrightQuery Pipeline"
    publisher_uri: str | None = "https://opendata.org"
    license_url: str = "https://creativecommons.org/publicdomain/zero/1.0/"
    publication_date: str = field(default_factory=current_date_iso)
    retrieved_at: str | None = field(default_factory=current_datetime_iso)
    output_path: str = "output.jsonl"
    output_format: str = "jsonl"

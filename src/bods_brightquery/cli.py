"""Command-line interface for the BODS BrightQuery pipeline.

Usage:
    # Transform a single organization file
    bods-bq transform --organizations org_file.json -o output.jsonl

    # Transform organizations and people from directories
    bods-bq transform --organizations ~/Desktop/Organization --people ~/Desktop/PeopleBusiness -o output.jsonl

    # Output as JSON array (small datasets)
    bods-bq transform --organizations org.json --people people.json -o output.json -f json
"""

from __future__ import annotations

import logging
import sys

import click

from bods_brightquery.config import PublisherConfig
from bods_brightquery.pipeline import BODSPipeline


@click.group()
@click.option(
    "--verbose", "-v",
    is_flag=True,
    default=False,
    help="Enable verbose logging.",
)
@click.option(
    "--quiet", "-q",
    is_flag=True,
    default=False,
    help="Suppress all output except errors.",
)
def main(verbose: bool, quiet: bool) -> None:
    """Transform BrightQuery/OpenData.org data into BODS v0.4 format."""
    level = logging.WARNING
    if verbose:
        level = logging.DEBUG
    elif not quiet:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )


@main.command("transform")
@click.option(
    "--organizations",
    type=click.Path(exists=True),
    help="Path to organizations JSONL file or directory.",
)
@click.option(
    "--people",
    type=click.Path(exists=True),
    help="Path to people-business JSONL file or directory.",
)
@click.option(
    "--output", "-o",
    type=click.Path(),
    default="output.jsonl",
    help="Output file path. Use '-' for stdout.",
)
@click.option(
    "--format", "-f",
    "output_format",
    type=click.Choice(["json", "jsonl"]),
    default="jsonl",
    help="Output format (default: jsonl for large data).",
)
@click.option(
    "--publisher-name",
    default="BODS BrightQuery Pipeline",
    help="Publisher name for BODS metadata.",
)
def transform(
    organizations: str | None,
    people: str | None,
    output: str,
    output_format: str,
    publisher_name: str,
) -> None:
    """Transform BrightQuery Senzing JSONL data to BODS format."""
    if not any([organizations, people]):
        raise click.UsageError(
            "At least one of --organizations or --people must be specified."
        )

    config = PublisherConfig(
        publisher_name=publisher_name,
        output_path=output,
        output_format=output_format,
    )

    pipeline = BODSPipeline(config)

    try:
        # Process organizations first (entity statements),
        # then people (person + relationship statements).
        if organizations:
            pipeline.process_organizations(organizations)

        if people:
            pipeline.process_people(people)

        pipeline.finalize()

    except Exception as e:
        logging.getLogger(__name__).error("Pipeline error: %s", e)
        raise click.ClickException(str(e))


if __name__ == "__main__":
    main()

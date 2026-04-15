# bods-brightquery

Transform [BrightQuery](https://brightquery.com/) / OpenData.org data into [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/) v0.4 format.

## Overview

This pipeline ingests BrightQuery Senzing JSONL data (organizations and people-business records) and produces BODS v0.4 compliant statements, including:

- **Entity statements** for organizations
- **Person statements** for individuals linked to businesses
- **Ownership-or-control statements** linking persons to entities

## Installation

```bash
pip install .
```

For development (includes pytest):

```bash
pip install ".[dev]"
```

## Usage

### Transform organization data

```bash
bods-bq transform --organizations org_file.json -o output.jsonl
```

### Transform organizations and people from directories

```bash
bods-bq transform --organizations ~/data/Organization --people ~/data/PeopleBusiness -o output.jsonl
```

### Output as JSON array (for smaller datasets)

```bash
bods-bq transform --organizations org.json --people people.json -o output.json -f json
```

### Options

| Flag | Description |
|------|-------------|
| `--organizations` | Path to organizations JSONL file or directory |
| `--people` | Path to people-business JSONL file or directory |
| `-o`, `--output` | Output file path (default: `output.jsonl`) |
| `-f`, `--format` | Output format: `json` or `jsonl` |
| `--publisher-name` | Publisher name for BODS metadata |
| `-v`, `--verbose` | Enable verbose logging |
| `-q`, `--quiet` | Suppress all output except errors |

## Project Structure

```
src/bods_brightquery/
├── ingestion/       # JSONL reading and data models
├── transform/       # BODS statement generation (entities, persons, relationships, identifiers)
├── output/          # Statement serialisation (JSON/JSONL)
├── utils/           # Date handling and statement helpers
├── pipeline.py      # Orchestrates ingestion -> transform -> output
└── cli.py           # Click CLI entry point
```

## Testing

```bash
pytest
```

## License

MIT

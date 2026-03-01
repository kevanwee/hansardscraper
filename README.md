# hansardscraper

A reliable Singapore Parliamentary Hansard scraper that pulls daily report JSON from the SPRS endpoint and stores normalized records in an incremental Excel master file.

## What this project does

- Scrapes Hansard report data by date from `https://sprs.parl.gov.sg/search/getHansardReport/`.
- Parses section HTML content into clean text.
- Stores one row per sitting date in `hansard_master.xlsx`.
- Supports incremental updates by continuing from the latest date already in the master file.
- Handles non-sitting dates gracefully.

## Architecture

- Full architecture and component flow: [docs/architecture.md](docs/architecture.md)

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run incremental scraping (default):

```bash
python hansardscrape.py
```

Run for a specific range:

```bash
python hansardscrape.py --start-date 01-01-2025 --end-date 31-12-2025
```

Run full rescrape from default baseline (`01-01-2020`):

```bash
python hansardscrape.py --full-rescrape
```

Use a custom output file:

```bash
python hansardscrape.py --master-file my_hansard.xlsx
```

## CLI Options

- `--start-date DD-MM-YYYY`: Start date override.
- `--end-date DD-MM-YYYY`: End date override (default: today).
- `--master-file PATH`: Output Excel file path (default: `hansard_master.xlsx`).
- `--full-rescrape`: Ignore incremental behavior and scrape from baseline start date.
- `--sleep-seconds FLOAT`: Delay between API requests (default: `0.2`).

## Output Schema

Each row in the master file contains:

- `Date`
- `ParliamentNo`
- `SessionNo`
- `VolumeNo`
- `SittingNo`
- `SittingType`
- `Language`
- `SectionCount`
- `AttendanceCount`
- `PTBACount`
- `VernacularDocCount`
- `SectionTitles`
- `DebateText`

## Notes

- The API may return `HTTP 500` for dates without sittings; this script treats those as no-data days.
- Data is deduplicated by `Date` before writing.
- Excel writing requires `openpyxl`.
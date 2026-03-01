from __future__ import annotations

import argparse
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

API_URL = "https://sprs.parl.gov.sg/search/getHansardReport/"
DEFAULT_MASTER_FILE = "hansard_master.xlsx"
DEFAULT_START_DATE = "01-01-2020"
DATE_FORMAT = "%d-%m-%Y"
EXCEL_CELL_CHAR_LIMIT = 32767


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Singapore Hansard reports into an incremental Excel master file."
    )
    parser.add_argument(
        "--start-date",
        help=f"Start date in DD-MM-YYYY. Defaults to {DEFAULT_START_DATE} or next date after existing data.",
    )
    parser.add_argument(
        "--end-date",
        help="End date in DD-MM-YYYY. Defaults to today's date.",
    )
    parser.add_argument(
        "--master-file",
        default=DEFAULT_MASTER_FILE,
        help=f"Excel output file path (default: {DEFAULT_MASTER_FILE}).",
    )
    parser.add_argument(
        "--full-rescrape",
        action="store_true",
        help="Ignore existing records and scrape from start date.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="Delay between requests in seconds (default: 0.2).",
    )
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, DATE_FORMAT).date()


def iter_dates(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    value = re.sub(r"\\[rnt]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def clamp_for_excel(value: Any, limit: int = EXCEL_CELL_CHAR_LIMIT) -> Any:
    if isinstance(value, str) and len(value) > limit:
        return value[:limit]
    return value


def html_to_text(html: str | None) -> str:
    if not html:
        return ""
    return clean_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))


def fetch_report(session: requests.Session, sitting_date: str) -> dict[str, Any] | None:
    try:
        response = session.get(
            API_URL,
            params={"sittingDate": sitting_date},
            timeout=30,
        )
    except requests.RequestException as exc:
        print(f"[WARN] Request failed for {sitting_date}: {exc}")
        return None

    if response.status_code >= 500:
        # For this endpoint, 500 is often returned for non-sitting days.
        print(f"[INFO] No report for {sitting_date} (HTTP {response.status_code}).")
        return None

    if response.status_code != 200:
        print(f"[WARN] Unexpected HTTP {response.status_code} for {sitting_date}.")
        return None

    try:
        payload = response.json()
    except ValueError:
        print(f"[WARN] Invalid JSON for {sitting_date}.")
        return None

    if isinstance(payload, dict) and payload.get("errorCode"):
        print(f"[INFO] No report for {sitting_date} ({payload.get('description', 'API error')}).")
        return None

    return payload


def build_row(sitting_date: str, payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    sections = payload.get("takesSectionVOList", []) if isinstance(payload, dict) else []

    section_texts: list[str] = []
    section_titles: list[str] = []

    for section in sections:
        if not isinstance(section, dict):
            continue
        title = clean_text(section.get("title"))
        subtitle = clean_text(section.get("subTitle"))
        text = html_to_text(section.get("content"))
        if not text:
            continue

        full_title = " - ".join(part for part in [title, subtitle] if part)
        if full_title:
            section_titles.append(full_title)
        section_texts.append(text)

    return {
        "Date": sitting_date,
        "ParliamentNo": metadata.get("parlimentNO"),
        "SessionNo": metadata.get("sessionNO"),
        "VolumeNo": metadata.get("volumeNO"),
        "SittingNo": metadata.get("sittingNO"),
        "SittingType": clean_text(metadata.get("sittingType")),
        "Language": clean_text(metadata.get("language")),
        "SectionCount": len(section_texts),
        "AttendanceCount": len(payload.get("attendanceList", [])),
        "PTBACount": len(payload.get("ptbaList", [])),
        "VernacularDocCount": len(payload.get("vernacularList", [])),
        "SectionTitles": " || ".join(section_titles),
        "DebateText": "\n\n".join(section_texts),
    }


def load_existing_dates(master_file: str) -> tuple[pd.DataFrame, set[str]]:
    if not os.path.exists(master_file):
        return pd.DataFrame(), set()

    try:
        df = pd.read_excel(master_file)
    except Exception as exc:
        raise RuntimeError(f"Failed to read existing master file '{master_file}': {exc}") from exc

    if "Date" not in df.columns:
        return df, set()

    existing_dates = set(df["Date"].astype(str).str.strip())
    return df, existing_dates


def determine_start_date(args: argparse.Namespace, existing_df: pd.DataFrame) -> date:
    if args.start_date:
        return parse_date(args.start_date)

    if args.full_rescrape:
        return parse_date(DEFAULT_START_DATE)

    if not existing_df.empty and "Date" in existing_df.columns:
        parsed = pd.to_datetime(existing_df["Date"], dayfirst=True, errors="coerce").dropna()
        if not parsed.empty:
            latest = parsed.max().date()
            return latest + timedelta(days=1)

    return parse_date(DEFAULT_START_DATE)


def save_master(master_file: str, existing_df: pd.DataFrame, new_rows: list[dict[str, Any]]) -> None:
    new_df = pd.DataFrame(new_rows)

    if existing_df.empty:
        combined = new_df
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)

    if "Date" in combined.columns:
        combined["DateSort"] = pd.to_datetime(combined["Date"], dayfirst=True, errors="coerce")
        combined = (
            combined.sort_values(by=["DateSort", "Date"], na_position="last")
            .drop(columns=["DateSort"])
            .drop_duplicates(subset=["Date"], keep="last")
        )

    # Excel cells have a hard limit of 32,767 characters.
    for col in combined.columns:
        combined[col] = combined[col].map(clamp_for_excel)

    combined.to_excel(master_file, index=False)


def main() -> int:
    args = parse_args()

    try:
        end_date = parse_date(args.end_date) if args.end_date else datetime.today().date()
    except ValueError as exc:
        print(f"[ERROR] Invalid --end-date: {exc}")
        return 1

    try:
        existing_df, existing_dates = load_existing_dates(args.master_file)
    except RuntimeError as exc:
        print(f"[ERROR] {exc}")
        return 1

    try:
        start_date = determine_start_date(args, existing_df)
    except ValueError as exc:
        print(f"[ERROR] Invalid --start-date: {exc}")
        return 1

    if start_date > end_date:
        print(
            f"[INFO] Nothing to do. Start date {start_date.strftime(DATE_FORMAT)} is after "
            f"end date {end_date.strftime(DATE_FORMAT)}."
        )
        return 0

    print(
        f"[INFO] Scraping from {start_date.strftime(DATE_FORMAT)} to {end_date.strftime(DATE_FORMAT)} "
        f"into {args.master_file}"
    )

    new_rows: list[dict[str, Any]] = []
    scraped_dates = 0
    skipped_existing = 0

    with requests.Session() as session:
        for current_date in iter_dates(start_date, end_date):
            date_str = current_date.strftime(DATE_FORMAT)

            if not args.full_rescrape and date_str in existing_dates:
                skipped_existing += 1
                continue

            payload = fetch_report(session, date_str)
            if not payload:
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
                continue

            row = build_row(date_str, payload)
            new_rows.append(row)
            scraped_dates += 1
            print(f"[OK] Scraped {date_str} ({row['SectionCount']} sections).")

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    if new_rows:
        save_master(args.master_file, existing_df, new_rows)
        print(f"[OK] Saved {len(new_rows)} new records to {args.master_file}.")
    else:
        print("[INFO] No new records found in the selected date range.")

    print(f"[INFO] Summary: scraped={scraped_dates}, skipped_existing={skipped_existing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# common/manual_push_to_bq.py

import argparse
from datetime import datetime, timedelta
import pandas as pd
import traceback
import sys
import os
import time
from typing import List, Optional, Tuple
from google.cloud import secretmanager, bigquery
import requests

# Ensure current directory is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    import gather_all
except ImportError:
    try:
        import common.gather_all as gather_all
    except ImportError as e:
        print(f"FATAL: Could not import gather_all.py. Details: {e}")
        sys.exit(1)

PROJECT_ID = "ums-adreal-471711"
TABLE_ID = f"{PROJECT_ID}.DLG.DataImport"

# ---------------- Secrets ----------------

def access_secret(secret_id: str, version_id: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

# ---------------- Dates ----------------

def get_month_range(year: int, month: int) -> Tuple[str, str]:
    start_date = datetime(year, month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date = next_month - timedelta(days=next_month.day)
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

def get_manual_period_info(year: int, month: int) -> Tuple[str, str]:
    start_date, _ = get_month_range(year, month)
    adreal_period = f"month_{start_date}"
    date_string = datetime(year, month, 1).strftime("%Y-%m-01")
    return adreal_period, date_string

# ---------------- Cleaning ----------------

def clean_manual_data(df: pd.DataFrame, date_string: str) -> pd.DataFrame:
    df = gather_all.clean_data(df)
    df["Date"] = date_string
    df["ContentType"] = df["MediaChannel"].apply(gather_all.decide_content_type)

    expected_columns = ["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaChannel", "AdContacts"]
    df = df.reindex(columns=expected_columns)

    df["AdContacts"] = pd.to_numeric(df["AdContacts"], errors="coerce").fillna(0).astype(int)
    return df

# ---------------- 403 diagnosis + brand filtering ----------------

def _is_permission_403(e: Exception) -> bool:
    if not isinstance(e, requests.HTTPError):
        return False
    r = e.response
    if r is None or r.status_code != 403:
        return False
    try:
        # Gemius returns {"detail":"You do not have permission to perform this action."}
        return "permission" in (r.text or "").lower()
    except Exception:
        return True

def _try_fetch_stats(
    username: str,
    password: str,
    market: str,
    start: str,
    end: str,
    brand_ids: List[str],
    platforms: str,
    page_types: str,
    segments: str,
    limit: int,
):
    adreal_fetcher = gather_all.AdRealFetcher(username=username, password=password, market=market)
    adreal_fetcher.login()
    adreal_fetcher.period_range = f"{start},{end},month"
    adreal_fetcher.period_label = adreal_fetcher._period_label_from_range(adreal_fetcher.period_range)

    # This call prints your "GET ---> ..." line and raises HTTPError on 403
    return adreal_fetcher.fetch_data(
        brand_ids,
        platforms=platforms,
        page_types=page_types,
        segments=segments,
        limit=limit
    )

def _filter_allowed_brand_ids(
    username: str,
    password: str,
    market: str,
    start: str,
    end: str,
    brand_ids: List[str],
    platforms: str,
    page_types: str,
    segments: str,
    limit: int,
) -> Tuple[List[str], List[str]]:
    """
    Returns (allowed_ids, forbidden_ids) by probing the API.
    Strategy:
      - If full list works => all allowed
      - If full list 403 => split into halves and recurse
      - When a single ID 403 => forbidden
    """
    def probe(ids: List[str]) -> Tuple[List[str], List[str]]:
        if not ids:
            return [], []
        try:
            _ = _try_fetch_stats(
                username, password, market, start, end, ids,
                platforms=platforms, page_types=page_types,
                segments=segments, limit=limit
            )
            return ids, []
        except Exception as e:
            if not _is_permission_403(e):
                raise
            if len(ids) == 1:
                return [], ids
            mid = len(ids) // 2
            left_allowed, left_forbidden = probe(ids[:mid])
            right_allowed, right_forbidden = probe(ids[mid:])
            return left_allowed + right_allowed, left_forbidden + right_forbidden

    allowed, forbidden = probe(brand_ids)
    return allowed, forbidden

# ---------------- Main fetch ----------------

def fetch_adreal_manual(
    username: str,
    password: str,
    year: int,
    month: int,
    parent_brand_ids: Optional[List[str]] = None,
    market: str = "ro",
) -> pd.DataFrame:
    if parent_brand_ids is None:
        parent_brand_ids = []

    adreal_period, date_string = get_manual_period_info(year, month)
    print(f"Fetching data for period {adreal_period} ({date_string})")

    # Brands & Publishers (these are OK)
    brand_fetcher = gather_all.BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=adreal_period)

    publisher_fetcher = gather_all.PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=adreal_period)

    # Stats
    start, end = get_month_range(year, month)

    platforms = "pc"
    page_types = "search,social,standard"
    segments = "brand,product,content_type,website"
    limit = 1000000

    try:
        stats_data = _try_fetch_stats(
            username, password, market, start, end, parent_brand_ids,
            platforms=platforms, page_types=page_types, segments=segments, limit=limit
        )
    except Exception as e:
        if not _is_permission_403(e):
            raise

        print("\n❌ 403 permission on full DLG brand list.")
        print("➡️  Auto-diagnosing which brand IDs are forbidden... (this may take a bit)\n")

        allowed_ids, forbidden_ids = _filter_allowed_brand_ids(
            username, password, market, start, end, parent_brand_ids,
            platforms=platforms, page_types=page_types, segments=segments, limit=limit
        )

        print("\n✅ Allowed brand IDs:", ",".join(allowed_ids) if allowed_ids else "(none)")
        print("⛔ Forbidden brand IDs:", ",".join(forbidden_ids) if forbidden_ids else "(none)")
        print("➡️  Proceeding with allowed IDs only.\n")

        if not allowed_ids:
            raise RuntimeError(
                "All provided brand IDs are forbidden for /stats. "
                "You need Gemius permissions for these brand IDs."
            )

        # Now fetch actual stats with allowed IDs
        stats_data = _try_fetch_stats(
            username, password, market, start, end, allowed_ids,
            platforms=platforms, page_types=page_types, segments=segments, limit=limit
        )

    merged_rows = gather_all.merge_data(stats_data, brands_data, websites_data)
    df = pd.DataFrame(merged_rows).drop_duplicates()
    df = clean_manual_data(df, date_string)
    return df

# ---------------- BigQuery push ----------------

def push_to_bigquery(df: pd.DataFrame, year: int, month: int):
    client = bigquery.Client()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # Validate schema compatibility
    table = client.get_table(TABLE_ID)
    table_cols = {s.name for s in table.schema}
    extra_cols = [c for c in df.columns if c not in table_cols]
    if extra_cols:
        raise ValueError(f"DataFrame has columns not in BQ schema for {TABLE_ID}: {extra_cols}")

    # Only delete after we successfully fetched data
    delete_query = f"""
    DELETE FROM `{TABLE_ID}`
    WHERE EXTRACT(YEAR FROM Date) = {year}
      AND EXTRACT(MONTH FROM Date) = {month}
    """
    print(f"Deleting existing rows for {year}-{month} from {TABLE_ID}...")
    client.query(delete_query).result()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"Inserted {len(df)} rows into BigQuery for {year}-{month}")

# ---------------- Entrypoint ----------------

def main():
    parser = argparse.ArgumentParser(description="Fetch AdReal data for a specific month and push to BigQuery.")
    parser.add_argument("year", type=int, help="Year (e.g., 2025)")
    parser.add_argument("month", type=int, help="Month (1-12)")
    args = parser.parse_args()

    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        # DLG parent brand IDs (as you provided)
        parent_brand_ids = [
            # Braun
            "95300", "91130", "98190", "88586", "53389", "96897", "88685",
            # DeLonghi
            "93674", "88597", "2531", "1488", "98224", "96924",
            # Kenwood
            "91516", "98006", "27428", "13098",
            # Nutribullet
            "96381", "96128", "88599", "97049", "97915", "98115", "93961"
        ]

        print("TARGET TABLE:", TABLE_ID)

        df = fetch_adreal_manual(
            username, password, args.year, args.month,
            parent_brand_ids=parent_brand_ids,
            market="ro"
        )

        if df.empty:
            print(f"No data for {args.year}-{args.month}. Nothing to push.")
            return

        print(f"Fetched data for {args.year}-{args.month}, shape: {df.shape}")
        print("DF columns:", df.columns.tolist())
        print(df.head(5).to_string(index=False))

        push_to_bigquery(df, args.year, args.month)

    except Exception:
        print("FATAL ERROR:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
# common/manual_push_to_bq.py

import argparse
from datetime import datetime, timedelta
import pandas as pd
import traceback
import sys
import os
import time
from typing import List, Optional
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

# ------------ Helpers ------------

def access_secret(secret_id: str, version_id: str = "latest") -> str:
    """Fetch a secret from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def get_month_range(year: int, month: int):
    """Return (start_date, end_date) in YYYYMMDD format for a given month."""
    start_date = datetime(year, month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)  # always in next month
    end_date = next_month - timedelta(days=next_month.day)
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

def get_manual_period_info(year: int, month: int):
    """Return AdReal period string and date string for a manual month."""
    start_date, _ = get_month_range(year, month)
    adreal_period = f"month_{start_date}"
    date_string = datetime(year, month, 1).strftime("%Y-%m-01")
    return adreal_period, date_string

def clean_manual_data(df: pd.DataFrame, date_string: str) -> pd.DataFrame:
    """Clean and reformat merged DataFrame, forcing the manual date."""
    df = gather_all.clean_data(df)

    # Force month date
    df["Date"] = date_string

    # Ensure ContentType is consistent (from MediaChannel)
    df["ContentType"] = df["MediaChannel"].apply(gather_all.decide_content_type)

    expected_columns = ["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaChannel", "AdContacts"]
    df = df.reindex(columns=expected_columns)

    df["AdContacts"] = pd.to_numeric(df["AdContacts"], errors="coerce").fillna(0).astype(int)
    return df

def _print_http_debug(err: requests.HTTPError):
    """Print extra diagnostics for HTTP errors."""
    resp = err.response
    try:
        body = resp.text
    except Exception:
        body = "<could not read body>"
    print("\n--- HTTP DEBUG ---")
    print("Status:", resp.status_code)
    print("URL:", resp.url)
    print("Response headers:", dict(resp.headers))
    print("Response body (first 2000 chars):")
    print(body[:2000])
    print("--- END HTTP DEBUG ---\n")

def fetch_adreal_manual(
    username: str,
    password: str,
    year: int,
    month: int,
    parent_brand_ids: Optional[List[str]] = None,
    market: str = "ro",
    retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch, merge, clean AdReal data for a manual month.
    Retries the /stats call on 403 with re-login + backoff and prints response details.
    """
    if parent_brand_ids is None:
        parent_brand_ids = []

    adreal_period, date_string = get_manual_period_info(year, month)
    print(f"Fetching data for period {adreal_period} ({date_string})")

    # Fetch brands & websites (these already succeed for you)
    brand_fetcher = gather_all.BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=adreal_period)

    publisher_fetcher = gather_all.PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=adreal_period)

    # Stats fetch (this is where you fail)
    start, end = get_month_range(year, month)

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            adreal_fetcher = gather_all.AdRealFetcher(username=username, password=password, market=market)
            adreal_fetcher.login()

            # Ensure the fetcher is configured consistently
            adreal_fetcher.period_range = f"{start},{end},month"
            adreal_fetcher.period_label = adreal_fetcher._period_label_from_range(adreal_fetcher.period_range)

            # Perform stats request
            stats_data = adreal_fetcher.fetch_data(
                parent_brand_ids,
                platforms="pc,mobile",
                page_types="search,social,standard",
                segments="brand,product,content_type,website",
                limit=1000000,
            )

            merged_rows = gather_all.merge_data(stats_data, brands_data, websites_data)
            df = pd.DataFrame(merged_rows).drop_duplicates()
            df = clean_manual_data(df, date_string)
            return df

        except requests.HTTPError as e:
            last_exc = e
            if e.response is not None and e.response.status_code == 403:
                print(f"\n⚠️ 403 Forbidden on /stats (attempt {attempt}/{retries}).")
                _print_http_debug(e)

                # Backoff then retry (re-login happens each loop)
                sleep_s = 3 * attempt
                print(f"Retrying after {sleep_s}s...\n")
                time.sleep(sleep_s)
                continue
            raise  # other HTTP errors should bubble up

        except Exception as e:
            last_exc = e
            print(f"\n⚠️ Unexpected error on attempt {attempt}/{retries}: {type(e).__name__}: {e}")
            if attempt < retries:
                time.sleep(2 * attempt)
                continue
            raise

    # If we got here, all retries failed
    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to fetch AdReal manual data for unknown reasons.")

def push_to_bigquery(df: pd.DataFrame, year: int, month: int):
    """Load DataFrame into BigQuery, replacing only the current month."""
    client = bigquery.Client()

    # Make Date a proper DATE
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # Safety: verify table schema columns exist
    table = client.get_table(TABLE_ID)
    table_cols = {s.name for s in table.schema}
    missing = [c for c in df.columns if c not in table_cols]
    if missing:
        raise ValueError(f"DataFrame has columns not in table schema {TABLE_ID}: {missing}")

    # Delete old rows for the month (only after we have data)
    delete_query = f"""
    DELETE FROM `{TABLE_ID}`
    WHERE EXTRACT(YEAR FROM Date) = {year}
      AND EXTRACT(MONTH FROM Date) = {month}
    """
    print(f"Deleting existing rows for {year}-{month} from {TABLE_ID}...")
    client.query(delete_query).result()

    # Load new rows
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"Inserted {len(df)} rows into {TABLE_ID} for {year}-{month}")

def main():
    parser = argparse.ArgumentParser(description="Fetch AdReal data for a specific month and push to BigQuery.")
    parser.add_argument("year", type=int, help="Year (e.g., 2025)")
    parser.add_argument("month", type=int, help="Month (1-12)")
    args = parser.parse_args()

    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        parent_brand_ids = [
            # Braun
            "95300", "91130", "98190", "88586", "53389", "96897", "88685",
            # DeLonghi
            "93674", "88597", "2531", "1488", "98224",
            # Kenwood
            "91516", "98006", "27428", "13098",
            # Nutribullet
            "96381", "96128", "88599", "97049", "97915", "98115", "93961",
        ]

        print("TARGET TABLE:", TABLE_ID)

        df = fetch_adreal_manual(
            username, password, args.year, args.month,
            parent_brand_ids=parent_brand_ids,
            market="ro",
            retries=3
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

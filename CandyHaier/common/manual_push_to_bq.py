# common/manual_push_to_bq.py

import argparse
from datetime import datetime, timedelta
import pandas as pd
import traceback
import sys
import os
from google.cloud import secretmanager, bigquery

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
TABLE_ID = f"{PROJECT_ID}.CandyHaier.DataImport"

def access_secret(secret_id, version_id="latest"):
    """Fetch a secret from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def get_month_range(year, month):
    """Return (start_date, end_date) in YYYYMMDD format for a given month."""
    start_date = datetime(year, month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)  # always in next month
    end_date = next_month - timedelta(days=next_month.day)
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

def get_manual_period_info(year, month):
    """Return AdReal period string and date string for a manual month."""
    start_date, _ = get_month_range(year, month)
    adreal_period = f"month_{start_date}"
    date_string = datetime(year, month, 1).strftime('%Y-%m-01')
    return adreal_period, date_string

def clean_manual_data(df, date_string):
    """Clean and reformat merged DataFrame, forcing the manual date."""
    df = gather_all.clean_data(df)
    df['Date'] = date_string
    df['ContentType'] = df['MediaChannel'].apply(gather_all.decide_content_type)

    # Ensure correct column order
    expected_columns = ["Date", "BrandOwner", "Brand", "ContentType", "MediaOwner", "MediaChannel", "AdContacts"]
    if "Product" in df.columns:
        expected_columns.insert(3, "Product")
    df = df.reindex(columns=expected_columns)

    if "AdContacts" in df.columns:
        df["AdContacts"] = pd.to_numeric(df["AdContacts"], errors="coerce").fillna(0).astype(int)
    return df

def fetch_adreal_manual(username, password, year, month, parent_brand_ids=None, market="ro"):
    """Fetch, merge, clean AdReal data for a manual month."""
    if parent_brand_ids is None:
        parent_brand_ids = []

    adreal_period, date_string = get_manual_period_info(year, month)
    print(f"Fetching data for period {adreal_period} ({date_string})")

    # Fetch brands & websites
    brand_fetcher = gather_all.BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=adreal_period)

    publisher_fetcher = gather_all.PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=adreal_period)

    # Fetch stats
    adreal_fetcher = gather_all.AdRealFetcher(username=username, password=password, market=market)
    adreal_fetcher.login()
    start, end = get_month_range(year, month)
    adreal_fetcher.period_range = f"{start},{end},month"
    adreal_fetcher.period_label = adreal_fetcher._period_label_from_range(adreal_fetcher.period_range)

    stats_data = adreal_fetcher.fetch_data(
        parent_brand_ids,
        platforms="pc",
        page_types="search,social,standard",
        segments="brand,product,content_type,website",
        limit=1000000
    )

    merged_rows = gather_all.merge_data(stats_data, brands_data, websites_data)
    df = pd.DataFrame(merged_rows).drop_duplicates()
    df = clean_manual_data(df, date_string)
    return df

def push_to_bigquery(df, year, month):
    """Load DataFrame into BigQuery, replacing only the current month."""
    client = bigquery.Client()

    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # Drop columns not in table schema
    df = df.drop(columns=["MediaOwner"], errors="ignore")

    # Delete old rows for the month
    delete_query = f"""
    DELETE FROM `{TABLE_ID}`
    WHERE EXTRACT(YEAR FROM Date) = {year}
      AND EXTRACT(MONTH FROM Date) = {month}
    """
    print(f"Deleting existing rows for {year}-{month}...")
    client.query(delete_query).result()

    # Load new rows
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"Inserted {len(df)} rows into BigQuery for {year}-{month}")

def main():
    parser = argparse.ArgumentParser(description="Fetch AdReal data for a specific month and push to BigQuery.")
    parser.add_argument("year", type=int, help="Year (e.g., 2025)")
    parser.add_argument("month", type=int, help="Month (1-12)")
    args = parser.parse_args()

    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        parent_brand_ids = ["76815", "1056", "947", "2126", "12988", "15651", "1708", "15875", "1551", "92605", "1248", "17575", "90577", "91050", "5298", "35135"]

        # Fetch AdReal data for the requested month
        df = fetch_adreal_manual(username, password, args.year, args.month, parent_brand_ids=parent_brand_ids)

        if df.empty:
            print(f"No data for {args.year}-{args.month}. Nothing to push.")
            return

        print(f"Fetched data for {args.year}-{args.month}, shape: {df.shape}")
        push_to_bigquery(df, args.year, args.month)

    except Exception as e:
        print("FATAL ERROR:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

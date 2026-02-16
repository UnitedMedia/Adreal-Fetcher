from google.cloud import secretmanager, bigquery
from common.gather_all import run_adreal_pipeline, get_correct_period
import pandas as pd
import traceback

PROJECT_ID = "ums-adreal-471711"
TABLE_ID = f"{PROJECT_ID}.AllianzTiriac.DataImport"


def access_secret(secret_id: str, version_id: str = "latest") -> str:
    """Fetch a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def push_to_bigquery(df: pd.DataFrame) -> str:
    """
    Load DataFrame into BigQuery, deleting/replacing only the months present in df.
    Ensures schema matches DataImport: Date, BrandOwner, Brand, ContentType, MediaOwner, MediaChannel, AdContacts
    """
    client = bigquery.Client()

    # Map incoming column names to BigQuery schema
    df = df.rename(columns={
        "Brand owner": "BrandOwner",
        "Brand": "Brand",
        "Content type": "ContentType",
        "Media owner": "MediaOwner",
        "Media channel": "MediaChannel",
        "Ad contacts": "AdContacts",
        "Date": "Date",
    })

    # Ensure all required columns exist
    required_cols = ["Date", "BrandOwner", "Brand", "ContentType", "MediaOwner", "MediaChannel", "AdContacts"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Keep only required columns in the expected order
    df = df[required_cols].copy()

    # ---- Type enforcement (critical) ----
    # Convert Date to python datetime.date (NOT string)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

    # Validate dates
    if df["Date"].isna().all():
        raise ValueError("All dates are missing/invalid; cannot load into BigQuery.")

    # Cast text fields (keep None as None where possible)
    for c in ["BrandOwner", "Brand", "ContentType", "MediaOwner", "MediaChannel"]:
        df[c] = df[c].astype("string")  # pandas nullable string type

    # Numeric
    df["AdContacts"] = pd.to_numeric(df["AdContacts"], errors="coerce").fillna(0).astype(int)

    # Preview logs
    print("Incoming columns after rename:", list(df.columns))
    print("Preview of data to load:")
    print(df.head(10))
    print("Date dtype:", df["Date"].dtype)
    first_date = df["Date"].dropna().iloc[0]
    print("Sample Date value/type:", first_date, type(first_date))

    # Determine distinct months present in the new data (as first day of month)
    months = pd.to_datetime(df["Date"]).to_period("M").dt.to_timestamp().dt.date.unique()
    print("Months detected for replacement:", months)

    # Delete old rows for those months
    for month in months:
        delete_query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE EXTRACT(YEAR FROM Date) = {month.year}
          AND EXTRACT(MONTH FROM Date) = {month.month}
        """
        print(f"Deleting old rows for month {month}...")
        client.query(delete_query).result()

    # Load new data
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    load_job.result()

    return f"Loaded {len(df)} rows into {TABLE_ID} (replaced months: {list(months)})"


def fetch_adreal_data(request):
    """Cloud Function entry point."""
    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        # AllianzTiriac competitors
        parent_brand_ids = ["16241", "18297", "13218", "72921", "40239", "51446"]

        # Fetch and process data
        df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
        print("DataFrame fetched. Shape:", df.shape)
        print("Columns:", list(df.columns))

        # Determine reporting period for logs (best-effort)
        period_raw = get_correct_period()
        period_date = pd.to_datetime(period_raw[-8:], format="%Y%m%d", errors="coerce")
        period_date_str = period_date.strftime("%Y-%m-01") if pd.notna(period_date) else "unknown"

        # Insert data into BigQuery
        result = push_to_bigquery(df)

        return f"Data fetched for period {period_date_str}: {result}"

    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        return f"Error: {str(e)}\n{traceback.format_exc()}"

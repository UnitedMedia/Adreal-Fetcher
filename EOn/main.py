from google.cloud import secretmanager, bigquery
from common.gather_all import run_adreal_pipeline, get_correct_period
import pandas as pd
import traceback

PROJECT_ID = "ums-adreal-471711"
TABLE_ID = f"{PROJECT_ID}.EOn.DataImport"

def access_secret(secret_id, version_id="latest"):
    """Fetch a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def push_to_bigquery(df):
    """Load DataFrame into BigQuery, replacing only the current month(s)."""
    client = bigquery.Client()

    # Rename columns to match BigQuery schema
    df = df.rename(columns={
        "Brand owner": "BrandOwner",
        "Brand": "Brand",
        "Product": "Product",
        "Content type": "ContentType",
        "Media channel": "MediaChannel",
        "Ad contacts": "AdContacts",
    })

    # Ensure required columns exist
    required_cols = ["Date", "BrandOwner", "Brand", "ContentType", "MediaChannel", "AdContacts"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Correct types
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["AdContacts"] = pd.to_numeric(df.get("AdContacts"), errors="coerce").fillna(0).astype(int)

    # Determine month(s) in the new data
    months = df["Date"].apply(lambda x: x.replace(day=1)).unique()

    # Delete old rows for these months (partition-aware if table is partitioned)
    for month in months:
        delete_query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE EXTRACT(YEAR FROM Date) = {month.year}
          AND EXTRACT(MONTH FROM Date) = {month.month}
        """
        client.query(delete_query).result()

    # Load new data
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    load_job.result()

    return f"Loaded {len(df)} rows into {TABLE_ID} (replacing months: {months})"


def fetch_adreal_data(request):
    """Cloud Function entry point."""
    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        # EOn competitors
        parent_brand_ids = ["95012", "93989", "64553", 
                            "94985", # E.ON solutii incalzire & racire
                            "94986", # E.ON Centrala termica
                            "94988", # E.ON Pompe caldura
                            "96090", # EON Drive
                            "94984", # E.ON Energie
                            "93994", # E.ON Green Energy
                            "93993", # E.ON Servicii Tehnice
                            "98380", # E.On Solar Home
                            "43287", # Electrica Furnizare
                            "95175", # Enel Aer conditionat
                            "95176", # Enel Centrale termice
                            "95174", # Enel servicii pentru casa
                            "95172", # Enel Energy
                            "94001", # Enel Green Energy
                            "94000", # Enel Servicii tehnice
                            "94996", # Energie
                            "93600", # Engie centrale termice
                            "95019", # Engie pompa caldura
                            "93984", # Engie CSR
                            "95015", # Engie Energie
                            "93983", # Engie Green Energy
                            "95198", # Engie Statii de incarcare
                            "95017", # Engie solutii de incarcare&racire
                            "95014", # Evryo
                            "95181", # Evryo Energy
                            "95180", # Evryo Maratonul Olteniei
                            "94999", "95005", "95036", # Green energy - toate 3 - care e cel bun?
                            "23942", # Hidroelectrica
                            "64451", # Maratonul olteniei dont use in 2024
                            "77108", # MyElectrica
                            "95003", # PPC aer conditionat
                            "95004", # PPC centrale termice
                            "95002", # PPC servicii pentru casa
                            "97977", # PPC Blue
                            "95169", # PPC energy
                            "94931", # PPC Green energy
                            "94932", # PPC Servicii tehnice
                            "94920", # Premier energy
                            "94922", # PRemier energy electric
                            ]
   
        # Fetch and process data
        df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
        print("DataFrame fetched. Shape:", df.shape)
        print("Columns:", df.columns)

        # Determine reporting period for logs
        period_date = pd.to_datetime(get_correct_period()[-8:], format="%Y%m%d").strftime("%Y-%m-01")

        # Insert data into BigQuery
        result = push_to_bigquery(df)

        return f"Data fetched for period {period_date}: {result}"

    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        return f"Error: {str(e)}\n{traceback.format_exc()}"

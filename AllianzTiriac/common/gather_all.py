from .brands_fetcher import BrandFetcher
from .websites_fetcher import PublisherFetcher
from .fetch_adreal import AdRealFetcher
import pandas as pd
from datetime import datetime, timedelta

def return_lookup(data):
    """Helper: build id -> full brand/publisher info dict."""
    return {dat["id"]: dat for dat in data}


def get_brand_owner(brand_id, brands_lookup):
    """
    Given a brand ID, find the top-level Brand owner.
    """
    brand_info = brands_lookup.get(brand_id)
    if not brand_info:
        return None  # brand not found

    parent_id = brand_info.get("parent_id")
    if not parent_id:
        return brand_info["name"]  # already a top-level owner

    parent_info = brands_lookup.get(parent_id)
    if not parent_info:
        return None  # parent not found

    return parent_info["name"]


def merge_data(stats_data, brands_data, websites_data):
    """Merge stats + brand + websites lookups, filling Brand owner properly."""
    brands_lookup = return_lookup(brands_data)
    websites_lookup = return_lookup(websites_data)

    all_rows = []
    for entry in stats_data:
        segment = entry.get("segment", {})
        stats_list = entry.get("stats", [])

        brand_id = segment.get("brand")
        brand_info = brands_lookup.get(brand_id, {})
        brand_name = brand_info.get("name", brand_id)
        brand_owner_name = get_brand_owner(brand_id, brands_lookup)

        product_name = brands_lookup.get(segment.get("product"), {}).get("name", segment.get("product"))
        website_name = websites_lookup.get(segment.get("website"), {}).get("name", segment.get("website"))

        # Use API-provided content_type if available
        content_type = segment.get("content_type")
        if not content_type or content_type == "None":
            content_type = decide_content_type(website_name)

        for stat in stats_list:
            row = {
                "period": stat.get("period"),
                "brand_owner_name": brand_owner_name,
                "brand_name": brand_name,
                "product": product_name,
                "website_name": website_name,
                "platform": segment.get("platform", None),
                "content_type": content_type,
            }
            # add values
            for k, v in stat.get("values", {}).items():
                row[k] = v
            # add uncertainty
            for k, v in stat.get("uncertainty", {}).items():
                row[f"{k}_uncertainty"] = v
            all_rows.append(row)

    return all_rows


def decide_content_type(website):
    """Fallback if API doesn't provide content_type."""
    if not isinstance(website, str) or not website:
        return "Unknown"
    lowered_website = website.lower()
    if "google." in lowered_website or "bing." in lowered_website:
        return "Search"
    if any(social in lowered_website for social in ["facebook", "instagram", "tiktok", "youtube"]):
        return "Social"
    return "Standard"

def get_previous_month_first_day():
    """Return the first day of the previous month as a string 'YYYY-MM-01'."""
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    previous_month_first_day = datetime(previous_month_last_day.year, previous_month_last_day.month, 1)
    return previous_month_first_day.strftime('%Y-%m-01')


def clean_data(df):
    """Clean merged DataFrame to match BigQuery schema."""
    # Rename columns to match BQ schema
    df = df.rename(columns={
        "brand_owner_name": "BrandOwner",
        "brand_name": "Brand",
        "website_name": "MediaChannel",
        "ad_cont": "AdContacts",
        "product": "Product",           # will drop it anyway
        "content_type": "ContentType",
        "Media owner": "MediaOwner",    # if you have this info
        "Brand owner": "BrandOwner"
    })

    # Drop the Product column (not in BQ)
    if "Product" in df.columns:
        df = df.drop("Product", axis=1)

    # Ensure all columns expected by BQ exist
    expected_columns = ["Date", "BrandOwner", "Brand", "ContentType", "MediaOwner", "MediaChannel", "AdContacts"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None  # fill missing columns with None

    # Remove summaries from MediaChannel
    df = df[df["MediaChannel"] != "Segment summary"]

    # Set Date to previous month first day
    df['Date'] = get_previous_month_first_day()

    # Force override of ContentType
    df["ContentType"] = df["MediaChannel"].apply(decide_content_type)
    
    # Reorder columns to match BigQuery
    df = df.reindex(columns=expected_columns)

    return df

def get_previous_month_range():
    """Return previous month in AdReal API range format (YYYYMM01,YYYYMMDD,month)."""
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    first_of_previous_month = datetime(previous_month_last_day.year, previous_month_last_day.month, 1)

    start_str = first_of_previous_month.strftime("%Y%m%d")
    end_str = previous_month_last_day.strftime("%Y%m%d")
    return f"{start_str},{end_str},month"

def get_correct_period():
    """Return the previous month in AdReal API period format."""
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    period = f"month_{previous_month_last_day.strftime('%Y%m01')}"
    return period


def run_adreal_pipeline(username, password, market="ro", parent_brand_ids=None):
    """Fetch, merge, clean AdReal data and return a DataFrame."""
    if parent_brand_ids is None:
        parent_brand_ids = []

    period = get_correct_period()

    # Fetch brands & websites
    brand_fetcher = BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=period)

    publisher_fetcher = PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=period)

    # Previus month range
    period_range = get_previous_month_range()

    # Fetch stats
    adreal_fetcher = AdRealFetcher(username=username, password=password, market=market, period_range=period_range)
    adreal_fetcher.login()
    stats_data = adreal_fetcher.fetch_data(
        parent_brand_ids,
        platforms="pc",
        page_types="search,social,standard",
        segments="brand,product,content_type,website",
        limit=1000000
    )

    merged_rows = merge_data(stats_data, brands_data, websites_data)
    df = pd.DataFrame(merged_rows).drop_duplicates()
    df = clean_data(df)
    return df

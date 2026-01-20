# 🧠 AdReal Fetcher Pipeline — Client Setup Guide

This document explains how to create, configure, and deploy a new AdReal data pipeline for a new client using Google Cloud Functions.

---

## 📁 Repository Structure

Each client (e.g. `Mega`, `Muller`, `ProCredit`) has its own folder under the main repository:

```
Adreal-Fetcher/
├── common/                 # Shared code for all clients (fetchers, utils, etc.)
├── DanoneDairy/
├── Digi/
├── Mega/
│   ├── common/             # Local copy of shared logic (specific to client)
│   ├── main.py             # Entry point for Cloud Function
│   ├── requirements.txt
├── Muller/
├── ProCredit/
└── ...
```

---

## 🧩 How to Add a New Client

### Step 1. Copy an Existing Client Folder

Pick a similar existing client (e.g. `Mega`) and duplicate it:

```bash
cp -r Mega NewClient
```

You’ll now have:

```
NewClient/
├── common/
├── main.py
├── requirements.txt
```

---

### Step 2. Edit `main.py`

Inside `main.py`, locate the line defining `parent_brand_ids` in `fetch_adreal_data`:

```python
parent_brand_ids = [
    "94444", "17127", "13367", "157", "51367", "11943"
]
```

Replace the IDs with those specific to the new client.

Inside `main.py`, locate the line 
```python
TABLE_ID = f"{PROJECT_ID}.Client.DataImport"
```
Replace 'Client' with the correct Client Name (Muller, Mega...)

⚠️Any change inside the specific code for each client requires redeployment.
---

### Step 3. Deploy the Cloud Function

From within the new client folder (`cd NewClient`), deploy the function:

```bash
gcloud functions deploy fetch_adreal_newclient   --region=europe-west1   --runtime=python310   --trigger-http   --allow-unauthenticated   --memory=512MB   --timeout=120s   --source=.   --entry-point=fetch_adreal_data   --gen2   --service-account=ums-adreal-471711@appspot.gserviceaccount.com
```

This creates a Cloud Function that:

- Fetches data from the AdReal API  
- Cleans and formats it  
- Pushes to BigQuery (`ums-adreal-471711`)  
- Uses secrets from Secret Manager for credentials  

---

### Step 4. Scheduling

Each client function is triggered automatically on the **3rd of every month** in the morning (configured via **Cloud Scheduler**).

**Example Cloud Scheduler setup:**  
- **Schedule:** `0 7 3 * *` → runs at 07:00 UTC on the 3rd day of each month  
- **Target:** HTTPS URL of the deployed Cloud Function  

---

### 🔐 Secrets & Permissions

**Stored in:** Google Secret Manager  
- `adreal-username`  
- `adreal-password`  

**Service Account:** `ums-adreal-471711@appspot.gserviceaccount.com`  
Has roles:  
- Secret Manager Accessor  
- BigQuery Admin  
- Cloud Functions Invoker  

---

## ⚙️ Manual Push Command

To manually push data for a specific month, run this command from the repository root:

```bash
python -m common.manual_push_to_bq 2025 8 --it works if you already set the parent ids in the script. it accepts just args YEAR and MONTH
python -m common.manual_push_to_bq 2025 10 --industries "312,345,314,319" - does not need parent ids, but industries (ids).
it accepts year, month + industries as in the example

For more info consult the api to get the induestries you want to query.
GET /api/ro/industries/?limit=1013
{
  "id": 359,
  "encrypted_id": "py4jqJPEmRGt1b6OfXFc5A==",
  "parent_id": 311,
  "name": "Automotive spare parts and services"
},
{
  "id": 360,
  "encrypted_id": "sVw8D5mlxGs_7VgQTCbBNA==",
  "parent_id": 359,
  "name": "Fuels, oils, lubricants"
},

```

> ⚠️ **CRITICAL WARNING:**  
> The manual push script (`manual_push_to_bq`) uses a Replace-by-Month ingestion strategy.  
> Any existing data in the BigQuery destination table for the specified month **WILL BE DELETED** and replaced with new data fetched from the AdReal API.  
> Use with caution.

---

## 🔐 Setting Up Secrets (AdReal Credentials)

The AdReal Fetcher pipeline uses **Google Secret Manager** to securely store credentials such as the AdReal username and password.  
Before deploying any new client function, ensure Secret Manager is enabled and the secrets are configured.

### Step 1. Enable Secret Manager API

Run this in **Cloud Shell** or your local terminal:

```bash
gcloud services enable secretmanager.googleapis.com
```
### Step 2. Enable Secret Manager API
Store your AdReal credentials as secrets in your Google Cloud project.

``` bash
# Create username secret
echo -n "USERNAME" | gcloud secrets create adreal-username \
  --replication-policy="automatic" \
  --data-file=-

# Create password secret
echo -n "PASSWORD" | gcloud secrets create adreal-password \
  --replication-policy="automatic" \
  --data-file=-
```

### Step 3. Grant Access to the Cloud Function
Your Cloud Function runs under a service account (for example:
ums-adreal-471711@appspot.gserviceaccount.com or a custom one you define).

Grant it permission to access the secrets:
```bash
gcloud secrets add-iam-policy-binding adreal-username \
  --member="serviceAccount:ums-adreal-471711@appspot.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding adreal-password \
  --member="serviceAccount:ums-adreal-471711@appspot.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```
Once this is done, the Cloud Function can securely retrieve credentials via the SecretManagerServiceClient() class in Python.

# Schedule the sql query (not very related)

After appending the data from AdReal to the tables (monthly), the sql scripts/dashboards that will consume the data needs to be triggered as per client request.

The client might request the sql script to run, for example, every 3 months starting from a date and it is not cron-like in BigQuery.
The full documentation is in https://docs.cloud.google.com/bigquery/docs/reference/datatransfer/rpc/google.cloud.bigquery.datatransfer.v1

The custom schedule will look like 
```bash
3 of feb,may,aug,nov 06:00
```
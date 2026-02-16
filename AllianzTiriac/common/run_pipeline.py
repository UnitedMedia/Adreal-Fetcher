from gather_all import run_adreal_pipeline, get_correct_period
from google.cloud import secretmanager

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "ums-adreal-471711"  # your actual project id
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def main():
    username = access_secret("adreal-username")
    password = access_secret("adreal-password")
    parent_brand_ids = [
            "16241",
            "18297",
            "13218",
            "72921",
            "40239",
            "51446"
        ]

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    print(f"Data fetched: {len(df)} rows")

    period = get_correct_period()
    df.to_csv(f"{period}_Adreal.csv", index=False)

if __name__ == "__main__":
    main()

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
    # Garanti Competitors
    parent_brand_ids = ["91125", "17497", "94361", "13257", "41762", "12167", "89996", "17335", "12621", "13102", "43549", "578",
                            "2308", "94085", "12164", "56939", "12065", "13364",
                            "97108", "13260", "56938", "13311", "15221", "23415", "16771", "1580", "26302", "16157", "94697", "38159", "12929", "93256"]
   
    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    print(f"Data fetched: {len(df)} rows")

    period = get_correct_period()
    df.to_csv(f"{period}_Adreal.csv", index=False)

if __name__ == "__main__":
    main()

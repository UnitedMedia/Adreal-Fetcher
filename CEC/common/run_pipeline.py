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
    
     # CEC competitors
    parent_brand_ids = ["43549", # ALD
                        "13260", # alpha bank
                        "13102", # BCR
                        "13364", # BRD
                        "13257", # BT
                        "13312", # CEC
                        "95746", # evrixa
                        "92288", #first bank
                        "12167", # garanti
                        "12621", # ing
                        "23415", # intesa
                        "16771", # libra
                        "2308", # otp bank
                        "12065", # raiffeisen
                        "94697", # salt bank
                        "12929" # unicredit
                        ]
    
    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    print(f"Data fetched: {len(df)} rows")

    period = get_correct_period()
    df.to_csv(f"{period}_Adreal.csv", index=False)

if __name__ == "__main__":
    main()

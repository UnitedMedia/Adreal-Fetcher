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
                        "94999", "95005", "95036" # Green energy - toate 3 - care e cel bun?
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

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    print(f"Data fetched: {len(df)} rows")

    period = get_correct_period()
    df.to_csv(f"{period}_Adreal.csv", index=False)

if __name__ == "__main__":
    main()

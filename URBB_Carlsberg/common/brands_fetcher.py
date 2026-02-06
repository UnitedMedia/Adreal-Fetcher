import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


class BrandFetcher:
    def __init__(self, username, password, market="ro", max_threads=5, limit=100000):
        self.BASE_URL = "https://adreal.gemius.com/api"
        self.LOGIN_URL = f"{self.BASE_URL}/login/?next=/api/"
        self.username = username
        self.password = password
        self.market = market
        self.limit = limit
        self.max_threads = max_threads
        self.session = requests.Session()
        self.all_brands = []

    # ---------------- LOGIN ----------------
    def login(self):
        print('\nStarted getting Brands data.')
        login_page = self.session.get(self.LOGIN_URL)
        csrftoken = self.session.cookies.get("csrftoken")
        payload = {
            "username": self.username,
            "password": self.password,
            "csrfmiddlewaretoken": csrftoken
        }
        headers = {"Referer": f"{self.BASE_URL}/{self.market}/stats/", "X-CSRFToken": csrftoken}
        resp = self.session.post(self.LOGIN_URL, data=payload, headers=headers)
        resp.raise_for_status()
        if "invalid" in resp.text.lower():
            raise Exception("Login failed")
        print("Login successful!")

    # ---------------- FETCH ----------------
    def fetch_brands(self, period):
        """Fetch all brands for a given period (handles pagination with threads)."""
        # Initial request
        resp = self.session.get(
            f"{self.BASE_URL}/{self.market}/brands/",
            params={"period": period, "limit": self.limit, "offset": 0},
        )
        resp.raise_for_status()
        data = resp.json()
        total_count = data.get("total_count", len(data.get("results", [])))
        print(f"Total brands to fetch for {period}: {total_count}")

        # Prepare offsets
        offsets = list(range(0, total_count, self.limit))

        def fetch_page(offset):
            params = {"period": period, "limit": self.limit, "offset": offset}
            r = self.session.get(
                f"{self.BASE_URL}/{self.market}/brands/", params=params, timeout=30
            )
            r.raise_for_status()
            results = r.json().get("results", [])
            print(f"Fetched {len(results)} brands at offset {offset}")
            return results

        # Fetch all concurrently
        results = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(fetch_page, o) for o in offsets]
            for future in as_completed(futures):
                results.extend(future.result())

        self.all_brands = results
        print(f"Done! Fetched {len(results)} brands for {period}")
        return results

    # ---------------- SAVE ----------------
    def save_json(self, filename="brands.json"):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.all_brands, f, indent=4, ensure_ascii=False)
        print(f"Saved JSON to {filename}")

    def save_csv(self, filename="brands.csv"):
        pd.DataFrame(self.all_brands).to_csv(filename, index=False)
        print(f"Saved CSV to {filename}")


# ---------------- MAIN ----------------
if __name__ == "__main__":
    fetcher = BrandFetcher(
        username = "",
        password = "",
        market="ro",
    )
    fetcher.login()
    fetcher.fetch_brands(period="month_20250801")
    fetcher.save_json("brands.json")
    #fetcher.save_csv("brands.csv")

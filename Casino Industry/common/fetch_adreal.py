import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
import time
from urllib.parse import urlencode

class AdRealFetcher:
    def __init__(self, username, password, market="ro",
                 period_range="20250801,20250831,month",
                 brand_ids="", limit=10000, max_threads=5, target_metric="ad_cont,ru"):
        self.BASE_URL = "https://adreal.gemius.com/api"
        self.LOGIN_URL = f"{self.BASE_URL}/login/?next=/api/"
        self.username = username
        self.password = password
        self.market = market
        self.period_range = period_range
        self.brand_ids = brand_ids
        self.limit = limit
        self.max_threads = max_threads
        self.target_metric = target_metric

        self.session = requests.Session()
        self.platform_id = None
        self.all_results = []

        # conservative default: product + content type (you can expand later)
        self.combined_segments = "brand_owner,brand,product,content_type,website,publisher,platform"
        # computed period label we will filter stats by (e.g. "month_20250801")
        self.period_label = self._period_label_from_range(period_range)

    def _period_label_from_range(self, periods_range):
        # periods_range expected "YYYYMMDD,YYYYMMDD,periodtype" (e.g. "20250801,20250831,month")
        parts = periods_range.split(",")
        if not parts:
            return None
        start = parts[0]
        period_type = parts[2] if len(parts) >= 3 else "day"
        return f"{period_type}_{start}"

    # ---------------- LOGIN ----------------
    def login(self):
        print('\nStarted getting Ad_conts metric.')
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

    # ---------------- FETCH OPTIONS ----------------
    def fetch_options(self, endpoint):
        resp = self.session.get(f"{self.BASE_URL}/{self.market}/{endpoint}/")
        resp.raise_for_status()
        return resp.json()

    def list_platforms(self):
        """Return raw platforms list (useful to inspect platform codes & ids)"""
        j = self.fetch_options("platforms")
        results = j.get("results", j)
        # print friendly table
        print("Available platforms (sample):")
        for p in results:
            # platform objects may contain id/code/name - print keys intelligently
            print({k: p.get(k) for k in ("id", "code", "label", "name") if k in p})
        return results

    def get_platform_id(self):
        platforms = self.fetch_options("platforms").get("results", [])
        if not platforms:
            raise RuntimeError("No platforms found")
        # save numeric id of first platform (what you had before)
        self.platform_id = platforms[0]["id"]
        print("Platform id (first):", self.platform_id)
        return platforms

    # ---------------- FETCH STATS (original multi-segment) ----------------
    def fetch_multi_segments(self):
        params_base = {
            "metrics": self.target_metric,
            "platforms": self.platform_id,
            "periods_range": self.period_range,
            "limit": self.limit,
            "brands": self.brand_ids,
            "segments": self.combined_segments
        }

        params = params_base.copy()
        params["offset"] = 0

        resp = self.session.get(f"{self.BASE_URL}/{self.market}/stats/", params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        total_count = data.get("total_count", len(results))
        print(f"Multi-segment request: total_count={total_count}")

        if total_count <= self.limit:
            self.all_results = results
            return

        offsets = range(self.limit, total_count, self.limit)

        def fetch_page(offset):
            p = params.copy()
            p["offset"] = offset
            r = self.session.get(f"{self.BASE_URL}/{self.market}/stats/", params=p, timeout=120)
            r.raise_for_status()
            return r.json().get("results", [])

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(fetch_page, o) for o in offsets]
            for future in as_completed(futures):
                results.extend(future.result())

        self.all_results = results

    # ---------------- FETCH STATS (support-style simple brand) ----------------
    def fetch_data(self, brand_ids, platforms="pc", page_types="search,social,standard",
                           metrics=None, segments="brand", limit=1000000):
        """
        Mimics the support code URL:
        /stats/?limit=1000000&brands=<ids>&format=json&metrics=ru,ad_cont,reach
                  &periods_range=<periods_range>&platforms=pc&page_types=search,social,standard&segments=brand
        """
        if isinstance(brand_ids, (list, tuple)):
            brands_param = ",".join(map(str, brand_ids))
        else:
            brands_param = str(brand_ids)

        if metrics is None:
            metrics = "ru,ad_cont,reach"

        params = {
            "limit": limit,
            "brands": brands_param,
            "format": "json",
            "metrics": metrics,
            "periods_range": self.period_range,
            "platforms": platforms,
            "page_types": page_types,
            "segments": segments
        }

        print("GET --->", f"{self.BASE_URL}/{self.market}/stats/?{urlencode(params)}")
        r = self.session.get(f"{self.BASE_URL}/{self.market}/stats/", params=params, timeout=120)
        r.raise_for_status()
        j = r.json()
        results = j.get("results", [])
        print(f"Support-style stats: total_count={j.get('total_count', len(results))}, returned={len(results)}")
        return results

    # ---------------- SAVE ----------------
    def save_json(self, filename, data=None):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data or self.all_results, f, indent=4, ensure_ascii=False)

    def flatten_to_excel(self, filename, results=None, filter_period=True):
        """
        Flatten results -> excel. If filter_period True, only keep stats rows
        whose 'period' equals the requested period_label (avoids duplicates).
        """
        results = results if results is not None else self.all_results
        all_rows = []
        for item in results:
            seg_info = item.get("segment", {})

            row = {}
            for seg_type, seg_values in seg_info.items():
                if isinstance(seg_values, dict):
                    for k, v in seg_values.items():
                        row[f"{seg_type}_{k}"] = v
                else:
                    row[seg_type] = seg_values

            for stat in item.get("stats", []):
                if filter_period and self.period_label:
                    if stat.get("period") != self.period_label:
                        # skip other stats entries (otherwise will see 3x duplicates)
                        continue
                row_copy = row.copy()
                row_copy["period"] = stat.get("period")
                for k, v in stat.get("values", {}).items():
                    row_copy[k] = v
                for k, v in stat.get("uncertainty", {}).items():
                    row_copy[f"{k}_uncertainty"] = v
                all_rows.append(row_copy)

        df = pd.DataFrame(all_rows)
        df.to_excel(filename, index=False)
        print(f"Saved {len(df)} rows to {filename}")
        return df

# ---------------- MAIN DEMO ----------------
if __name__ == "__main__":
    start_time = time.time()

    USERNAME = ""
    PASSWORD = ""

    fetcher = AdRealFetcher(username=USERNAME, password=PASSWORD, brand_ids="13549")

    # login using your session method
    fetcher.login()

    # list platforms so you can see what 'pc' corresponds to (id/code)
    platforms = fetcher.list_platforms()

    # try support-style query for Dream&co (95638)
    brand_to_test = 13549
    support_results = fetcher.fetch_data([brand_to_test], platforms="pc",
                                                 page_types="search,social,standard",
                                                 segments="brand,product,content_type,website", limit=1000000)
    #fetcher.save_json("support_results.json", support_results)
    #fetcher.flatten_to_excel("support_results.xlsx", results=support_results, filter_period=True)

    end_time = time.time()
    print(f"Done in {round((end_time - start_time)/60, 2)} minutes")

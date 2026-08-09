"""
Bronze layer ingestion. VesselAPI REST focuses on Port of Rotterdam vessel positions.

Design principles:
1. Preserve raw fidelity: thta is, store the full API response as received, no filtering.
2. Idempotency:that is each file is timestamped, so repeated runs never collide or overwrite.
3. Schema resilience: storeing as JSON, not a rigid table. Then Schema enforcement happens in Silver Layer.

"""
 
import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
 
load_dotenv()  # reads .env into environment variables
 
# CONFIG 
API_KEY = os.environ.get("VESSELAPI_KEY")
BRONZE_DIR = "../bronze/vessel_positions"   # using relative path to keep the project portable
POLL_INTERVAL_SECONDS = 300                  # pulling every 5 minutes interval
 
# Port of Rotterdam bounding box (approx) 
BBOX = {
    "filter.latBottom": 51.85,
    "filter.latTop": 52.05,
    "filter.lonLeft": 3.95,
    "filter.lonRight": 4.20,
}
 
BASE_URL = "https://api.vesselapi.com/v1/location/vessels/bounding-box"
MAX_RETRIES = 3
MAX_PAGES = 15  
PAGE_SIZE = 50    # max allowed per docs
 
 
def _build_time_window() -> dict:
    """Build a time.from/time.to window matching the poll interval.
 
    """
    now = datetime.now(timezone.utc)
    time_from = now.timestamp() - POLL_INTERVAL_SECONDS
    return {
        "time.from": datetime.fromtimestamp(time_from, tz=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "time.to": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }
 
 
def _get_page(params: dict) -> dict | None:
    """Fetch a single page from the bounding-box endpoint, with 429/error retry logic.
 
    Implements Retry-After-aware backoff per VesselAPI docs: on a 429, wait for the
    duration the server tells us to (falling back to exponential backoff starting
    at 1s if no Retry-After header is present), then retry, up to MAX_RETRIES times.
    """
    if not API_KEY:
        raise RuntimeError("Set VESSELAPI_KEY in your .env file before running.")
 
    headers = {"Authorization": f"Bearer {API_KEY}"}
    wait_seconds = 1
 
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
 
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", wait_seconds))
                print(f"[RATE LIMITED] attempt {attempt}/{MAX_RETRIES}, "
                      f"waiting {retry_after}s before retry")
                time.sleep(retry_after)
                wait_seconds *= 2  # exponential backoff fallback
                continue
 
            response.raise_for_status()  # raises an error for other 4xx/5xx responses
            return response.json()
 
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] API request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(wait_seconds)
            wait_seconds *= 2
 
    print("[ERROR] All retry attempts exhausted for this page.")
    return None
 
 
def fetch_vessels() -> dict | None:
    """Fetch ALL pages for the current bounding box and combine into one response.
 
    The bounding-box endpoint paginates (max 50 results per page per the docs).
    since a single poll can easily span multiple pages for a place like Rotterdam,
    following the nextToken until it's exhausted or MAX_PAGES is hit is ideal and combining
    every page's vessels into one list before returning.
 
    """
    all_vessels: list[dict] = []
    time_window = _build_time_window()
    params = dict(BBOX)
    params.update(time_window)
    params["pagination.limit"] = PAGE_SIZE
    page_count = 0
 
    while page_count < MAX_PAGES:
        page_count += 1
        page = _get_page(params)
 
        if page is None:
            # a page failed even after retries, stop rather than risk a gap
            print(f"[ERROR] Page {page_count} failed. Returning what we have so far.")
            break
 
        vessels = page.get("vessels", [])
        all_vessels.extend(vessels)
 
        next_token = page.get("nextToken")
        if not next_token:
            break  # no more pages
 
        # note: nextToken must be reused with the SAME time.from/time.to
        params = dict(BBOX)
        params.update(time_window)
        params["pagination.limit"] = PAGE_SIZE
        params["pagination.nextToken"] = next_token
 
    if page_count >= MAX_PAGES:
        print(f"[WARNING] Hit MAX_PAGES cap ({MAX_PAGES}), The results may be INCOMPLETE. "
              f"There could be more vessels beyond what was collected this cycle.")
 
    print(f"[PAGINATION] collected {len(all_vessels)} vessels across {page_count} page(s)")
    return {"vessels": all_vessels}
 
 
def write_batch(data: dict) -> None:
    """Write the raw API response to a Bronze JSON file."""
    if not data:
        return
 
    os.makedirs(BRONZE_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    filepath = os.path.join(BRONZE_DIR, f"{ts}.json")
 
    with open(filepath, "w") as f:
        json.dump(data, f)
 
    vessel_count = len(data.get("vessels", []))
    print(f"[BRONZE WRITE] {vessel_count} vessels -> {filepath}")
 
 
def run_ingestion_loop():
    print(f"Starting ingestion. Polling every {POLL_INTERVAL_SECONDS} seconds. Ctrl+C to stop.")
    while True:
        data = fetch_vessels()
        write_batch(data)
        time.sleep(POLL_INTERVAL_SECONDS)
 
 
if __name__ == "__main__":
    run_ingestion_loop()
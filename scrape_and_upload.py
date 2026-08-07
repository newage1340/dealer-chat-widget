"""Offloaded inventory scraper — runs in GitHub Actions, NOT on the Render web
service. It scrapes each dealer's inventory with Playwright/Chromium (installed
in the Actions runner) and POSTs the results to the web app's
/admin/inventory-upload endpoint, which writes them to the same SQLite DB.

This is what lets the web service drop `playwright` from requirements.txt so its
Render build drops from ~25 min to ~2 min.

Required env (set as GitHub Actions repo secrets):
  RENDER_APP_URL                e.g. https://dealer-chat-widget.onrender.com
  INVENTORY_UPLOAD_TOKEN        shared secret — MUST match the web app's env var
  SERVICE_ACCOUNT_JSON_CONTENT  raw Google service-account JSON (to read dealers)
"""
import os
import re
import sys

# Import app.py for its dealer-reading + scraper wiring, but keep it inert:
# no web server, no scheduler, no startup scrape, dummy creds for clients.
os.environ["DISABLE_SCHEDULER"] = "1"
os.environ["SKIP_STARTUP_SCRAPE"] = "1"
os.environ["DEV_CLEAR_DB"] = "0"
os.environ.setdefault("OPENAI_API_KEY", "sk-not-used-in-scraper")
os.environ.setdefault("TWILIO_ACCOUNT_SID", "ACnotused")
os.environ.setdefault("TWILIO_AUTH_TOKEN", "notused")
os.environ.setdefault("DB_PATH", "scrape_tmp.db")  # throwaway; we POST, not write local

import requests  # noqa: E402
import app as A  # noqa: E402

RENDER = os.environ.get("RENDER_APP_URL", "").rstrip("/")
TOKEN = os.environ.get("INVENTORY_UPLOAD_TOKEN", "")
# Optional: scrape ONLY this dealer's Twilio number (e.g. when onboarding a new
# dealer — run it alone instead of waiting through everyone else). Blank = all.
ONLY = A.normalize_phone(os.environ.get("ONLY_TWILIO", "").strip())


def main() -> int:
    if not RENDER or not TOKEN:
        print("ERROR: RENDER_APP_URL and INVENTORY_UPLOAD_TOKEN are required", file=sys.stderr)
        return 1
    try:
        dealers = A.read_dealers()
    except Exception as e:
        print(f"ERROR: could not read dealers: {e}", file=sys.stderr)
        return 1

    uploaded = 0
    for dealer in dealers:
        tn = A.normalize_phone(A.get_row_field(dealer, A.TWILIO_NUMBER_ALIASES))
        url = A.get_row_field(dealer, A.WEBSITE_URL_ALIASES)
        name = A.get_row_field(dealer, A.DEALER_NAME_ALIASES) or tn
        if not (tn and url):
            continue
        if ONLY and tn != ONLY:
            continue
        print(f"[{name}] scraping {url} ...", flush=True)
        try:
            vehicles = A.scrape_dealer_inventory(url, max_vehicles=0)
        except Exception as e:
            print(f"[{name}] scrape FAILED: {e}", file=sys.stderr)
            continue
        if not vehicles:
            # Never upload an empty result — the endpoint refuses it anyway, but
            # skip so we don't even try to wipe a live dealer's inventory.
            print(f"[{name}] 0 vehicles scraped — skipping upload", flush=True)
            continue
        # Flat dealer fee: some dealers (e.g. 465 Auto) DISPLAY base + a lot-wide
        # fee but store only the base price in their page data. If the sheet has a
        # 'Dealer Fee' set for this dealer, bake it into each priced vehicle so the
        # bot quotes the after-fee price the dealer actually advertises. Cars with
        # no price ("call for price") are left untouched.
        _fee = A._flat_dealer_fee(dealer)
        if _fee > 0:
            _bumped = 0
            for _v in vehicles:
                _p = re.sub(r"[^\d]", "", str(_v.get("Price", "") or ""))
                if _p and int(_p) > 0:
                    _v["Price"] = str(int(_p) + _fee)
                    _bumped += 1
            print(f"[{name}] applied ${_fee} flat dealer fee to {_bumped} priced vehicle(s)", flush=True)
        doc_fee = next((v.get("DocFee", "") for v in vehicles if v.get("DocFee")), "")
        tt_fee = next((v.get("TitleTagFee", "") for v in vehicles if v.get("TitleTagFee")), "")
        payload = {
            "twilio_number": tn,
            "vehicles": vehicles,
            "doc_fee": doc_fee,
            "title_tag_fee": tt_fee,
        }
        try:
            r = requests.post(f"{RENDER}/admin/inventory-upload", json=payload,
                              headers={"X-Upload-Token": TOKEN}, timeout=180)
            if r.status_code == 200:
                print(f"[{name}] uploaded {len(vehicles)} vehicles OK", flush=True)
                uploaded += 1
            else:
                print(f"[{name}] upload FAILED {r.status_code}: {r.text[:200]}", file=sys.stderr)
        except Exception as e:
            print(f"[{name}] upload error: {e}", file=sys.stderr)

    print(f"Done. {uploaded} dealer(s) uploaded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

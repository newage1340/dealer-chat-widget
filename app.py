# twilio-bot2/app.py
import os
import re
import json
import sqlite3
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import gspread
from flask import Flask, request, g, jsonify, render_template, session
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client as TwilioClient
from openai import OpenAI
from apscheduler.schedulers.background import BackgroundScheduler

from scraper import scrape_dealer_inventory

# =========================
# CONFIG
# =========================
SERVICE_ACCOUNT_JSON         = os.getenv("SERVICE_ACCOUNT_JSON", r"C:\twilio-bot\service_account.json")
DEALER_SHEET_ID              = "1zR8zbkpbqCyKNIDrbDLsLYenSMrOcOGRz5vRTrfjBvI"
OPENAI_MODEL                 = os.getenv("OPENAI_MODEL", "gpt-4o")
TWILIO_ACCOUNT_SID           = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN            = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_MESSAGING_SERVICE_SID = os.getenv("TWILIO_MESSAGING_SERVICE_SID", "")
DB_PATH                      = os.getenv("DB_PATH", r"C:\twilio-bot2\bot.db")
REMINDER_LEAD_MINUTES        = 60
COLD_FOLLOWUP_AFTER_MINUTES  = 30
COLD_FOLLOWUP_MAX_AGE_HOURS  = 72
MAX_MESSAGES_PER_CHAT        = 40
PURGE_MESSAGES_OLDER_THAN_DAYS = 30
# Dev mode: set DEV_CLEAR_DB=1 to wipe appointments/conversations on startup
DEV_CLEAR_DB      = os.getenv("DEV_CLEAR_DB", "0") == "1"
# Dev mode: set DEV_MAX_VEHICLES=5 to only load first N vehicles (0 = no limit)
DEV_MAX_VEHICLES  = int(os.getenv("DEV_MAX_VEHICLES", "0"))

PRIMER_TERMS_URL = os.getenv(
    "PRIMER_TERMS_URL",
    "https://docs.google.com/document/d/1Klia9h9ANWUaL-2P4yoPtUppqSFHv0-o7oM8B3jEwGU/view",
)
CAPABILITY_PRIMER = (
    "FYI - I can help with inventory, vehicles, financing, or scheduling a visit. "
    "By texting this number you agree to our Terms of Service. "
    "Replies are AI-assisted. Reply MENU for options, STOP to opt out. "
    f"Terms: {PRIMER_TERMS_URL}"
)
# Sent on a customer's FIRST message when that message triggers the menu/
# greeting path. The menu itself already explains what the bot does, so the
# capability primer would be redundant - just include the terms/consent piece.
TERMS_ONLY_PRIMER = (
    "By texting this number you agree to our Terms of Service. "
    "Replies are AI-assisted. Reply STOP to opt out anytime. "
    f"Terms: {PRIMER_TERMS_URL}"
)

# =========================
# APP + CLIENTS
# =========================
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
openai_client = OpenAI()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _build_gspread_client():
    """Build a gspread client from either:
      - SERVICE_ACCOUNT_JSON_CONTENT env var (raw JSON string, easiest on Render)
      - SERVICE_ACCOUNT_JSON env var (path to file on disk)
    Tries inline content first, falls back to file path."""
    inline = os.getenv("SERVICE_ACCOUNT_JSON_CONTENT", "").strip()
    if inline:
        info = json.loads(inline)
        return gspread.service_account_from_dict(info, scopes=SCOPES)
    return gspread.service_account(filename=SERVICE_ACCOUNT_JSON, scopes=SCOPES)


gs = _build_gspread_client()
# Fail fast if Google is slow - Twilio gives up waiting on the webhook at ~15s.
try:
    gs.session.timeout = 6  # seconds for connect+read
except Exception:
    pass

# =========================
# REGEX CONSTANTS
# =========================
YES_RE = re.compile(
    r"\b(yes|yep|yeah|yup|ok|okay|sure|confirm|confirmed|correct|that works|works|sounds good|definitely|absolutely|of course|will do)\b",
    re.I,
)
NO_RE = re.compile(
    r"\b(no|nah|nope|cancel|not|don't|do not|can't|cannot|won't|different|change|reschedule|never mind|nevermind|forget it)\b",
    re.I,
)
DISINTEREST_RE = re.compile(
    r"\b(not\s+interested|no\s+thanks|no\s+thank\s+you|don'?t\s+need|don'?t\s+want|"
    r"never\s+mind|nevermind|forget\s+it|stop|unsubscribe|remove\s+me|"
    r"not\s+looking|no\s+longer|changed\s+my\s+mind|found\s+one|already\s+bought|"
    r"bought\s+one|got\s+one|found\s+a\s+car|found\s+another)\b",
    re.I,
)
CANCEL_APPT_RE = re.compile(
    r"\b(cancel|cancelling|canceling|cancel\s+my\s+appointment|cancel\s+the\s+appointment|"
    r"won'?t\s+be\s+able\s+to\s+make\s+it|can'?t\s+make\s+it|can'?t\s+come|won'?t\s+be\s+coming|"
    r"not\s+going\s+to\s+make\s+it|not\s+coming|need\s+to\s+cancel|want\s+to\s+cancel|"
    r"something\s+came\s+up|no\s+longer\s+coming|don'?t\s+need\s+the\s+appointment)\b",
    re.I,
)
WEEKDAY_TO_INT = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}

# =========================
# GOOGLE SHEETS - HELPERS
# =========================

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _cell_to_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        try:
            return str(int(v))
        except Exception:
            return str(v)
    return str(v)


def _unique_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    result = []
    for idx, raw in enumerate(headers, start=1):
        base = (raw or "").strip() or f"column_{idx}"
        if base not in seen:
            seen[base] = 1
            result.append(base)
        else:
            seen[base] += 1
            result.append(f"{base}__{seen[base]}")
    return result


def _worksheet_to_records(ws: Any) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if not values:
        return []
    headers = _unique_headers(values[0])
    records = []
    for raw_row in values[1:]:
        row = list(raw_row) + [""] * (len(headers) - len(raw_row))
        row = row[:len(headers)]
        if not any((c or "").strip() for c in row):
            continue
        records.append(dict(zip(headers, row)))
    return records


_DEALERS_CACHE: Dict[str, Any] = {"data": None, "ts": 0.0}
_DEALERS_CACHE_TTL = 60.0  # seconds - fresh window before we re-fetch
_DEALERS_STALE_MAX = 600.0  # seconds - beyond this we won't serve stale even on error


def _refresh_gs_client() -> None:
    """Recreate the gspread client. Useful after a network failure leaves the
    underlying HTTP session in a bad state."""
    global gs
    try:
        gs = _build_gspread_client()
        try:
            gs.session.timeout = 6
        except Exception:
            pass
    except Exception as e:
        app.logger.warning("Failed to refresh gspread client: %s", e)


def read_dealers() -> List[Dict[str, Any]]:
    now = time.time()
    cached = _DEALERS_CACHE["data"]
    cache_age = now - _DEALERS_CACHE["ts"]

    # Serve fresh cache
    if cached is not None and cache_age < _DEALERS_CACHE_TTL:
        return cached

    # Need a fresh fetch - try once, then retry once with a refreshed client
    last_err: Optional[Exception] = None
    for attempt in range(2):
        try:
            sh = gs.open_by_key(DEALER_SHEET_ID)
            data = _worksheet_to_records(sh.sheet1)
            _DEALERS_CACHE["data"] = data
            _DEALERS_CACHE["ts"] = now
            return data
        except Exception as e:
            last_err = e
            app.logger.warning("read_dealers attempt %d failed: %s", attempt + 1, e)
            if attempt == 0:
                _refresh_gs_client()

    # Both attempts failed - fall back to stale cache if it's recent enough
    if cached is not None and cache_age < _DEALERS_STALE_MAX:
        app.logger.warning("Sheet read failed; serving stale cache (age=%.0fs)", cache_age)
        return cached

    raise last_err if last_err else RuntimeError("Sheet read failed and no cache available")


def get_row_field(row: Dict[str, Any], aliases: set) -> str:
    alias_norms = {_norm(a) for a in aliases}
    first_match = ""
    for k, v in row.items():
        if _norm(k) in alias_norms:
            txt = _cell_to_text(v).strip()
            if txt:
                return txt
            if not first_match:
                first_match = txt
    return first_match


def get_row_field_values(row: Dict[str, Any], aliases: set) -> List[str]:
    alias_norms = {_norm(a) for a in aliases}
    return [_cell_to_text(v).strip() for k, v in row.items()
            if _norm(k) in alias_norms and _cell_to_text(v).strip()]


def normalize_phone(n: str) -> str:
    n = (n or "").strip()
    if not n:
        return ""
    if n.startswith("+"):
        return "+" + re.sub(r"\D", "", n)
    digits = re.sub(r"\D", "", n)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return n


def select_dealer_for_twilio_number(dealers: List[Dict[str, Any]], twilio_to: str) -> Dict[str, Any]:
    tn = normalize_phone(twilio_to)
    for d in reversed(dealers):
        if normalize_phone(get_row_field(d, TWILIO_NUMBER_ALIASES)) == tn:
            return d
    return dealers[-1] if dealers else {}


# =========================
# FIELD ALIAS SETS
# (match Google Form column headers exactly)
# =========================

TWILIO_NUMBER_ALIASES = {
    "twilio number given to dealer (leave this blank)",
    "twilio number given to dealer leave this blank",
    "twilio number given to dealer",
    "twilio number", "assigned number", "twilio #",
}
DEALER_NAME_ALIASES = {
    "dealership name", "dealer name", "business name", "name",
}
DEALER_NOTIFY_PHONE_ALIASES = {
    "dealer phone number", "dealer phone", "dealership phone number",
    "dealership phone", "phone number", "phone",
}
DEALER_ADDRESS_ALIASES = {
    "dealer address", "dealership address", "address",
}
DEALER_HOURS_ALIASES = {
    "dealer hours", "dealership hours", "hours", "business hours",
    "hours of operation",
}
DEALER_FINANCING_ALIASES = {
    "do you offer financing?", "do you offer financing",
    "financing", "financing available",
}
DEALER_TRADEINS_ALIASES = {
    "do you accept trade-ins? (feel free to be as detailed as you like)",
    "do you accept trade-ins? feel free to be as detailed as you like",
    "do you accept trade-ins?", "do you accept trade-ins",
    "trade-ins", "trade ins", "trade-in policy",
}
DEALER_POLICIES_ALIASES = {
    'any dealership policies the ai should know? (ex: "no deposits" or "prices are firm")',
    "any dealership policies the ai should know? ex no deposits or prices are firm",
    "any dealership policies the ai should know",
    "dealership policies", "policies", "ai notes", "policy",
    # also doubles as additional services
    "additional services", "extra services", "services offered",
}
SALESMAN_PHONES_ALIASES = {
    "salesman phone numbers", "salesman phones", "salesman phone",
    "staff phone numbers", "staff phones", "notification phones",
}
WEBSITE_URL_ALIASES = {
    "website url", "website", "dealer website", "dealership website",
    "inventory website", "url", "site url",
}

# Inventory alias sets
VIN_ALIASES   = {"vin", "vin number", "vehicle id", "vehicle identification number"}
STOCK_ALIASES = {"stock", "stock number", "stock#", "stock #", "stock no", "stock id"}
TRIM_ALIASES  = {"trim", "package", "submodel", "trim level"}
ISSUE_NOTE_HEADER_ALIASES = {
    "issues", "issue", "problem", "problems", "needs", "need",
    "cons", "flaws", "faults", "known issues", "known problems",
    "damage", "damages", "notes", "note", "comments", "comment",
    "work needed", "repairs needed", "defects", "defect",
}
MAINT_WORK_HEADER_ALIASES = {
    "seats", "highlights", "features", "feature", "recent work",
    "maintenance", "service", "serviced", "service history",
    "repairs", "repair", "work done", "fixed", "new parts", "upgrades",
    "reconditioning",
}
TITLE_STATUS_ALIASES = {
    "title status", "title", "title type", "title condition",
    "lien", "title notes", "salvage", "clean title", "rebuilt title",
}


# =========================
# SALESMAN PHONES HELPER
# =========================

def get_salesman_phones(dealer_row: Dict[str, Any]) -> List[str]:
    raw = get_row_field(dealer_row, SALESMAN_PHONES_ALIASES)
    if not raw:
        return []
    parts = re.split(r"[,;\n]+", raw)
    return [normalize_phone(p.strip()) for p in parts if p.strip() and normalize_phone(p.strip())]


# =========================
# SQLITE - INIT
# =========================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _db()
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                twilio_number TEXT NOT NULL,
                year TEXT NOT NULL DEFAULT '',
                make TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '',
                trim TEXT NOT NULL DEFAULT '',
                color TEXT NOT NULL DEFAULT '',
                price TEXT NOT NULL DEFAULT '',
                mileage TEXT NOT NULL DEFAULT '',
                vin TEXT NOT NULL DEFAULT '',
                stock TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                carfax_url TEXT NOT NULL DEFAULT '',
                detail_url TEXT NOT NULL DEFAULT '',
                scraped_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_inventory_twilio ON inventory(twilio_number)")
        # Migration: add carfax_url / detail_url columns if they don't exist yet
        existing = {row[1] for row in conn.execute("PRAGMA table_info(inventory)")}
        if "carfax_url" not in existing:
            conn.execute("ALTER TABLE inventory ADD COLUMN carfax_url TEXT NOT NULL DEFAULT ''")
        if "detail_url" not in existing:
            conn.execute("ALTER TABLE inventory ADD COLUMN detail_url TEXT NOT NULL DEFAULT ''")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_names (
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                name TEXT NOT NULL,
                last_name TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (customer_phone, twilio_number)
            )
        """)
        for col_def in ("last_name TEXT NOT NULL DEFAULT ''",
                        "email TEXT NOT NULL DEFAULT ''",
                        "trade_in_vehicle TEXT NOT NULL DEFAULT ''"):
            try:
                conn.execute(f"ALTER TABLE customer_names ADD COLUMN {col_def}")
            except sqlite3.OperationalError:
                pass  # column already exists

        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_chat
            ON messages (customer_phone, twilio_number, id)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_appointments (
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                dealer_notify_phone TEXT NOT NULL DEFAULT '',
                visit_time TEXT NOT NULL,
                visit_time_iso TEXT NOT NULL DEFAULT '',
                car_desc TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (customer_phone, twilio_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                dealer_notify_phone TEXT NOT NULL DEFAULT '',
                visit_time TEXT NOT NULL,
                visit_time_iso TEXT NOT NULL DEFAULT '',
                car_desc TEXT NOT NULL,
                created_at TEXT NOT NULL,
                reminder_sent INTEGER NOT NULL DEFAULT 0,
                reconfirmed INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_reconfirmations (
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                dealer_notify_phone TEXT NOT NULL DEFAULT '',
                visit_time TEXT NOT NULL,
                car_desc TEXT NOT NULL,
                appointment_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (customer_phone, twilio_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_cancellations (
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                dealer_notify_phone TEXT NOT NULL DEFAULT '',
                visit_time TEXT NOT NULL,
                car_desc TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (customer_phone, twilio_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS cold_followups (
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                PRIMARY KEY (customer_phone, twilio_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS primer_sent (
                customer_phone TEXT NOT NULL,
                twilio_number TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                PRIMARY KEY (customer_phone, twilio_number)
            )
        """)

        if DEV_CLEAR_DB:
            app.logger.warning("DEV_CLEAR_DB=1 - wiping appointments, pending, messages, cold_followups, customer_names")
            conn.execute("DELETE FROM appointments")
            conn.execute("DELETE FROM pending_appointments")
            conn.execute("DELETE FROM pending_reconfirmations")
            conn.execute("DELETE FROM pending_cancellations")
            conn.execute("DELETE FROM cold_followups")
            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM customer_names")

    conn.close()


# =========================
# SQLITE - INVENTORY
# =========================

def get_inventory_for_twilio(twilio_number: str) -> List[Dict[str, Any]]:
    tn = normalize_phone(twilio_number)
    conn = _db()
    rows = conn.execute(
        "SELECT * FROM inventory WHERE twilio_number=? ORDER BY id", (tn,)
    ).fetchall()
    conn.close()
    return [{
        "Year": r["year"], "Make": r["make"], "Model": r["model"],
        "Trim": r["trim"], "Color": r["color"], "Price": r["price"],
        "Mileage": r["mileage"], "VIN": r["vin"], "Stock": r["stock"],
        "Description": r["description"], "CarfaxURL": r["carfax_url"],
        "DetailURL": r["detail_url"],
    } for r in rows]


def refresh_inventory_for_twilio(twilio_number: str, website_url: str, max_vehicles: int = 0) -> int:
    """Scrape this dealer's inventory and persist row-by-row as each vehicle
    is scraped. Old inventory stays available the whole time; stale rows
    (vehicles no longer found at the dealer) are pruned at the end based on
    the scrape session timestamp. Survives mid-scrape crashes - already-saved
    vehicles persist and the next attempt picks up from there."""
    tn = normalize_phone(twilio_number)
    if not tn or not website_url:
        return 0

    scrape_start_iso = _utc_now_iso()
    # Resume window: any vehicle for this dealer scraped in the last 15 min is
    # treated as "this session's already-done work" and skipped on retry. Long
    # enough to cover a crash + Render worker restart, short enough that the
    # scheduled 30-min refresh still re-scrapes everything fresh.
    resume_cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
    _recently_scraped_urls: set = set()
    try:
        conn = _db()
        rows = conn.execute(
            "SELECT detail_url FROM inventory WHERE twilio_number=? AND scraped_at >= ? AND detail_url <> ''",
            (tn, resume_cutoff),
        ).fetchall()
        conn.close()
        _recently_scraped_urls = {r[0] for r in rows}
        if _recently_scraped_urls:
            app.logger.info(
                "refresh_inventory_for_twilio %s: resuming - skipping %d already-scraped URLs",
                tn, len(_recently_scraped_urls),
            )
    except Exception as e:
        app.logger.warning("refresh resume lookup failed for %s: %s", tn, e)

    def _should_skip(detail_url: str) -> bool:
        return detail_url in _recently_scraped_urls

    def _save_one(v):
        """Called by scraper after each vehicle is scraped. Replaces the row
        for the same detail_url (so re-scrapes update prices/details cleanly)
        and stamps it with the current scrape's timestamp."""
        conn = _db()
        with conn:
            detail_url = v.get("DetailURL", "")
            if detail_url:
                conn.execute(
                    "DELETE FROM inventory WHERE twilio_number=? AND detail_url=?",
                    (tn, detail_url),
                )
            conn.execute("""
                INSERT INTO inventory
                (twilio_number, year, make, model, trim, color, price, mileage, vin, stock, description, carfax_url, detail_url, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tn,
                v.get("Year", ""), v.get("Make", ""), v.get("Model", ""),
                v.get("Trim", ""), v.get("Color", ""), v.get("Price", ""),
                v.get("Mileage", ""), v.get("VIN", ""), v.get("Stock", ""),
                v.get("Description", ""), v.get("CarfaxURL", ""),
                detail_url, _utc_now_iso(),
            ))
        conn.close()

    vehicles = scrape_dealer_inventory(
        website_url,
        max_vehicles=max_vehicles,
        on_vehicle_scraped=_save_one,
        should_skip=_should_skip,
    )

    # Prune stale rows: anything whose scraped_at is older than this scrape
    # started is a vehicle that wasn't seen this round (sold / removed).
    if vehicles:
        try:
            conn = _db()
            with conn:
                conn.execute(
                    "DELETE FROM inventory WHERE twilio_number=? AND scraped_at < ?",
                    (tn, scrape_start_iso),
                )
            conn.close()
        except Exception as e:
            app.logger.warning("Stale-row prune failed for %s: %s", tn, e)

    return len(vehicles)


def refresh_all_inventory(max_vehicles: int = 0) -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        dealers = read_dealers()
    except Exception as e:
        app.logger.error("refresh_all_inventory: sheet read failed: %s", e)
        return

    # Build list of dealers that have both a twilio number and website URL
    tasks = []
    for dealer in dealers:
        twilio_number = get_row_field(dealer, TWILIO_NUMBER_ALIASES)
        website_url   = get_row_field(dealer, WEBSITE_URL_ALIASES)
        dealer_name   = get_row_field(dealer, DEALER_NAME_ALIASES)
        if twilio_number and website_url:
            tasks.append((twilio_number, website_url, dealer_name))

    if not tasks:
        return

    def _scrape_one(twilio_number, website_url, dealer_name):
        count = refresh_inventory_for_twilio(twilio_number, website_url, max_vehicles=max_vehicles)
        return twilio_number, dealer_name, count

    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        futures = {
            executor.submit(_scrape_one, tn, url, name): name
            for tn, url, name in tasks
        }
        for future in as_completed(futures):
            try:
                twilio_number, dealer_name, count = future.result()
                app.logger.info(
                    "Inventory refreshed for %s (%s): %d vehicles",
                    dealer_name, twilio_number, count,
                )
            except Exception as e:
                app.logger.error("Inventory refresh failed for %s: %s", futures[future], e)


# =========================
# SQLITE - CUSTOMER NAMES
# =========================

def get_customer_profile(customer_phone: str, twilio_number: str) -> Dict[str, str]:
    conn = _db()
    row = conn.execute(
        "SELECT name, last_name, email, trade_in_vehicle FROM customer_names WHERE customer_phone=? AND twilio_number=?",
        (customer_phone, twilio_number),
    ).fetchone()
    conn.close()
    if not row:
        return {"name": "", "last_name": "", "email": "", "trade_in_vehicle": ""}
    return {
        "name": (row["name"] or "").strip(),
        "last_name": (row["last_name"] or "").strip(),
        "email": (row["email"] or "").strip(),
        "trade_in_vehicle": (row["trade_in_vehicle"] or "").strip(),
    }


def save_customer_profile(customer_phone: str, twilio_number: str, *,
                          name: Optional[str] = None,
                          last_name: Optional[str] = None,
                          email: Optional[str] = None,
                          trade_in_vehicle: Optional[str] = None) -> None:
    """Upsert customer profile. Only fields passed (non-None) are updated; existing values are preserved."""
    current = get_customer_profile(customer_phone, twilio_number)
    new_name = (name if name is not None else current["name"]).strip()
    new_last = (last_name if last_name is not None else current["last_name"]).strip()
    new_email = (email if email is not None else current["email"]).strip()
    new_trade = (trade_in_vehicle if trade_in_vehicle is not None else current["trade_in_vehicle"]).strip()
    conn = _db()
    conn.execute(
        "INSERT OR REPLACE INTO customer_names "
        "(customer_phone, twilio_number, name, last_name, email, trade_in_vehicle) VALUES (?, ?, ?, ?, ?, ?)",
        (customer_phone, twilio_number, new_name, new_last, new_email, new_trade),
    )
    conn.commit()
    conn.close()


def get_customer_name(customer_phone: str, twilio_number: str) -> str:
    return get_customer_profile(customer_phone, twilio_number)["name"]


def save_customer_name(customer_phone: str, twilio_number: str, name: str) -> None:
    save_customer_profile(customer_phone, twilio_number, name=name)


_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def is_valid_email(s: str) -> bool:
    return bool(_EMAIL_RE.match((s or "").strip()))


_NAME_STOPWORDS = {
    "yes", "yeah", "yep", "no", "nope", "ok", "okay", "sure", "thanks",
    "thank", "hi", "hey", "hello", "yo", "sup", "bye", "later", "cool",
    "nice", "good", "fine", "great", "today", "tomorrow", "now", "asap",
    "ready", "interested", "maybe", "idk", "lol", "yup",
}


def is_valid_name(s: str) -> bool:
    s = (s or "").strip()
    if len(s) < 2:
        return False
    if s.lower() in _NAME_STOPWORDS:
        return False
    # Must be mostly letters (allow apostrophe / hyphen / space for compound names).
    if not re.match(r"^[A-Za-z][A-Za-z'\- ]{1,40}$", s):
        return False
    return True


def missing_profile_field(profile: Dict[str, str]) -> Optional[str]:
    """Return a human-readable label for the next missing/invalid field, or None if profile is complete."""
    if not profile.get("name"):
        return "first name"
    if not profile.get("last_name"):
        return "last name"
    if not is_valid_email(profile.get("email", "")):
        return "email address"
    return None


# =========================
# SQLITE - MESSAGES
# =========================

def purge_old_data() -> None:
    cutoff = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=PURGE_MESSAGES_OLDER_THAN_DAYS)).isoformat(timespec="seconds")
    conn = _db()
    with conn:
        conn.execute("DELETE FROM messages WHERE created_at < ?", (cutoff,))
        conn.execute("DELETE FROM pending_appointments WHERE created_at < ?", (cutoff,))
        conn.execute("DELETE FROM pending_reconfirmations WHERE created_at < ?", (cutoff,))
    conn.close()


def save_message(customer_phone: str, twilio_number: str, role: str, content: str) -> None:
    conn = _db()
    with conn:
        conn.execute(
            "INSERT INTO messages (customer_phone, twilio_number, role, content, created_at) VALUES (?, ?, ?, ?, ?)",
            (customer_phone, twilio_number, role, content, _utc_now_iso()),
        )
        conn.execute("""
            DELETE FROM messages
            WHERE id NOT IN (
                SELECT id FROM messages WHERE customer_phone=? AND twilio_number=?
                ORDER BY id DESC LIMIT ?
            ) AND customer_phone=? AND twilio_number=?
        """, (customer_phone, twilio_number, MAX_MESSAGES_PER_CHAT, customer_phone, twilio_number))
    conn.close()
    purge_old_data()


def get_recent_messages(customer_phone: str, twilio_number: str, limit: int = 14) -> List[Dict[str, Any]]:
    conn = _db()
    rows = conn.execute("""
        SELECT role, content FROM messages
        WHERE customer_phone=? AND twilio_number=?
        ORDER BY id DESC LIMIT ?
    """, (customer_phone, twilio_number, limit)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def get_last_customer_message(customer_phone: str, twilio_number: str) -> str:
    conn = _db()
    row = conn.execute("""
        SELECT content FROM messages
        WHERE customer_phone=? AND twilio_number=? AND role='user'
        ORDER BY id DESC LIMIT 1
    """, (customer_phone, twilio_number)).fetchone()
    conn.close()
    return row["content"] if row else ""


def has_primer_been_sent(customer_phone: str, twilio_number: str) -> bool:
    conn = _db()
    row = conn.execute(
        "SELECT 1 FROM primer_sent WHERE customer_phone=? AND twilio_number=?",
        (customer_phone, twilio_number),
    ).fetchone()
    conn.close()
    return row is not None


def mark_primer_sent(customer_phone: str, twilio_number: str) -> None:
    conn = _db()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO primer_sent (customer_phone, twilio_number, sent_at) VALUES (?, ?, ?)",
            (customer_phone, twilio_number, _utc_now_iso()),
        )
    conn.close()


# =========================
# SQLITE - APPOINTMENTS
# =========================

def set_pending(customer_phone, twilio_number, dealer_notify_phone, visit_time, visit_time_iso, car_desc):
    conn = _db()
    with conn:
        conn.execute("""
            INSERT OR REPLACE INTO pending_appointments
            (customer_phone, twilio_number, dealer_notify_phone, visit_time, visit_time_iso, car_desc, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (customer_phone, twilio_number, dealer_notify_phone, visit_time, visit_time_iso, car_desc, _utc_now_iso()))
    conn.close()


def get_pending(customer_phone, twilio_number):
    conn = _db()
    row = conn.execute("SELECT * FROM pending_appointments WHERE customer_phone=? AND twilio_number=?",
                       (customer_phone, twilio_number)).fetchone()
    conn.close()
    return dict(row) if row else None


def clear_pending(customer_phone, twilio_number):
    conn = _db()
    with conn:
        conn.execute("DELETE FROM pending_appointments WHERE customer_phone=? AND twilio_number=?",
                     (customer_phone, twilio_number))
    conn.close()


def log_appointment(customer_phone, twilio_number, dealer_notify_phone, visit_time, visit_time_iso, car_desc) -> Tuple[int, bool]:
    conn = _db()
    existing = conn.execute("""
        SELECT id FROM appointments WHERE customer_phone=? AND twilio_number=? ORDER BY id DESC LIMIT 1
    """, (customer_phone, twilio_number)).fetchone()
    if existing:
        row_id = int(existing["id"])
        conn.execute("""
            UPDATE appointments SET dealer_notify_phone=?, visit_time=?, visit_time_iso=?,
            car_desc=?, created_at=?, reminder_sent=0, reconfirmed=0 WHERE id=?
        """, (dealer_notify_phone, visit_time, visit_time_iso, car_desc, _utc_now_iso(), row_id))
        is_reschedule = True
    else:
        cur = conn.execute("""
            INSERT INTO appointments
            (customer_phone, twilio_number, dealer_notify_phone, visit_time, visit_time_iso, car_desc, created_at, reminder_sent, reconfirmed)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
        """, (customer_phone, twilio_number, dealer_notify_phone, visit_time, visit_time_iso, car_desc, _utc_now_iso()))
        row_id = int(cur.lastrowid)
        is_reschedule = False
    conn.commit()
    conn.close()
    return row_id, is_reschedule


def get_latest_appointment(customer_phone, twilio_number):
    conn = _db()
    row = conn.execute("""
        SELECT id, visit_time, visit_time_iso, car_desc, dealer_notify_phone FROM appointments
        WHERE customer_phone=? AND twilio_number=? ORDER BY id DESC LIMIT 1
    """, (customer_phone, twilio_number)).fetchone()
    conn.close()
    return dict(row) if row else None


def cancel_appointment(customer_phone, twilio_number):
    conn = _db()
    with conn:
        conn.execute("""
            DELETE FROM appointments WHERE id = (
                SELECT id FROM appointments WHERE customer_phone=? AND twilio_number=? ORDER BY id DESC LIMIT 1
            )
        """, (customer_phone, twilio_number))
    conn.close()


def set_pending_reconfirmation(customer_phone, twilio_number, dealer_notify_phone, visit_time, car_desc, appointment_id):
    conn = _db()
    with conn:
        conn.execute("""
            INSERT OR REPLACE INTO pending_reconfirmations
            (customer_phone, twilio_number, dealer_notify_phone, visit_time, car_desc, appointment_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (customer_phone, twilio_number, dealer_notify_phone, visit_time, car_desc, appointment_id, _utc_now_iso()))
    conn.close()


def get_pending_reconfirmation(customer_phone, twilio_number):
    conn = _db()
    row = conn.execute("SELECT * FROM pending_reconfirmations WHERE customer_phone=? AND twilio_number=?",
                       (customer_phone, twilio_number)).fetchone()
    conn.close()
    return dict(row) if row else None


def clear_pending_reconfirmation(customer_phone, twilio_number):
    conn = _db()
    with conn:
        conn.execute("DELETE FROM pending_reconfirmations WHERE customer_phone=? AND twilio_number=?",
                     (customer_phone, twilio_number))
    conn.close()


def mark_reminder_sent(appointment_id):
    conn = _db()
    with conn:
        conn.execute("UPDATE appointments SET reminder_sent=1 WHERE id=?", (appointment_id,))
    conn.close()


def mark_reconfirmed(appointment_id):
    conn = _db()
    with conn:
        conn.execute("UPDATE appointments SET reconfirmed=1 WHERE id=?", (appointment_id,))
    conn.close()


def set_pending_cancellation(customer_phone, twilio_number, dealer_notify_phone, visit_time, car_desc):
    conn = _db()
    with conn:
        conn.execute("""
            INSERT OR REPLACE INTO pending_cancellations
            (customer_phone, twilio_number, dealer_notify_phone, visit_time, car_desc, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (customer_phone, twilio_number, dealer_notify_phone, visit_time, car_desc, _utc_now_iso()))
    conn.close()


def get_pending_cancellation(customer_phone, twilio_number):
    conn = _db()
    row = conn.execute("SELECT * FROM pending_cancellations WHERE customer_phone=? AND twilio_number=?",
                       (customer_phone, twilio_number)).fetchone()
    conn.close()
    return dict(row) if row else None


def clear_pending_cancellation(customer_phone, twilio_number):
    conn = _db()
    with conn:
        conn.execute("DELETE FROM pending_cancellations WHERE customer_phone=? AND twilio_number=?",
                     (customer_phone, twilio_number))
    conn.close()


def get_cold_conversations() -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    min_age = (now - timedelta(minutes=COLD_FOLLOWUP_AFTER_MINUTES)).isoformat(timespec="seconds")
    max_age = (now - timedelta(hours=COLD_FOLLOWUP_MAX_AGE_HOURS)).isoformat(timespec="seconds")
    conn = _db()
    rows = conn.execute("""
        SELECT m.customer_phone, m.twilio_number, m.created_at
        FROM messages m
        WHERE m.role = 'assistant'
          AND m.created_at <= ?
          AND m.created_at >= ?
          AND m.id = (SELECT MAX(id) FROM messages m2
                      WHERE m2.customer_phone=m.customer_phone AND m2.twilio_number=m.twilio_number)
          AND NOT EXISTS (SELECT 1 FROM cold_followups cf
                          WHERE cf.customer_phone=m.customer_phone AND cf.twilio_number=m.twilio_number)
    """, (min_age, max_age)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_cold_followup_sent(customer_phone, twilio_number):
    conn = _db()
    with conn:
        conn.execute("INSERT OR REPLACE INTO cold_followups (customer_phone, twilio_number, sent_at) VALUES (?, ?, ?)",
                     (customer_phone, twilio_number, _utc_now_iso()))
    conn.close()


def clear_cold_followup(customer_phone, twilio_number):
    conn = _db()
    with conn:
        conn.execute("DELETE FROM cold_followups WHERE customer_phone=? AND twilio_number=?",
                     (customer_phone, twilio_number))
    conn.close()


def get_upcoming_unreminded_appointments() -> List[Dict[str, Any]]:
    now = datetime.now()
    window_end = now + timedelta(minutes=REMINDER_LEAD_MINUTES + 5)
    conn = _db()
    rows = conn.execute("""
        SELECT id, customer_phone, twilio_number, dealer_notify_phone, visit_time, visit_time_iso, car_desc
        FROM appointments WHERE reminder_sent=0 AND visit_time_iso != ''
    """).fetchall()
    conn.close()
    due = []
    for row in rows:
        visit_dt = _parse_visit_time_iso_to_local_naive(str(row["visit_time_iso"] or "").strip())
        if visit_dt and now <= visit_dt <= window_end:
            due.append(dict(row))
    return due


# =========================
# TIME HELPERS
# =========================

def _parse_visit_time_iso_to_local_naive(iso_str: str) -> Optional[datetime]:
    if not iso_str:
        return None
    candidate = iso_str.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def _validate_iso(iso_str: str) -> str:
    dt = _parse_visit_time_iso_to_local_naive(str(iso_str or "").strip())
    return dt.isoformat(timespec="seconds") if dt else ""


_HAS_CLOCK_TIME_RE = re.compile(
    r"\b(1[0-2]|0?[1-9])(?::([0-5][0-9]))?\s*(am|pm)\b|\b([01]?\d|2[0-3]):[0-5]\d\b|\b(noon|midnight)\b",
    re.IGNORECASE,
)


def has_clock_time(s: str) -> bool:
    """True if the string contains a specific clock time (am/pm, 24h HH:MM, or noon/midnight)."""
    return bool(_HAS_CLOCK_TIME_RE.search(s or ""))


def parse_visit_time_from_text(text: str, now: Optional[datetime] = None) -> Tuple[str, str]:
    raw = (text or "").strip()
    if not raw:
        return "", ""
    now = now or datetime.now()
    lowered = raw.lower()
    tm = re.search(r"\b(1[0-2]|0?[1-9])(?::([0-5][0-9]))?\s*(am|pm)\b", lowered)
    if not tm:
        return "", ""
    hour = int(tm.group(1))
    minute = int(tm.group(2) or "0")
    ampm = tm.group(3)
    if ampm == "pm" and hour != 12:
        hour += 12
    if ampm == "am" and hour == 12:
        hour = 0
    target_date = now.date()
    day_token = ""
    if "tomorrow" in lowered:
        day_token, target_date = "tomorrow", (now + timedelta(days=1)).date()
    elif "tonight" in lowered or "today" in lowered:
        day_token = "tonight" if "tonight" in lowered else "today"
    else:
        for wd, wd_idx in WEEKDAY_TO_INT.items():
            if re.search(rf"\b{wd}\b", lowered):
                day_token = wd
                days_ahead = (wd_idx - now.weekday()) % 7 or 7
                target_date = (now + timedelta(days=days_ahead)).date()
                break
    dt = datetime.combine(target_date, datetime.min.time()).replace(hour=hour, minute=minute, second=0)
    if not day_token and dt < now:
        dt += timedelta(days=1)
        day_token = "tomorrow"
    time_display = tm.group(0).replace(" ", "")
    return (f"{time_display} {day_token}" if day_token else time_display), dt.isoformat(timespec="seconds")


_WORD_NUM = {"a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6}

_RELATIVE_DIRECTION_LATER_RE = re.compile(
    r"\b(later|after|push(?:ed)?\s*back|move(?:d)?\s*back|delay(?:ed)?|postpone[d]?|behind)\b",
    re.IGNORECASE,
)
_RELATIVE_DIRECTION_EARLIER_RE = re.compile(
    r"\b(earlier|sooner|before|move(?:d)?\s*up|push(?:ed)?\s*up|ahead\s*of\s*schedule)\b",
    re.IGNORECASE,
)
_RELATIVE_AMOUNT_RE = re.compile(
    r"\b(\d+|an?|one|two|three|four|five|six)\s+(hours?|hrs?|minutes?|mins?)\b",
    re.IGNORECASE,
)
_RELATIVE_HALF_HOUR_RE = re.compile(r"\bhalf\s+(?:an?\s+)?hour\b", re.IGNORECASE)
RESCHEDULE_INTENT_RE = re.compile(
    r"\b(reschedule|reschedul|move|push|change|switch|shift|bump|delay|postpone|"
    r"later|earlier|sooner|push\s*back|move\s*up)\b",
    re.IGNORECASE,
)


def parse_relative_offset(text: str) -> Optional[timedelta]:
    """Parse phrases like 'an hour later' or '30 min earlier' into a signed timedelta.
    Returns None if no relative offset is found."""
    if not text:
        return None
    t = text.lower()

    if _RELATIVE_HALF_HOUR_RE.search(t):
        amount_min = 30
    else:
        m = _RELATIVE_AMOUNT_RE.search(t)
        if not m:
            return None
        n_raw, unit = m.group(1).lower(), m.group(2).lower()
        if n_raw in _WORD_NUM:
            num = _WORD_NUM[n_raw]
        else:
            try:
                num = int(n_raw)
            except ValueError:
                return None
        amount_min = num * 60 if unit.startswith("hour") or unit.startswith("hr") else num

    is_earlier = bool(_RELATIVE_DIRECTION_EARLIER_RE.search(t))
    is_later = bool(_RELATIVE_DIRECTION_LATER_RE.search(t))
    if is_earlier and not is_later:
        return timedelta(minutes=-amount_min)
    return timedelta(minutes=amount_min)


def format_visit_time_display(dt: datetime, now: Optional[datetime] = None) -> str:
    """Format a datetime like '3pm' or '3:30pm tomorrow' for display in confirmations."""
    now = now or datetime.now()
    hour_12 = int(dt.strftime("%I"))
    ampm = dt.strftime("%p").lower()
    base = f"{hour_12}{ampm}" if dt.minute == 0 else f"{hour_12}:{dt.minute:02d}{ampm}"
    today = now.date()
    if dt.date() == today:
        return base
    if dt.date() == today + timedelta(days=1):
        return f"{base} tomorrow"
    return f"{base} {dt.strftime('%a').lower()}"


# =========================
# INVENTORY MATCHING + DISPLAY
# =========================

# Aliases: what a customer might say -> what's stored in the DB make field
_MAKE_ALIASES: dict = {
    "chevy":       "chevrolet",
    "chev":        "chevrolet",
    "vw":          "volkswagen",
    "merc":        "mercedes-benz",
    "mercedes":    "mercedes-benz",
    "range rover": "land rover",
    "rover":       "land rover",
    "land rover":  "land rover",
}

# Known car brands used to detect when a customer asks about a make we don't carry
_KNOWN_BRANDS: set = {
    # Active brands
    "acura", "alfa", "audi", "bentley", "bmw", "bugatti", "buick",
    "cadillac", "chevrolet", "chevy", "chrysler", "dodge", "ferrari",
    "fiat", "ford", "genesis", "gmc", "honda", "hyundai", "infiniti",
    "jaguar", "jeep", "kia", "lamborghini", "lexus", "lincoln", "lotus",
    "maserati", "mazda", "mclaren", "mercedes", "mercedes-benz", "merc",
    "mini", "mitsubishi", "nissan", "porsche", "ram", "rivian", "subaru",
    "tesla", "toyota", "volkswagen", "vw", "volvo", "land rover",
    # Discontinued / less common
    "pontiac", "saturn", "oldsmobile", "hummer", "scion", "mercury",
    "plymouth", "saab", "isuzu", "daewoo", "suzuki", "panoz", "fisker",
    "polestar", "lucid", "scout", "studebaker", "packard", "delorean",
    "geo", "eagle", "datsun", "renault", "peugeot", "citroen",
}

# Common model names - lets us catch "do you have any Silverados?" style queries
_KNOWN_MODELS: set = {
    # Chevrolet
    "silverado", "tahoe", "suburban", "equinox", "traverse", "malibu",
    "impala", "camaro", "corvette", "blazer", "colorado", "trax", "trailblazer",
    # Ford
    "f150", "mustang", "bronco", "expedition", "fusion", "fiesta", "maverick",
    # Toyota
    "camry", "corolla", "highlander", "rav4", "tacoma", "tundra",
    "sienna", "sequoia", "prius", "avalon", "venza",
    # Honda
    "accord", "civic", "odyssey", "ridgeline",
    # Nissan
    "altima", "sentra", "maxima", "pathfinder", "armada", "murano", "versa",
    # Jeep
    "wrangler", "cherokee", "renegade", "gladiator",
    # GMC
    "yukon", "acadia",
    # Dodge
    "charger", "challenger", "durango",
    # Hyundai
    "elantra", "sonata", "tucson", "palisade", "ioniq", "veloster",
    # Kia
    "optima", "sorento", "sportage", "telluride", "stinger",
    # Subaru
    "outback", "forester", "crosstrek", "impreza", "ascent", "wrx",
    # Others
    "escalade", "4runner", "taurus", "lacrosse", "enclave", "envision",
    "grand cherokee", "grand prix", "grand am",
    "range rover",
}


def _asked_brand_not_in_inventory(msg: str, rows: List[Dict[str, Any]]) -> bool:
    """Return True if the message names a known brand or model that we don't carry at all."""
    body_lower = re.sub(r"[^a-z0-9 ]", " ", msg.lower())
    body_words = set(body_lower.split())
    our_makes  = {str(r.get("Make",  "")).strip().lower() for r in rows if r.get("Make")}
    our_models = {str(r.get("Model", "")).strip().lower() for r in rows if r.get("Model")}

    # Expand our_makes with aliases so "chevy" matches "chevrolet" inventory, etc.
    our_makes_expanded = set(our_makes)
    for alias, canonical in _MAKE_ALIASES.items():
        if canonical in our_makes:
            our_makes_expanded.add(alias)

    # Check known brands (exact word match; also handle hyphenated makes like "mercedes-benz")
    for brand in _KNOWN_BRANDS:
        # Multi-word brands (e.g. "land rover", "mercedes-benz") match as substring; single words match as whole word
        if " " in brand or "-" in brand:
            brand_present = brand in body_lower
        else:
            brand_present = brand in body_words
        if brand_present:
            brand_in_inv = any(
                om == brand or om.startswith(brand + "-") or om.startswith(brand + " ")
                or brand.startswith(om) or om.startswith(brand.split("-")[0] + "-")
                for om in our_makes_expanded
            )
            if not brand_in_inv:
                return True

    # Check known models - exact or simple plural (e.g. "silverados" -> "silverado" + "s")
    # Avoid startswith to prevent false positives ("titan" -> "titanium", "accord" -> "according")
    # Use prefix matching on our_models so "accord" matches "accord hybrid", etc.
    for model in _KNOWN_MODELS:
        def _model_in_inventory(m, our_models=our_models):
            return any(om == m or om.startswith(m + " ") for om in our_models)
        if " " in model:
            # Multi-word model: check as substring of full body
            if model in body_lower and not _model_in_inventory(model):
                return True
        else:
            if any(w == model or w == model + "s" for w in body_words) and not _model_in_inventory(model):
                return True

    return False


def _vehicle_title(r: Dict[str, Any]) -> str:
    year  = str(r.get("Year",  "")).strip()
    make  = str(r.get("Make",  "")).strip()
    model = str(r.get("Model", "")).strip()
    trim  = get_row_field(r, TRIM_ALIASES).strip()
    return " ".join(p for p in [year, make, model, trim] if p) or "that vehicle"


def format_inventory_rows(rows: List[Dict[str, Any]], limit: int = 80) -> str:
    lines = []
    for r in rows[:limit]:
        year    = str(r.get("Year",    "")).strip()
        make    = str(r.get("Make",    "")).strip()
        model   = str(r.get("Model",   "")).strip()
        color   = str(r.get("Color",   "")).strip()
        price   = str(r.get("Price",   "")).strip()
        mileage = str(r.get("Mileage", "")).strip()
        if not (year or make or model):
            continue
        car = f"{year} {make} {model}".strip()
        extras = [x for x in [color, f"{mileage} mi" if mileage else "", f"${price}" if price else ""] if x]
        if extras:
            car += " (" + ", ".join(extras) + ")"
        lines.append(car)
    if not lines:
        return "(No inventory listed yet.)"
    if len(rows) > limit:
        lines.append(f"...and {len(rows) - limit} more.")
    return "\n".join(lines)


def _row_text_for_match(r: Dict[str, Any]) -> str:
    searchable = (
        {"year", "make", "model", "color", "price", "mileage"}
        | ISSUE_NOTE_HEADER_ALIASES | MAINT_WORK_HEADER_ALIASES
        | VIN_ALIASES | STOCK_ALIASES | TRIM_ALIASES | TITLE_STATUS_ALIASES
    )
    return " ".join(f"{k}: {v}" for k, v in r.items() if _norm(k) in searchable).lower()


def _sim(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()


def _keyword_score(current_msg: str, row: Dict[str, Any]) -> float:
    # Normalize hyphens away so the customer's "f250" / "F-250" and the row's
    # stored "F-250" tokenize to the same form. Without this, F-250 vs F-350
    # disambiguation collapses (both miss the keyword bonus, fuzzy similarity
    # decides) and the wrong truck can be picked. Same for RX-350, MX-5, etc.
    cm = current_msg.lower().replace("-", "")
    q_words = set(re.sub(r"[^a-z0-9 ]", " ", cm).split())
    bonus = 0.0
    for field, weight, min_len in [
        ("Make", 0.30, 4), ("Model", 0.30, 2), ("Year", 0.12, 4),
        ("Color", 0.08, 4), ("Trim", 0.25, 2),
    ]:
        val = str(row.get(field, "")).strip().lower().replace("-", "")
        if not val:
            continue
        tokens = [t for t in re.sub(r"[^a-z0-9 ]", " ", val).split() if len(t) >= min_len]
        if tokens and any(t in q_words for t in tokens):
            bonus += weight
    model = str(row.get("Model", "")).strip().lower().replace("-", "")
    trim  = str(row.get("Trim",  "")).strip().lower().replace("-", "")
    if model and trim:
        combo = re.sub(r"[^a-z0-9 ]", " ", f"{model} {trim}").strip()
        if combo and combo in re.sub(r"[^a-z0-9 ]", " ", cm):
            bonus += 0.40
    return bonus


def find_inventory_matches(rows, query, top_k=3, current_msg=""):
    q  = re.sub(r"\s+", " ", (query or "").strip().lower())
    cm = re.sub(r"\s+", " ", (current_msg or q).strip().lower())
    if not q or not rows:
        return []
    scored = []
    for r in rows:
        hay   = _row_text_for_match(r)
        # Weight the current message heavily so it overrides history noise
        score = _sim(q, hay) * 0.5 + _sim(cm, hay) * 0.7 + _keyword_score(cm, r)
        vin   = get_row_field(r, VIN_ALIASES).lower()
        if vin and vin in q:
            score += 0.35
        elif vin and len(vin) >= 6 and vin[-6:] in q:
            score += 0.20
        stock = get_row_field(r, STOCK_ALIASES).lower()
        if stock and stock in q:
            score += 0.25
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for s, r in scored[:top_k] if s >= 0.25]


def _body_mentions_car(body: str, rows: List[Dict[str, Any]]) -> bool:
    """Return True if the message contains a year, make, or model from inventory."""
    b = body.lower()
    if re.search(r"\b(19|20)\d{2}\b", b):
        return True
    # Build canonical makes from inventory
    inv_makes = {str(r.get("Make", "")).strip().lower() for r in rows if r.get("Make")}
    # Check aliases first (e.g. "chevy" -> "chevrolet", "range rover" -> "land rover")
    for alias, canonical in _MAKE_ALIASES.items():
        if canonical in inv_makes and alias in b:
            return True
    b_words = set(re.sub(r"[^a-z0-9 ]", " ", b).split())
    for r in rows:
        make  = str(r.get("Make",  "")).strip().lower()
        model = str(r.get("Model", "")).strip().lower()
        if make  and len(make)  >= 3 and make  in b: return True
        if model and len(model) >= 3 and model in b: return True
        # First 2 words of model (e.g. "range rover" from "Range Rover Velar P250 R-Dynamic...")
        model_words = model.split()
        if len(model_words) >= 2 and " ".join(model_words[:2]) in b: return True
        # Distinctive single-token model nameplates (e.g. "xc90", "330i", "tahoe", "f150").
        # The scraper stores Model as "<nameplate> <trim word>" (e.g. "XC90 T6"), so the
        # full string never matches when the customer types just the nameplate.
        # 2-char tokens are accepted only when they mix letters and digits ("X7", "Q5",
        # "M3") so common English words ("to", "is", "of") never trigger a false match.
        for tok in re.sub(r"[^a-z0-9]", " ", model).split():
            if tok in b_words and (
                len(tok) >= 3
                or (len(tok) == 2 and any(c.isdigit() for c in tok) and any(c.isalpha() for c in tok))
            ):
                return True
        # Hyphenated make components (e.g. "mercedes" from "Mercedes-Benz")
        if "-" in make:
            for part in make.split("-"):
                if len(part) >= 4 and re.search(r"\b" + re.escape(part) + r"\b", b):
                    return True
    return False


def find_row_by_car_desc(rows, car_desc):
    if not car_desc or not rows:
        return None
    car_desc_lower = car_desc.lower()
    best_row, best_score = None, 0.0
    for r in rows:
        row_text = _row_text_for_match(r)
        score = _sim(car_desc_lower, row_text)
        for word in set(re.sub(r"[^a-z0-9 ]", " ", car_desc_lower).split()):
            if len(word) >= 4 and word in row_text:
                score += 0.10
        if score > best_score:
            best_score, best_row = score, r
    return best_row if best_score > 0.1 else None


_PRICE_TOKEN = r"\$?\s*([\d]{1,3}(?:,?\d{3})*|\d+)\s*(k|K)?"

def _parse_price_token(num_str: str, k_marker: Optional[str]) -> int:
    n = int(num_str.replace(",", ""))
    # "20k" -> 20000; "20" alone is ambiguous but we treat sub-1000 as thousands too
    # to match how customers actually text ("under 20" means $20k).
    if k_marker or n < 1000:
        n *= 1000
    return n


def _extract_price_range(body: str) -> tuple:
    """Pull (min_price, max_price) out of a customer message. Either may be None.
    Handles 'between $X and $Y', 'between X-Y' (compact hyphen range, with or
    without spaces), 'X to Y', plain 'X-Yk', under/over/min/max forms."""
    b = body.lower()
    # 1) "between X (and|to|-) Y" - hyphen allowed without surrounding whitespace
    bet_m = re.search(rf"\bbetween\s+{_PRICE_TOKEN}\s*(?:and|to|-)\s*{_PRICE_TOKEN}", b)
    if bet_m:
        lo = _parse_price_token(bet_m.group(1), bet_m.group(2))
        hi = _parse_price_token(bet_m.group(3), bet_m.group(4))
        return (min(lo, hi), max(lo, hi))
    # 2) Bare "X-Yk" / "X to Y" range without "between" prefix (e.g. "10-15k", "10k to 15k")
    bare_m = re.search(rf"(?<!\w){_PRICE_TOKEN}\s*(?:-|to)\s*{_PRICE_TOKEN}(?!\w)", b)
    if bare_m:
        lo_raw, lo_k = bare_m.group(1), bare_m.group(2)
        hi_raw, hi_k = bare_m.group(3), bare_m.group(4)
        # If neither side has a 'k' marker, propagate from whichever side has one.
        if not lo_k and hi_k:
            lo_k = hi_k
        if not hi_k and lo_k:
            hi_k = lo_k
        lo = _parse_price_token(lo_raw, lo_k)
        hi = _parse_price_token(hi_raw, hi_k)
        # Sanity: only treat as a range if both look like reasonable car prices
        if lo > 0 and hi > 0 and lo != hi:
            return (min(lo, hi), max(lo, hi))
    max_p = None
    min_p = None
    under_m = re.search(rf"\b(?:under|less than|below|cheaper than|max(?:imum)?|up to|no more than|<=?)\s+{_PRICE_TOKEN}", b)
    if under_m:
        max_p = _parse_price_token(under_m.group(1), under_m.group(2))
    over_m = re.search(rf"\b(?:over|more than|above|at least|min(?:imum)?|>=?)\s+{_PRICE_TOKEN}", b)
    if over_m:
        min_p = _parse_price_token(over_m.group(1), over_m.group(2))
    return (min_p, max_p)


def _row_price_int(r: Dict[str, Any]) -> int:
    """Extract a row's price as int, or 0 if missing/unparseable."""
    raw = re.sub(r"[^\d]", "", str(r.get("Price", "")))
    try:
        return int(raw) if raw else 0
    except ValueError:
        return 0


def _format_price_listing(rows: List[Dict[str, Any]], min_p: Optional[int], max_p: Optional[int]) -> str:
    """Deterministic, complete listing of inventory rows that match a price filter."""
    matching = []
    for r in rows:
        p = _row_price_int(r)
        if p <= 0:
            continue
        if max_p is not None and p > max_p:
            continue
        if min_p is not None and p < min_p:
            continue
        matching.append((p, r))
    matching.sort(key=lambda t: t[0])
    if min_p is not None and max_p is not None:
        header = f"Here are our vehicles between ${min_p:,} and ${max_p:,}:"
        empty  = f"We don't currently have any vehicles between ${min_p:,} and ${max_p:,}."
    elif max_p is not None:
        header = f"Here are our vehicles under ${max_p:,}:"
        empty  = f"We don't currently have any vehicles under ${max_p:,}."
    elif min_p is not None:
        header = f"Here are our vehicles over ${min_p:,}:"
        empty  = f"We don't currently have any vehicles over ${min_p:,}."
    else:
        return ""  # no filter - caller shouldn't have routed here
    if not matching:
        return empty + " Would you like to widen the price range?"
    LIST_LIMIT = 5
    lines = [header]
    for p, r in matching[:LIST_LIMIT]:
        year  = str(r.get("Year",  "")).strip()
        make  = str(r.get("Make",  "")).strip()
        model = str(r.get("Model", "")).strip()
        title = " ".join(s for s in [year, make, model] if s)
        lines.append(f"- {title}: ${p:,}")
    lines.append("")
    if len(matching) > LIST_LIMIT:
        lines.append(f"...and {len(matching) - LIST_LIMIT} more. Tell me a make, year, or anything else and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


# ── Body/fuel/drivetrain feature filtering ──────────────────────────────────
# Customers ask "diesel trucks", "any AWD SUVs", "Ford trucks". Without these
# filters the LLM was either hallucinating or dropping cars from the list.

_BODY_TYPE_QUERY = {
    "truck": r"\b(trucks?|pickups?(?:\s+trucks?)?)\b",
    "suv":   r"\b(suvs?|crossovers?)\b",
    "sedan": r"\b(sedans?)\b",
    "van":   r"\b(vans?|minivans?)\b",
    "coupe": r"\b(coupes?)\b",
    "hatchback":   r"\b(hatchbacks?)\b",
    "wagon":       r"\b(wagons?)\b",
    "convertible": r"\b(convertibles?|drop[- ]?tops?)\b",
}

_FUEL_TYPE_QUERY = {
    "diesel":   r"\b(diesel)\b",
    "hybrid":   r"\b(hybrids?|hybrid\s+vehicles?)\b",
    "electric": r"\b(electric|all[- ]electric|battery\s+electric|ev|evs)\b",
}

_DRIVETRAIN_QUERY = {
    "awd": r"\b(awd|all[- ]wheel\s+drive|all[- ]wheel)\b",
    "4wd": r"\b(4wd|4x4|four[- ]wheel\s+drive|four[- ]wheel)\b",
    "fwd": r"\b(fwd|front[- ]wheel\s+drive|front[- ]wheel)\b",
    "rwd": r"\b(rwd|rear[- ]wheel\s+drive|rear[- ]wheel)\b",
}


def _extract_body_type(body: str) -> Optional[str]:
    b = (body or "").lower()
    for key, pat in _BODY_TYPE_QUERY.items():
        if re.search(pat, b):
            return key
    return None


def _extract_fuel_type(body: str) -> Optional[str]:
    b = (body or "").lower()
    # Avoid matching "ev" inside "ever", "every", etc - already handled by \b
    for key, pat in _FUEL_TYPE_QUERY.items():
        if re.search(pat, b):
            return key
    return None


def _extract_drivetrain(body: str) -> Optional[str]:
    b = (body or "").lower()
    for key, pat in _DRIVETRAIN_QUERY.items():
        if re.search(pat, b):
            return key
    return None


def _row_haystack(r: Dict[str, Any]) -> str:
    """Combined searchable text from model, trim, and description for feature matching."""
    parts = [
        str(r.get("Model", "")),
        str(r.get("Trim", "")),
        str(r.get("Description", "")),
    ]
    return " ".join(p.strip() for p in parts if p).lower()


def _row_matches_body_type(r: Dict[str, Any], body_type: str) -> bool:
    if not body_type:
        return True
    h = _row_haystack(r)
    aliases = {
        "truck": ["truck", "pickup"],
        "suv":   ["suv", "crossover"],
        "sedan": ["sedan"],
        "van":   ["van", "minivan"],
        "coupe": ["coupe"],
        "hatchback":   ["hatchback"],
        "wagon":       ["wagon"],
        "convertible": ["convertible", "drop-top", "drop top"],
    }.get(body_type, [body_type])
    return any(a in h for a in aliases)


def _row_matches_fuel_type(r: Dict[str, Any], fuel_type: str) -> bool:
    if not fuel_type:
        return True
    h = _row_haystack(r)
    if fuel_type == "diesel":
        return "diesel" in h
    if fuel_type == "hybrid":
        return "hybrid" in h
    if fuel_type == "electric":
        # Match "electric" but NOT inside "electric power steering" if also "gas/diesel/hybrid"
        # Simplest: require the word "electric" and absence of typical ICE markers.
        if "battery electric" in h or " ev " in f" {h} ":
            return True
        return ("electric" in h and not any(k in h for k in ["gasoline", "diesel", "hybrid", "v6", "v8", "ecoboost", "turbo i4", "i-4"]))
    return True


def _row_matches_drivetrain(r: Dict[str, Any], drivetrain: str) -> bool:
    if not drivetrain:
        return True
    # Trusted signals:
    #   1. Title (model + trim) contains an explicit drivetrain marker.
    #   2. Description has a strict "Drive: X" spec-sheet label - NOT free
    #      prose mentioning "xDrive available" or "Quattro variant".
    #   3. Models that are universally one drivetrain in every trim ever made
    #      (e.g. Wrangler / Gladiator are 4WD only - no 2WD versions exist).
    title = " ".join([
        str(r.get("Model", "")).strip().lower(),
        str(r.get("Trim", "")).strip().lower(),
    ])
    desc  = str(r.get("description", "") or "").lower()
    desc_drive_match = re.search(
        r"\bdrive\b\s*[:\-]?\s*(awd|4wd|4x4|fwd|rwd|all[- ]wheel\s+drive|four[- ]wheel\s+drive|front[- ]wheel\s+drive|rear[- ]wheel\s+drive)\b",
        desc,
    )
    desc_drive = desc_drive_match.group(1) if desc_drive_match else ""
    model_lc = str(r.get("Model", "")).strip().lower()
    # Models where 4WD is the standard/dominant configuration. Wrangler &
    # Gladiator have no 2WD versions at all. Heavy-duty pickups (F-250/350/450,
    # Silverado/Sierra 2500/3500, Ram 2500/3500) are sold predominantly in 4WD,
    # especially in used BHPH inventory.
    is_universal_4wd = (
        any(m in model_lc for m in ("wrangler", "gladiator"))
        or bool(re.search(r"\bf-?[234]50\b", model_lc))
        or bool(re.search(r"\b(silverado|sierra|ram)\s+[23]500\b", model_lc))
    )

    awd_markers = ["awd", "all-wheel drive", "all wheel drive", "quattro", "4matic", "4-matic", "xdrive", "x-drive"]
    fwd_markers = ["fwd", "front-wheel drive", "front wheel drive"]
    rwd_markers = ["rwd", "rear-wheel drive", "rear wheel drive"]
    fourwd_markers = ["4wd", "4x4", "four-wheel drive", "four wheel drive"]

    def _desc_says(*needles):
        return any(n in desc_drive for n in needles)

    if drivetrain == "awd":
        if any(m in title for m in awd_markers + fourwd_markers): return True
        if _desc_says("awd", "4wd", "4x4", "all", "four"):        return True
        if is_universal_4wd:                                       return True
        return False
    if drivetrain == "4wd":
        if any(m in title for m in fourwd_markers + awd_markers): return True
        if _desc_says("4wd", "4x4", "four", "awd", "all"):        return True
        if is_universal_4wd:                                       return True
        return False
    if drivetrain == "fwd":
        if any(m in title for m in fwd_markers): return True
        if _desc_says("fwd", "front"):           return True
        return False
    if drivetrain == "rwd":
        if any(m in title for m in rwd_markers): return True
        if _desc_says("rwd", "rear"):            return True
        return False
    return True


def _row_matches_features(r: Dict[str, Any],
                          body_type: Optional[str] = None,
                          fuel_type: Optional[str] = None,
                          drivetrain: Optional[str] = None) -> bool:
    return (_row_matches_body_type(r, body_type)
            and _row_matches_fuel_type(r, fuel_type)
            and _row_matches_drivetrain(r, drivetrain))


_BODY_TYPE_LABEL = {
    "truck": "trucks", "suv": "SUVs", "sedan": "sedans", "van": "vans",
    "coupe": "coupes", "hatchback": "hatchbacks", "wagon": "wagons",
    "convertible": "convertibles",
}
_FUEL_TYPE_LABEL = {
    "diesel": "diesel", "hybrid": "hybrid", "electric": "electric",
}
_DRIVETRAIN_LABEL = {
    "awd": "AWD", "4wd": "4WD", "fwd": "FWD", "rwd": "RWD",
}


def _inherit_filters_from_prior(body: str, history: List[Dict[str, Any]]) -> Dict[str, Any]:
    """When a follow-up message has only a make ('what about hondas') but the
    immediately prior user turn had a price/year/feature filter ('any toyotas
    or hondas under 15k'), carry those filters forward. Returns a dict with
    keys min_p / max_p / year / body / fuel / drive - only filled when found
    on the prior message; missing keys mean 'no inherited value'."""
    prior_user = None
    for msg in reversed(history):
        if msg.get("role") != "user":
            continue
        content = (msg.get("content") or "").strip()
        if not content or content == body:
            continue
        if _is_more_question(content):
            continue
        prior_user = content
        break
    if not prior_user:
        return {}
    inherited: Dict[str, Any] = {}
    p_min, p_max = _extract_price_range(prior_user)
    if p_min is not None:
        inherited["min_p"] = p_min
    if p_max is not None:
        inherited["max_p"] = p_max
    yr = re.search(r"\b(19|20)\d{2}\b", prior_user)
    if yr:
        inherited["year"] = yr.group(0)
    bt = _extract_body_type(prior_user)
    if bt:
        inherited["body"] = bt
    ft = _extract_fuel_type(prior_user)
    if ft:
        inherited["fuel"] = ft
    dt = _extract_drivetrain(prior_user)
    if dt:
        inherited["drive"] = dt
    return inherited


def _extract_make_filters(body: str, rows: List[Dict[str, Any]]) -> List[str]:
    """Return ALL canonical inventory make names the customer is asking us to LIST,
    in order of appearance in the message. Returns an empty list if no make-listing
    intent is found, or if the message names a specific model (year + model token)
    indicating a single-car query that should fall through to the specific-car path.

    Supports compound queries like 'any toyotas or hondas' -> ['toyota', 'honda']."""
    b = body.lower()
    listing_intent = bool(re.search(
        r"\b(any|other|more|what|which|list|show|all|got|"
        r"do you have|are there|is there|carry|stock|"
        r"got any|have you got)\b",
        b,
    ))
    if not listing_intent:
        return []

    inv_makes = {str(r.get("Make", "")).strip().lower() for r in rows if r.get("Make")}

    def _names_specific_model(target_make: str) -> bool:
        for r in rows:
            rmake = str(r.get("Make", "")).strip().lower()
            if rmake != target_make:
                continue
            model = str(r.get("Model", "")).strip().lower()
            if not model:
                continue
            for tok in re.sub(r"[^a-z0-9]", " ", model).split():
                if len(tok) < 3:
                    continue
                if re.search(rf"\b{re.escape(tok)}\b", b):
                    return True
        return False

    # Collect every make hit in the body, with its position so we can sort by
    # appearance order. Don't return early on the first match - compound queries
    # ("toyotas or hondas") need to surface every make.
    hits: List[tuple] = []  # (position, canonical_make)
    seen: set = set()

    for alias, canonical in _MAKE_ALIASES.items():
        if canonical not in inv_makes or canonical in seen:
            continue
        m = re.search(rf"\b{re.escape(alias)}s?\b", b)
        if m:
            if _names_specific_model(canonical):
                return []
            hits.append((m.start(), canonical))
            seen.add(canonical)

    for make in inv_makes:
        if make in seen:
            continue
        if " " in make or "-" in make:
            idx = b.find(make)
            if idx >= 0:
                if _names_specific_model(make):
                    return []
                hits.append((idx, make))
                seen.add(make)
        else:
            m = re.search(rf"\b{re.escape(make)}s?\b", b)
            if m:
                if _names_specific_model(make):
                    return []
                hits.append((m.start(), make))
                seen.add(make)

    hits.sort(key=lambda t: t[0])
    return [c for _, c in hits]


def _extract_make_filter(body: str, rows: List[Dict[str, Any]]) -> Optional[str]:
    """Return the first canonical inventory make found in the body (single-make
    convenience wrapper around _extract_make_filters). Used by callers that only
    want one make at a time."""
    filters = _extract_make_filters(body, rows)
    return filters[0] if filters else None


def _format_make_listing(rows: List[Dict[str, Any]], make_name,
                         min_p: Optional[int] = None, max_p: Optional[int] = None,
                         year: Optional[str] = None,
                         body_type: Optional[str] = None,
                         fuel_type: Optional[str] = None,
                         drivetrain: Optional[str] = None) -> str:
    """Deterministic, complete listing of all inventory rows of the requested make(s),
    optionally narrowed by price/year/body/fuel/drivetrain filters. ``make_name`` may
    be a single canonical make string or a list of canonical makes (compound query)."""
    if isinstance(make_name, str):
        targets = [make_name.strip().lower()]
    else:
        targets = [m.strip().lower() for m in make_name if m]

    def _row_make_in_targets(rmake: str) -> bool:
        for t in targets:
            if rmake == t or rmake.startswith(t + "-") or rmake.startswith(t + " "):
                return True
        return False

    matching = []
    for r in rows:
        rmake = str(r.get("Make", "")).strip().lower()
        if not _row_make_in_targets(rmake):
            continue
        if year and str(r.get("Year", "")).strip() != year:
            continue
        p = _row_price_int(r)
        if max_p is not None and p > max_p:
            continue
        if min_p is not None and p < min_p:
            continue
        if not _row_matches_features(r, body_type, fuel_type, drivetrain):
            continue
        matching.append((p, r))

    # Pretty display name for each make - prefer the actual cased value from the row.
    def _pretty_for(target: str) -> str:
        for _, r in matching:
            if str(r.get("Make", "")).strip().lower() == target:
                return str(r.get("Make", "")).strip()
        for r in rows:
            if str(r.get("Make", "")).strip().lower() == target:
                return str(r.get("Make", "")).strip()
        return target.title()

    pretty_makes = [_pretty_for(t) for t in targets]
    if len(pretty_makes) == 1:
        pretty = pretty_makes[0]
    elif len(pretty_makes) == 2:
        pretty = f"{pretty_makes[0]} and {pretty_makes[1]}"
    else:
        pretty = ", ".join(pretty_makes[:-1]) + ", and " + pretty_makes[-1]

    # Build descriptive label like "2017 AWD diesel Ford trucks" - only includes
    # parts the customer actually asked for.
    label_parts = []
    if year:
        label_parts.append(year)
    if drivetrain:
        label_parts.append(_DRIVETRAIN_LABEL.get(drivetrain, drivetrain.upper()))
    if fuel_type:
        label_parts.append(_FUEL_TYPE_LABEL.get(fuel_type, fuel_type))
    label_parts.append(pretty)
    if body_type:
        label_parts.append(_BODY_TYPE_LABEL.get(body_type, body_type + "s"))
        label_noun = ""  # body_type already implies "vehicles"
    else:
        label_noun = " vehicles"
    label = " ".join(label_parts)

    if min_p is not None and max_p is not None:
        price_qual = f" between ${min_p:,} and ${max_p:,}"
    elif max_p is not None:
        price_qual = f" under ${max_p:,}"
    elif min_p is not None:
        price_qual = f" over ${min_p:,}"
    else:
        price_qual = ""

    if not matching:
        any_make_in_inv = any(_row_make_in_targets(str(r.get("Make", "")).strip().lower()) for r in rows)
        if not any_make_in_inv:
            return (f"We don't currently have any {pretty} vehicles in our inventory. "
                    f"Would you like to hear about something similar?")
        # Drivetrain data is incomplete in dealer inventory feeds. If the only
        # filter blocking all matches is drivetrain, acknowledge the data gap
        # rather than claim "zero" - which would mislead the customer.
        if drivetrain:
            no_dt_count = sum(
                1 for r in rows
                if _row_make_in_targets(str(r.get("Make", "")).strip().lower())
                and (not year or str(r.get("Year", "")).strip() == year)
                and (max_p is None or _row_price_int(r) <= max_p)
                and (min_p is None or _row_price_int(r) >= min_p)
                and _row_matches_features(r, body_type, fuel_type, None)
            )
            if no_dt_count:
                return (f"Our listings don't always specify drivetrain, so I can't confirm "
                        f"which {pretty} {body_type+'s' if body_type else 'vehicle(s)'} are {drivetrain.upper()}. "
                        f"Want me to list our {pretty} {body_type+'s' if body_type else 'vehicles'} so you can ask about a specific one?")
        return (f"We don't currently have any {label}{label_noun}{price_qual}. "
                f"Would you like to widen your search?")

    matching.sort(key=lambda t: -t[0])

    if len(matching) == 1:
        _, r = matching[0]
        title = _vehicle_title(r)
        p = _row_price_int(r)
        price_str = f" for ${p:,}" if p > 0 else ""
        return (f"Yes - we have the {title}{price_str}. "
                f"Would you like more details or to schedule a visit?")

    LIST_LIMIT = 5
    lines = [f"Here are our {label}{label_noun}{price_qual}:"]
    for p, r in matching[:LIST_LIMIT]:
        title = _vehicle_title(r)
        price_str = f": ${p:,}" if p > 0 else ""
        lines.append(f"- {title}{price_str}")
    lines.append("")
    if len(matching) > LIST_LIMIT:
        lines.append(f"...and {len(matching) - LIST_LIMIT} more. Tell me a price range, year, or anything else and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


def _format_feature_listing(rows: List[Dict[str, Any]],
                            body_type: Optional[str] = None,
                            fuel_type: Optional[str] = None,
                            drivetrain: Optional[str] = None,
                            min_p: Optional[int] = None,
                            max_p: Optional[int] = None,
                            year: Optional[str] = None) -> str:
    """Deterministic listing of all inventory rows matching the given feature
    filters (body type / fuel / drivetrain), with optional price + year. Used
    when the customer asks for a category without naming a specific make."""
    matching = []
    for r in rows:
        if year and str(r.get("Year", "")).strip() != year:
            continue
        p = _row_price_int(r)
        if max_p is not None and p > max_p:
            continue
        if min_p is not None and p < min_p:
            continue
        if not _row_matches_features(r, body_type, fuel_type, drivetrain):
            continue
        matching.append((p, r))

    label_parts = []
    if year:
        label_parts.append(year)
    if drivetrain:
        label_parts.append(_DRIVETRAIN_LABEL.get(drivetrain, drivetrain.upper()))
    if fuel_type:
        label_parts.append(_FUEL_TYPE_LABEL.get(fuel_type, fuel_type))
    if body_type:
        label_parts.append(_BODY_TYPE_LABEL.get(body_type, body_type + "s"))
        label_noun = ""
    else:
        label_noun = " vehicles"
    label = " ".join(label_parts) if label_parts else "matching"

    if min_p is not None and max_p is not None:
        price_qual = f" between ${min_p:,} and ${max_p:,}"
    elif max_p is not None:
        price_qual = f" under ${max_p:,}"
    elif min_p is not None:
        price_qual = f" over ${min_p:,}"
    else:
        price_qual = ""

    if not matching:
        # Drivetrain data is incomplete in dealer inventory feeds. If the only
        # filter blocking all matches is drivetrain, acknowledge the data gap
        # rather than claim "zero" - which would mislead the customer.
        if drivetrain:
            no_dt_count = sum(
                1 for r in rows
                if (not year or str(r.get("Year", "")).strip() == year)
                and (max_p is None or _row_price_int(r) <= max_p)
                and (min_p is None or _row_price_int(r) >= min_p)
                and _row_matches_features(r, body_type, fuel_type, None)
            )
            if no_dt_count:
                body_label = (_BODY_TYPE_LABEL.get(body_type, body_type+"s") if body_type else "vehicles")
                return (f"Our listings don't always specify drivetrain, so I can't confirm "
                        f"which {body_label} are {drivetrain.upper()}. "
                        f"Want me to list our {body_label} so you can ask about a specific one?")
        return (f"We don't currently have any {label}{label_noun}{price_qual}. "
                f"Would you like to widen your search?")

    matching.sort(key=lambda t: -t[0])

    if len(matching) == 1:
        _, r = matching[0]
        title = _vehicle_title(r)
        p = _row_price_int(r)
        price_str = f" for ${p:,}" if p > 0 else ""
        return (f"Yes - we have the {title}{price_str}. "
                f"Would you like more details or to schedule a visit?")

    LIST_LIMIT = 5
    lines = [f"Here are our {label}{label_noun}{price_qual}:"]
    for p, r in matching[:LIST_LIMIT]:
        title = _vehicle_title(r)
        price_str = f": ${p:,}" if p > 0 else ""
        lines.append(f"- {title}{price_str}")
    lines.append("")
    if len(matching) > LIST_LIMIT:
        lines.append(f"...and {len(matching) - LIST_LIMIT} more. Tell me a price range, year, or anything else and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


def _is_more_question(body: str) -> bool:
    """Detect listing-continuation questions like 'is there anymore', 'is that all
    you have', 'what else'. Anchored patterns with an optional trailing 'you have /
    you got / in stock / of them / of those / here' so we catch natural variants
    without over-triggering on casual uses of 'more'."""
    s = (body or "").strip().lower()
    s = re.sub(r"[?.!,]+$", "", s).strip()
    if not s:
        return False
    suffix = r"(\s+(you\s+(have|got)|in\s+stock|of\s+them|of\s+those|here|now|left))?"
    patterns = [
        rf"^is\s+that\s+(it|all){suffix}$",
        rf"^thats?\s+(it|all){suffix}$",
        rf"^that\s+all{suffix}$",
        rf"^is\s+that\s+everything{suffix}$",
        rf"^thats?\s+everything{suffix}$",
        rf"^is\s+that\s+the\s+only\s+one\b",
        rf"^thats?\s+the\s+only\s+one\b",
        rf"^(the\s+)?only\s+one\s*\??$",
        rf"^just\s+(the\s+)?one\s*\??$",
        rf"^is\s+there\s+(any\s*)?(more|others?|anything\s+else){suffix}$",
        rf"^are\s+there\s+(any\s*)?(more|others?|anything\s+else){suffix}$",
        rf"^any\s*(more|others?|else){suffix}$",
        rf"^anything\s+else{suffix}$",
        rf"^what\s+else{suffix}$",
        rf"^what\s+other(s)?{suffix}$",
        rf"^show\s+(me\s+)?more{suffix}$",
        rf"^got\s+(any\s+)?more{suffix}$",
        rf"^do\s+you\s+have\s+(any\s+)?more{suffix}$",
        rf"^you\s+(got|have)\s+(any\s+)?more{suffix}$",
        rf"^so\s+you\s+have\s+more{suffix}$",
        rf"^so\s+(are\s+there\s+)?more{suffix}$",
        rf"^(any\s+)?more{suffix}$",
        r"^and\s*\?$",
    ]
    return any(re.search(p, s) for p in patterns)


def _row_id(r: Dict[str, Any]) -> str:
    """Stable row identifier for de-duplication. Uses VIN, then stock, then year+make+model+price."""
    vin = str(r.get("VIN", "")).strip().lower()
    if vin:
        return f"vin:{vin}"
    stock = str(r.get("Stock", "")).strip().lower()
    if stock:
        return f"stk:{stock}"
    return "ymp:{}|{}|{}|{}".format(
        str(r.get("Year", "")).strip().lower(),
        str(r.get("Make", "")).strip().lower(),
        str(r.get("Model", "")).strip().lower(),
        str(r.get("Price", "")).strip(),
    )


def _extract_listed_vehicles(text: str, candidate_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return rows from candidate_rows that appear to be referenced in `text`
    (year + at least one >=3-char model token both present)."""
    if not text or not candidate_rows:
        return []
    t = text.lower()
    listed = []
    for r in candidate_rows:
        year  = str(r.get("Year",  "")).strip()
        model = str(r.get("Model", "")).strip().lower()
        if not year:
            continue
        if year not in t:
            continue
        model_tokens = [tok for tok in re.sub(r"[^a-z0-9]", " ", model).split() if len(tok) >= 3]
        if not model_tokens:
            listed.append(r)
            continue
        if any(re.search(rf"\b{re.escape(tok)}\b", t) for tok in model_tokens):
            listed.append(r)
    return listed


def _handle_more_question(body: str, history: List[Dict[str, Any]],
                          inventory_rows: List[Dict[str, Any]]) -> Optional[str]:
    """If the customer is asking for the rest of a listing ('is there anymore',
    'is that all', 'what else'), infer the prior filter from history and
    deterministically list whatever wasn't already covered."""
    if not _is_more_question(body):
        return None

    # Walk back through user messages, skipping more-questions, to find the
    # original listing query that established the filter.
    original_query = None
    for msg in reversed(history):
        if msg.get("role") != "user":
            continue
        content = (msg.get("content") or "").strip()
        if not content or content == body:
            continue
        if _is_more_question(content):
            continue
        original_query = content
        break

    if not original_query:
        return None

    make_filters = _extract_make_filters(original_query, inventory_rows)
    min_p, max_p = _extract_price_range(original_query)
    yrm = re.search(r"\b(19|20)\d{2}\b", original_query)
    year_filter = yrm.group(0) if yrm else None
    body_filter = _extract_body_type(original_query)
    fuel_filter = _extract_fuel_type(original_query)
    drive_filter = _extract_drivetrain(original_query)

    if (not make_filters and min_p is None and max_p is None and not year_filter
            and not body_filter and not fuel_filter and not drive_filter):
        return None

    def _row_make_in_filters(rmake: str) -> bool:
        if not make_filters:
            return True
        for t in make_filters:
            if rmake == t or rmake.startswith(t + "-") or rmake.startswith(t + " "):
                return True
        return False

    matching = []
    for r in inventory_rows:
        rmake = str(r.get("Make", "")).strip().lower()
        if not _row_make_in_filters(rmake):
            continue
        if year_filter and str(r.get("Year", "")).strip() != year_filter:
            continue
        p = _row_price_int(r)
        if max_p is not None and p > max_p:
            continue
        if min_p is not None and p < min_p:
            continue
        if not _row_matches_features(r, body_filter, fuel_filter, drive_filter):
            continue
        matching.append(r)

    if not matching:
        return None

    already_keys = set()
    for msg in history[-10:]:
        if msg.get("role") != "assistant":
            continue
        content = msg.get("content", "") or ""
        for r in _extract_listed_vehicles(content, matching):
            already_keys.add(_row_id(r))

    remaining = [r for r in matching if _row_id(r) not in already_keys]

    if make_filters:
        def _pretty_for(target):
            for r in matching:
                if str(r.get("Make", "")).strip().lower() == target:
                    return str(r.get("Make", "")).strip()
            for r in inventory_rows:
                if str(r.get("Make", "")).strip().lower() == target:
                    return str(r.get("Make", "")).strip()
            return target.title()
        pretty_list = [_pretty_for(t) for t in make_filters]
        if len(pretty_list) == 1:
            pretty_make = pretty_list[0]
        elif len(pretty_list) == 2:
            pretty_make = f"{pretty_list[0]} and {pretty_list[1]}"
        else:
            pretty_make = ", ".join(pretty_list[:-1]) + ", and " + pretty_list[-1]
    else:
        pretty_make = ""

    if min_p is not None and max_p is not None:
        price_qual = f" between ${min_p:,} and ${max_p:,}"
    elif max_p is not None:
        price_qual = f" under ${max_p:,}"
    elif min_p is not None:
        price_qual = f" over ${min_p:,}"
    else:
        price_qual = ""

    scope_parts = []
    if year_filter:
        scope_parts.append(year_filter)
    if drive_filter:
        scope_parts.append(_DRIVETRAIN_LABEL.get(drive_filter, drive_filter.upper()))
    if fuel_filter:
        scope_parts.append(_FUEL_TYPE_LABEL.get(fuel_filter, fuel_filter))
    if pretty_make:
        scope_parts.append(pretty_make)
    if body_filter:
        scope_parts.append(_BODY_TYPE_LABEL.get(body_filter, body_filter + "s"))
        scope_noun = ""
    else:
        scope_noun = " vehicles"
    scope = " ".join(scope_parts)
    scope_phrase = f" {scope}" if scope else ""

    if not remaining:
        return (f"Yes - those are all the{scope_phrase}{scope_noun}{price_qual} we currently have. "
                f"Would you like more details on any of them or to schedule a visit?")

    if len(remaining) == 1:
        r = remaining[0]
        title = _vehicle_title(r)
        p = _row_price_int(r)
        price_str = f" for ${p:,}" if p > 0 else ""
        return (f"Yes - we also have the {title}{price_str}. "
                f"Would you like more details or to schedule a visit?")

    remaining.sort(key=lambda r: -_row_price_int(r))
    lines = [f"Yes - here are the rest of our{scope_phrase}{scope_noun}{price_qual}:"]
    for r in remaining:
        title = _vehicle_title(r)
        p = _row_price_int(r)
        price_str = f": ${p:,}" if p > 0 else ""
        lines.append(f"- {title}{price_str}")
    lines.append("")
    lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


def _is_generic_listing_query(body: str) -> bool:
    """Detect a generic 'browse the inventory' request with no make/price/year
    filter ('show me your inventory', 'what do you have', 'what cars do you have',
    "what's available", 'list your cars'). Specific filters are caught by 4.65/4.7
    earlier in the route, so this only fires on truly unfiltered browse intents."""
    s = (body or "").strip().lower()
    s = re.sub(r"[?.!,]+$", "", s).strip()
    if not s:
        return False
    patterns = [
        r"^show\s+(me\s+)?(your\s+|the\s+)?inventory\b",
        r"^show\s+(me\s+)?(your\s+|the\s+)?(cars|vehicles|stock|lot|selection|everything|what\s+you\s+have)\b",
        r"^what\s+(do\s+)?you\s+(have|got|carry)\s*\??$",
        r"^what\s+(cars|vehicles|kind\s+of\s+cars|kind\s+of\s+vehicles|makes|models)\s+(do\s+you\s+have|are\s+available|are\s+on\s+the\s+lot)\b",
        r"^what(?:\s+is|'?s)\s+(available|on\s+the\s+lot|in\s+stock|for\s+sale)\b",
        r"^browse(\s+(inventory|cars|vehicles))?\b",
        r"^(see|view)\s+(your\s+|the\s+)?(inventory|cars|vehicles|stock|selection)\b",
        r"^list\s+(your\s+|the\s+|all\s+)?(cars|vehicles|inventory|stock)\b",
        r"^(show|list)\s+all(\s+(your\s+|the\s+))?(cars|vehicles|inventory)?\b",
        r"^all\s+(your\s+|the\s+)?(cars|vehicles|inventory)\b",
        r"^everything\s+(you\s+have|on\s+the\s+lot|in\s+stock|for\s+sale)\b",
        r"^do\s+you\s+have\s+(any\s+)?(cars|vehicles)\s*(for\s+sale|available)?$",
    ]
    return any(re.search(p, s) for p in patterns)


def _format_generic_listing(rows: List[Dict[str, Any]], limit: int = 10) -> str:
    """Deterministic top-N inventory snapshot. Sorted by year descending
    (newest first), price descending as tiebreaker. Used when the customer
    asks for a generic browse with no make/price/year filter - replaces
    LLM-driven listings that were hallucinating non-existent vehicles."""
    valid = []
    for r in rows:
        year  = str(r.get("Year",  "")).strip()
        make  = str(r.get("Make",  "")).strip()
        model = str(r.get("Model", "")).strip()
        if not (year and make and model):
            continue
        valid.append(r)

    if not valid:
        return ("Our inventory list isn't available right now. "
                "Please contact us directly and we'll be happy to share what we currently have.")

    def _sort_key(r):
        year = str(r.get("Year", "")).strip()
        try:
            yi = int(year)
        except ValueError:
            yi = 0
        return (-yi, -_row_price_int(r))

    valid.sort(key=_sort_key)
    shown = valid[:limit]

    lines = ["Here's a snapshot of our current inventory:"]
    for r in shown:
        year  = str(r.get("Year",  "")).strip()
        make  = str(r.get("Make",  "")).strip()
        model = str(r.get("Model", "")).strip()
        title = " ".join(s for s in [year, make, model] if s)
        p = _row_price_int(r)
        price_str = f": ${p:,}" if p > 0 else ""
        lines.append(f"- {title}{price_str}")

    lines.append("")
    if len(valid) > limit:
        lines.append(f"...and {len(valid) - limit} more. Tell me a make, model, or price range and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


def _extract_car_from_last_bot_message(history: List[Dict[str, Any]], inventory_rows: List[Dict[str, Any]]):
    """Find the vehicle the bot most recently mentioned. Returns None if the
    most recent assistant message named multiple plausible cars (e.g. a price
    listing) - the caller should fall through to the LLM rather than guess."""
    for msg in reversed(history):
        if msg.get("role") != "assistant":
            continue
        content = (msg.get("content") or "").lower()
        # Build a hyphen-stripped word set from content so "F-250" (which
        # tokenizes to ["f", "250"] under default splitting) still matches a
        # row whose model nameplate is "F-250" -> "f250".
        content_words = set(re.split(
            r"[^a-z0-9]+",
            content.replace("-", ""),
        ))
        best_row, best_score = None, 0
        plausible = 0
        for r in inventory_rows:
            year  = str(r.get("Year",  "")).strip().lower()
            make  = str(r.get("Make",  "")).strip().lower()
            model = str(r.get("Model", "")).strip().lower()
            # Require the model NAMEPLATE (first token of the model field) to
            # appear in the message before counting this vehicle as plausibly
            # mentioned. Year+make alone isn't enough - otherwise a 2019 Jetta
            # would be counted whenever a 2019 Volkswagen Tiguan was named.
            first_tok = model.split()[0] if model.split() else ""
            nameplate = re.sub(r"[^a-z0-9]", "", first_tok)
            if not nameplate or len(nameplate) < 2:
                continue
            if nameplate not in content_words:
                continue
            score = 0
            if year and year in content:   score += 2
            if make and make in content:   score += 3
            score += 3  # nameplate match (already required above)
            plausible += 1
            if score > best_score:
                best_score, best_row = score, r
        # If the last bot message named several cars (a list), refuse to pick
        # one arbitrarily - caller falls through to the LLM with full context.
        if best_row and plausible <= 1:
            return best_row
        if plausible >= 2:
            return None
    return None


def _best_history_vehicle_match(rows, history_text):
    h = (history_text or "").lower()
    if not h or not rows:
        return None
    best_row, best_score = None, 0
    for r in rows:
        make  = str(r.get("Make",  "")).strip().lower()
        model = str(r.get("Model", "")).strip().lower()
        year  = str(r.get("Year",  "")).strip().lower()
        score = (2 if make and make in h else 0) + (2 if model and model in h else 0) + (1 if year and year in h else 0)
        if model:
            tokens = [t for t in re.sub(r"[^a-z0-9 ]", " ", model).split() if len(t) >= 2]
            if tokens and all(t in h for t in tokens):
                score += 1
        if score > best_score:
            best_score, best_row = score, r
    return best_row if best_score > 0 else None


def inventory_row_details(r: Dict[str, Any]) -> str:
    year    = str(r.get("Year",  "")).strip()
    make    = str(r.get("Make",  "")).strip()
    model   = str(r.get("Model", "")).strip()
    trim    = str(r.get("Trim",  "")).strip()
    color   = str(r.get("Color",   "")).strip()
    price   = str(r.get("Price",   "")).strip()
    mileage = str(r.get("Mileage", "")).strip()
    vin          = get_row_field(r, VIN_ALIASES).strip()
    stock        = get_row_field(r, STOCK_ALIASES).strip()
    issues       = " | ".join(get_row_field_values(r, ISSUE_NOTE_HEADER_ALIASES))
    work_done    = " | ".join(get_row_field_values(r, MAINT_WORK_HEADER_ALIASES))
    title_status = " | ".join(get_row_field_values(r, TITLE_STATUS_ALIASES))
    # Description comes from scraped websites (engine, options, etc.)
    description = str(r.get("Description", "")).strip()
    carfax_url  = str(r.get("CarfaxURL", "")).strip()

    title = " ".join(p for p in [year, make, model, trim] if p) or "Vehicle"
    lines = [title]
    extras = []
    if color:   extras.append(f"Color: {color}")
    if mileage: extras.append(f"Mileage: {mileage} mi")
    if price:   extras.append(f"Price: ${price}")
    if stock:   extras.append(f"Stock: {stock}")
    if vin:     extras.append(f"VIN: {vin}")
    lines.extend(extras)
    if title_status:
        lines.append(f"Title status: {title_status}")
    if issues:
        lines.append(f"Known issues: {issues}")
    if work_done:
        lines.append(f"Features / highlights: {work_done}")
    if description:
        lines.append(f"Details: {description}")
    if carfax_url:
        lines.append(f"CarFax report: {carfax_url}")
    return "\n".join(lines)


# =========================
# DETERMINISTIC RESPONSE HELPERS
# =========================

UNKNOWN_ANSWER_PREFIX = "I don't have that information readily available. Please feel free to contact us at "
UNKNOWN_PATTERNS = re.compile(
    r"\b(i(?:\s*am|'m)?\s*not\s*sure|i\s*don'?t\s*know|i\s*do\s*not\s*know|"
    r"i\s*don'?t\s*have|i\s*do\s*not\s*have|not\s*listed|not\s*seeing|"
    r"can'?t\s*confirm|cannot\s*confirm|no\s*details\s*listed)\b",
    re.I,
)
# These phrases mean the AI gave a valid "not in inventory" answer - don't replace them
NOT_IN_INVENTORY_PATTERNS = re.compile(
    r"\b(not\s+in\s+our\s+(?:current\s+)?inventory|don'?t\s+(?:currently\s+)?have\s+that|"
    r"not\s+(?:currently\s+)?(?:in\s+stock|available|listed|carry|carrying)|"
    r"isn'?t\s+(?:currently\s+)?(?:in\s+our|available)|"
    r"we\s+don'?t\s+(?:currently\s+)?(?:carry|have|stock)|"
    r"that\s+(?:vehicle|car|model)\s+is\s+not|"
    r"unfortunately\s+(?:we\s+)?(?:don'?t|do\s+not)|"
    r"don'?t\s+(?:currently\s+)?(?:have|carry|stock)\s+(?:a\s+|any\s+|that\s+)?(?:\w+\s+){0,4}(?:in\s+(?:our|my|the)\s+inventory|available|in\s+stock)|"
    r"(?:that(?:\s+\w+){0,4}|it)\s+(?:is\s+)?not\s+(?:currently\s+)?(?:something\s+we|in\s+our)|"
    r"we\s+(?:currently\s+)?(?:do\s+not|don'?t)\s+(?:have|carry|stock|offer)|"
    r"not\s+(?:something\s+we|part\s+of\s+our|in\s+our\s+current))\b",
    re.I,
)


def build_unknown_answer(dealer_phone: str) -> str:
    dealer_phone = normalize_phone(dealer_phone)
    if dealer_phone:
        return f"{UNKNOWN_ANSWER_PREFIX}{dealer_phone} and one of our representatives will be glad to assist you"
    return "I don't have that information readily available. Please contact us directly and one of our representatives will be glad to assist you"


def should_force_unknown_answer(reply_text: str) -> bool:
    text = reply_text or ""
    # Don't replace valid "not in our inventory" answers with the generic contact message
    if NOT_IN_INVENTORY_PATTERNS.search(text):
        return False
    return bool(UNKNOWN_PATTERNS.search(text))


def _issue_response_for_match(r):
    title   = _vehicle_title(r)
    issues  = " | ".join(get_row_field_values(r, ISSUE_NOTE_HEADER_ALIASES)).strip()
    service = " | ".join(get_row_field_values(r, MAINT_WORK_HEADER_ALIASES)).strip()
    if issues:
        return f"Regarding the {title} - disclosed concerns: {issues}." + (f" Features/highlights: {service}." if service else "")
    if service:
        return f"The {title} has no known issues on file. Features/highlights: {service}."
    return f"The {title} has no known issues on file."


def _title_status_response_for_match(r):
    title        = _vehicle_title(r)
    title_status = " | ".join(get_row_field_values(r, TITLE_STATUS_ALIASES)).strip()
    return f"The {title} carries a {title_status} title." if title_status else f"Title status information is not currently on file for the {title}."


def _dealer_info_response(dealer: Dict[str, Any], dealer_phone: str, msg: str = "") -> str:
    msg_lower    = (msg or "").lower()
    dealer_name  = get_row_field(dealer, DEALER_NAME_ALIASES) or "the dealership"
    dealer_phone = normalize_phone(dealer_phone)

    if re.search(r"\b(address|location|where|located|directions)\b", msg_lower):
        return f"We are located at {get_row_field(dealer, DEALER_ADDRESS_ALIASES) or '(not listed)'}."
    if re.search(r"\b(hour|hours|open|close|operation)\b", msg_lower):
        return f"Our hours of operation are {get_row_field(dealer, DEALER_HOURS_ALIASES) or '(not listed)'}."
    if re.search(r"\b(financ\w*)\b", msg_lower):
        return f"Regarding financing: {get_row_field(dealer, DEALER_FINANCING_ALIASES) or '(not listed)'}."
    if re.search(r"\b(trade[- ]?in)\b", msg_lower):
        return f"Regarding trade-ins: {get_row_field(dealer, DEALER_TRADEINS_ALIASES) or '(not listed)'}."
    if re.search(r"\b(polic|rules|restrictions)\b", msg_lower):
        return f"Our dealership policy: {get_row_field(dealer, DEALER_POLICIES_ALIASES) or '(none listed)'}."

    address   = get_row_field(dealer, DEALER_ADDRESS_ALIASES) or "(not listed)"
    hours     = get_row_field(dealer, DEALER_HOURS_ALIASES) or "(not listed)"
    financing = get_row_field(dealer, DEALER_FINANCING_ALIASES) or "(not listed)"
    tradeins  = get_row_field(dealer, DEALER_TRADEINS_ALIASES) or "(not listed)"
    policies  = get_row_field(dealer, DEALER_POLICIES_ALIASES) or "(none)"
    phone_part = f", and you may reach us at {dealer_phone}" if dealer_phone else ""
    return (
        f"{dealer_name} is located at {address}, with operating hours of {hours}{phone_part}. "
        f"Financing: {financing}. Trade-ins: {tradeins}. Additional notes: {policies}."
    )


# =========================
# INTENT DETECTORS
# =========================

def _is_stock_number_question(msg):
    return bool(re.search(r"\b(stock|stock\s*#|stock\s*number)\b", (msg or "").lower()))

def _is_vin_question(msg):
    return bool(re.search(r"\bvin\b|\bvehicle\s+identification\s+number\b", (msg or "").lower()))

def _is_dealer_phone_question(msg):
    msg = (msg or "").lower()
    if _is_stock_number_question(msg) or _is_vin_question(msg):
        return False
    asks_phone = bool(re.search(r"\b(phone|call|contact|number)\b", msg))
    dealer_context = bool(re.search(r"\b(dealer|dealership|you\s+guys|you\s+all|y'?all|location|store|lot|office)\b", msg))
    generic_phone_number = "phone number" in msg and not re.search(r"\b(stock|vin|mileage|price)\b", msg)
    return (asks_phone and dealer_context) or bool(generic_phone_number)

def _is_dealer_warranty_question(msg):
    """Questions about warranties/services the dealership offers - not a specific car's features."""
    return bool(re.search(
        r"\b(warrant(y|ies)|guarantee|after.?sale\s+service|service\s+plan|"
        r"protection\s+plan|coverage\s+(plan|option)|coverage\s+option|"
        r"enhanced\s+coverage|powertrain|premier\s+coverage|ultimate\s+coverage|"
        r"what\s+warrant|what\s+coverage|"
        r"do\s+you\s+(offer|provide|include|give)\s+\w*\s*warrant|"
        r"do\s+you\s+(offer|provide|include|give)\s+\w*\s*guarantee)\b",
        (msg or "").lower(),
    ))

def _is_vehicle_detail_question(msg):
    # Don't catch warranty questions here - those are dealer policy questions
    if _is_dealer_warranty_question(msg):
        return False
    return bool(re.search(
        r"\b(engine|motor|horsepower|hp|torque|cylinder|v6|v8|v10|v12|turbocharg|supercharg|"
        r"interior|leather|seats?|upholstery|cabin|headroom|legroom|exterior|body style|body type|"
        r"convertible|coupe|sedan|suv|truck|van|transmission|automatic|manual|gearbox|gear|"
        r"drivetrain|awd|rwd|fwd|4wd|4x4|all.wheel|rear.wheel|front.wheel|"
        r"mpg|fuel economy|gas mileage|fuel type|hybrid|electric|diesel|"
        r"suspension|wheelbase|dimensions|length|width|height|weight|towing|"
        r"sound system|audio|speakers|navigation|nav|"
        r"sunroof|moonroof|bluetooth|backup camera|parking sensor|lane.keep|blind.spot|"
        r"heated seat|cooled seat|ventilated seat|heated steering|"
        r"package|option|feature|trim|what kind|what type|does it have|does it come)\b",
        (msg or "").lower(),
    ))

def _is_issue_question(msg):
    return bool(re.search(
        r"\b(issue|issues|problem|problems|anything wrong|what'?s wrong|fault|damage|"
        r"needs work|concern|concerns|condition)\b",
        (msg or "").lower(),
    ))

def _is_general_info_question(msg):
    if _is_dealer_warranty_question(msg) or _is_financing_question(msg):
        return False
    return bool(re.search(
        r"\b(more info|more information|anymore information|any more information|"
        r"tell me more|more details|more about|details on|info on|information on|"
        r"what can you tell|what else|can you tell me more|give me more|anything else about|"
        r"what.?s it like|describe it|describe the|learn more|"
        r"what about the|what about it|what about that|how about the|how about that|"
        r"tell me about|all the info|all the information|everything about|all details|full details|"
        r"is that all|that all you have|anything else on it|what else do you have|"
        r"show me the|show me more|give me info|give me details|"
        r"can I get info|can I get details|can I get more|any other info|any other details|"
        r"any more details|anymore details|what do you have on|what.?s the deal with|"
        r"rundown on|overview of|overview on|break it down|break down the)\b",
        (msg or "").lower(),
    ))

def _is_vehicle_link_question(msg):
    """Customer asking for the listing URL of a specific vehicle."""
    return bool(re.search(
        r"\b(link|url|web\s*page|webpage|web\s*link)\b",
        (msg or "").lower(),
    ))

def _is_title_status_question(msg):
    return bool(re.search(
        r"\b(clean\s+title|title\s+status|salvage\s+title|rebuilt\s+title|"
        r"title\s+clean|is\s+it\s+clean\s+title|is\s+the\s+title\s+clean|what'?s\s+the\s+title)\b",
        (msg or "").lower(),
    ))

def _is_dealer_info_question(msg):
    msg = (msg or "").lower()
    if "dealership" in msg or "dealer" in msg:
        if re.search(r"\b(info|information|details|about|hours|address|location|financing|trade[- ]?ins?|policy|policies)\b", msg):
            return True
    return bool(re.search(
        r"\b(dealership info|dealer info|about the dealership|your hours|business hours|"
        r"what are your hours|hours of operation|what time do you open|what time do you close|"
        r"when do you open|when do you close|are you open|when are you open|"
        r"address|location|where are you|where you at|how do i get there|"
        r"financing|finance|do you finance|can i finance|offer financing|"
        r"trade[- ]?ins?|will you take my|can i trade|policy|policies|rules|restrictions)\b",
        msg,
    ))

def _is_pricing_policy_question(msg):
    return bool(re.search(r"\b(discount|best price|price negotiable|negotiable|deal on price|lower price)\b", (msg or "").lower()))

def _is_financing_question(msg):
    return bool(re.search(
        r"\b(financ\w*|payment\s*plan|monthly\s*payment|down\s*payment|loan|apr|interest\s*rate|"
        r"bad\s*credit|good\s*credit|no\s*credit|credit\s*check|credit\s*approv|credit\s*score|"
        r"\d{3}\s*credit|accept.*credit|credit.*accept|credit.*ok|ok.*credit|"
        r"pay\s*monthly|monthly\s*installment|afford|buy\s*here\s*pay\s*here|bhph)\b",
        (msg or "").lower(),
    ))


# =========================
# SMS SEND HELPERS
# =========================

def _send_sms(to: str, from_number: str, body: str) -> Tuple[bool, str]:
    to = normalize_phone(to)
    from_number = normalize_phone(from_number)
    if not to:
        return False, "Missing recipient phone"
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN):
        return False, "Missing Twilio credentials"
    try:
        tw = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        kwargs: Dict[str, Any] = {"to": to, "body": body[:1500]}
        if TWILIO_MESSAGING_SERVICE_SID:
            kwargs["messaging_service_sid"] = TWILIO_MESSAGING_SERVICE_SID
        else:
            if not from_number:
                return False, "Missing from number"
            kwargs["from_"] = from_number
        tw.messages.create(**kwargs)
        return True, "sent"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def notify_all_staff(dealer_row: Dict[str, Any], from_number: str, body: str) -> None:
    dealer_ph = normalize_phone(get_row_field(dealer_row, DEALER_NOTIFY_PHONE_ALIASES))
    salesman_phones = get_salesman_phones(dealer_row)

    # Merge dealer phone + salesman phones, dedup, preserve order
    phones: List[str] = []
    seen = set()
    for raw in ([dealer_ph] if dealer_ph else []) + salesman_phones:
        np = normalize_phone(raw)
        if np and np not in seen:
            seen.add(np)
            phones.append(np)

    if not phones:
        app.logger.warning("No notification phones found for %s - skipping",
                           get_row_field(dealer_row, DEALER_NAME_ALIASES))
        return

    app.logger.info("Notifying %d phone(s): %s", len(phones), phones)
    for phone in phones:
        if phone == normalize_phone(from_number):
            app.logger.warning("Skipping staff notify: To == From (%s)", phone)
            continue
        ok, err = _send_sms(phone, from_number, body)
        if ok:
            app.logger.info("Staff notified: %s", phone)
        else:
            app.logger.warning("Staff notify failed for %s: %s", phone, err)


def send_sms_to_customer(*, customer_phone: str, from_number: str, body: str) -> Tuple[bool, str]:
    return _send_sms(customer_phone, from_number, body)


# =========================
# ALERT BODY HELPERS
# =========================

def _format_customer_lines(customer_name: str = "", customer_last_name: str = "", customer_email: str = "") -> str:
    full = " ".join(p for p in (customer_name, customer_last_name) if p).strip()
    lines = []
    if full:
        lines.append(f"Customer Name: {full}")
    if customer_email:
        lines.append(f"Email: {customer_email}")
    return ("\n".join(lines) + "\n") if lines else ""


def _dealer_alert_body(*, customer_phone, customer_name="", customer_last_name="", customer_email="",
                      dealership_line, visit_time, car_desc, additional_info=""):
    body = (
        "Appointment confirmed\n"
        f"{_format_customer_lines(customer_name, customer_last_name, customer_email)}"
        f"Customer: {customer_phone}\n"
        f"Time: {visit_time}\n"
        f"Vehicle: {car_desc}\n"
        f"Dealership line: {dealership_line}"
    )
    if additional_info:
        body += f"\n\nAdditional Information:\n{additional_info}"
    return body


def _dealer_reconfirm_body(*, customer_phone, customer_name="", customer_last_name="", customer_email="",
                          dealership_line, visit_time, car_desc):
    return (
        "Appointment re-confirmed\n"
        f"{_format_customer_lines(customer_name, customer_last_name, customer_email)}"
        f"Customer: {customer_phone}\n"
        f"Re-confirmed for {visit_time} to see the {car_desc}\n"
        f"Dealership line: {dealership_line}"
    )


def _dealer_reschedule_body(*, customer_phone, customer_name="", customer_last_name="", customer_email="",
                           dealership_line, visit_time, car_desc, additional_info=""):
    body = (
        "Appointment rescheduled\n"
        f"{_format_customer_lines(customer_name, customer_last_name, customer_email)}"
        f"Customer: {customer_phone}\n"
        f"New time: {visit_time}\n"
        f"Vehicle: {car_desc}\n"
        f"Dealership line: {dealership_line}"
    )
    if additional_info:
        body += f"\n\nAdditional Information:\n{additional_info}"
    return body


def _dealer_cancellation_body(*, customer_phone, customer_name="", customer_last_name="", customer_email="",
                             dealership_line, visit_time, car_desc):
    return (
        "Appointment cancelled\n"
        f"{_format_customer_lines(customer_name, customer_last_name, customer_email)}"
        f"Customer: {customer_phone}\n"
        f"Original time: {visit_time}\n"
        f"Vehicle: {car_desc}\n"
        f"Dealership line: {dealership_line}"
    )


# =========================
# AI HELPERS
# =========================

def extract_customer_insights(history: List[Dict[str, Any]]) -> str:
    if not history:
        return ""
    convo_lines = [
        f"{'Customer' if m.get('role')=='user' else 'Consultant'}: {(m.get('content') or '').replace(chr(10), ' ').strip()}"
        for m in history if (m.get("content") or "").strip()
    ]
    prompt = f"""You are reviewing a car dealership SMS conversation to extract useful customer information for the dealer.

Conversation:
{chr(10).join(convo_lines)}

Extract ONLY genuinely useful facts the customer explicitly stated or expressed clear interest in. Look for:
- Trade-in vehicle (year, make, model, mileage, title status, condition)
- Credit situation (good credit, bad credit, needs financing, paying cash)
- Budget or price range
- Warranty interest (extended warranty, service contract, GAP coverage - yes/no/asked about)
- Add-on services interest (detailing, ceramic coating, tinting, accessories, maintenance plans)
- Specific concerns or requirements about the vehicle

Rules:
- Only include what the customer actually said or affirmed - do not infer
- For warranty / add-on services: include them whether the answer was YES or NO, as long as it was discussed (e.g. "Interested in extended warranty" or "Declined extended warranty")
- Be concise - short bullet points
- If nothing notable was mentioned, reply with exactly: NONE
- Do NOT include the car they are seeing or the appointment time

Reply with bullet points only.""".strip()
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
        )
        result = (resp.choices[0].message.content or "").strip()
        return "" if not result or result.upper() == "NONE" else result
    except Exception as e:
        app.logger.warning("extract_customer_insights failed: %s", e)
        return ""


def ai_vehicle_detail_reply(customer_msg, vehicle_data, dealer_phone, history):
    history_snippet = " ".join((m.get("content") or "") for m in history[-4:])
    prompt = f"""You are a professional automotive sales consultant responding via SMS.

A customer asked: "{customer_msg}"

Vehicle data (use ONLY this - do not guess):
{vehicle_data}

Recent conversation: {history_snippet or "(none)"}

Write one natural, conversational SMS reply. 1-3 sentences. No bullet points. Do not reference spreadsheets or databases.""".strip()
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.warning("ai_vehicle_detail_reply failed: %s", e)
        return ""


# Section names emitted by the dealer's spec-sheet pages. Listed longest-first
# so prefix matching picks "Stability and Traction" over "Stability".
_SPEC_SECTIONS_LONG_FIRST = sorted([
    "Air Conditioning", "Airbags", "Audio System", "Brakes",
    "Comfort Features", "Convenience Features", "Exterior Features",
    "In Car Entertainment", "Instrumentation", "Lights", "Mirrors",
    "Powertrain", "Roof", "Safety", "Seatbelts", "Seats", "Security",
    "Stability and Traction", "Suspension", "Telematics",
    "Wheels and Tires", "Windows",
], key=len, reverse=True)

# Order in which sections are shown - most buyer-relevant first. If the total
# overview goes over budget, lower-priority sections drop off the end.
_SPEC_SECTIONS_DISPLAY_ORDER = [
    "Powertrain", "In Car Entertainment", "Audio System", "Air Conditioning",
    "Roof", "Seats", "Comfort Features", "Convenience Features", "Safety",
    "Lights", "Mirrors", "Telematics", "Wheels and Tires",
    "Exterior Features", "Brakes", "Suspension", "Stability and Traction",
    "Instrumentation", "Airbags", "Seatbelts", "Security", "Windows",
]


def _split_section_block(block: str) -> tuple:
    """Return (section_name, content) by matching the longest known prefix."""
    block = (block or "").strip()
    for name in _SPEC_SECTIONS_LONG_FIRST:
        if block == name:
            return name, ""
        if block.startswith(name + " "):
            return name, block[len(name):].strip()
        if block.startswith(name + ":"):
            return name, block[len(name) + 1:].strip()
    return "", block


_MAX_ITEMS_PER_SECTION = 3   # only the top items per section
_MAX_SECTIONS_DISPLAYED = 8  # drop low-priority sections entirely


def _section_items(content: str) -> List[str]:
    """Return a section's items as separate lines.

    Newer scraped data uses " ;; " as an item sentinel (one item per line in
    the source HTML). Older data has no sentinel - fall back to a single line.
    """
    s = (content or "").strip()
    if not s:
        return []
    if " ;; " in s:
        items = [re.sub(r"\s+", " ", it).strip() for it in s.split(" ;; ")]
        items = [it for it in items if it]
    else:
        items = [re.sub(r"\s+", " ", s).strip()]
    return items[:_MAX_ITEMS_PER_SECTION]


def format_vehicle_overview(row: Dict[str, Any]) -> str:
    """Deterministic single-message vehicle overview. No LLM in the loop."""
    title_parts = [
        str(row.get("Year", "")).strip(),
        str(row.get("Make", "")).strip(),
        str(row.get("Model", "")).strip(),
        str(row.get("Trim", "")).strip(),
    ]
    title = " ".join(p for p in title_parts if p) or "Vehicle"

    out = [title, ""]

    price   = str(row.get("Price",   "")).strip()
    mileage = str(row.get("Mileage", "")).strip()
    color   = str(row.get("Color",   "")).strip()
    vin     = get_row_field(row, VIN_ALIASES).strip()
    stock   = get_row_field(row, STOCK_ALIASES).strip()
    if price:   out.append(f"Price: ${price}")
    if mileage: out.append(f"Mileage: {mileage} mi")
    if color:   out.append(f"Color: {color}")
    if vin:     out.append(f"VIN: {vin}")
    if stock:   out.append(f"Stock: {stock}")

    description = str(row.get("Description", "")).strip()
    if not description:
        return "\n".join(out).strip()

    # Description has up to 3 zones separated by " || ":
    #   1) free-form marketing copy (skip)
    #   2) "Engine: ... | Transmission: ... | Fuel: ... | Interior: ... | Title: ..."
    #   3) feature blocks separated by " | ", each starting with a section name
    parts = [p.strip() for p in description.split(" || ")]
    spec_block = ""
    feature_text = ""
    if len(parts) >= 3:
        spec_block, feature_text = parts[1], parts[2]
    elif len(parts) == 2:
        if "Engine:" in parts[0] or "Transmission:" in parts[0]:
            spec_block, feature_text = parts[0], parts[1]
        else:
            feature_text = parts[1]
    elif len(parts) == 1 and ("Engine:" in parts[0] or " | " in parts[0]):
        feature_text = parts[0]

    if spec_block:
        out.append("")
        out.append("Details:")
        for spec in spec_block.split(" | "):
            spec = spec.strip()
            if spec and ":" in spec:
                out.append(spec)

    if feature_text:
        # Map raw blocks to (section_name, content)
        section_map: Dict[str, str] = {}
        for block in feature_text.split(" | "):
            section, content = _split_section_block(block)
            if section and section not in section_map:
                section_map[section] = content

        # Emit in display priority order, then any unknown sections last.
        # Full content goes through; _split_for_sms chunks into multiple bubbles.
        ordered = [s for s in _SPEC_SECTIONS_DISPLAY_ORDER if s in section_map]
        for s in section_map:
            if s not in ordered:
                ordered.append(s)
        shown = 0
        for section in ordered:
            if shown >= _MAX_SECTIONS_DISPLAYED:
                break
            items = _section_items(section_map[section])
            if not items:
                continue
            out.append("")
            out.append(f"{section}:")
            out.extend(items)
            shown += 1

    return "\n".join(out).strip()


def ai_refine_vehicle_overview(overview: str) -> str:
    """Ask GPT to trim overwhelming feature sections while keeping the exact structure.

    Returns the refined text on success, or the original overview on any failure
    (network error, malformed response, output that lost the structure).
    """
    if not overview or len(overview) < 400:
        return overview
    prompt = f"""You are formatting an SMS reply about a used vehicle. Below is the raw overview. Trim it so it's not overwhelming, but keep the EXACT visual structure (one item per line, blank line between blocks).

Required output shape:

<Year Make Model Trim>

Price: $<price>
Mileage: <miles> mi
Color: <color>
VIN: <vin>
Stock: <stock>

Details:
Engine: <engine>
Transmission: <transmission>
Fuel: <fuel>
Interior: <interior>
Title: <title>

<Section Name>:
<item 1>
<item 2>
<item 3>

<Section Name>:
<item 1>
<item 2>

Hard rules:
- Keep the title line, the Price/Mileage/Color/VIN/Stock block, and the "Details:" block UNCHANGED from the raw overview.
- For each feature section (Powertrain, In Car Entertainment, Audio System, Air Conditioning, Seats, Comfort Features, Convenience Features, Safety, etc.), keep the section header verbatim (with trailing colon) and trim its items to the 3-4 most useful ones. ONE item per line - do not merge items onto one line, do not use commas to combine them.
- Preserve section order from the raw overview. Preserve the blank line between every block.
- No bullets, no dashes, no marketing copy, no closing question, no extra commentary.
- No Markdown - do NOT add trailing spaces to lines, do NOT use ** or *.
- If a field is missing in the raw overview (e.g., no Mileage, no VIN), DROP that entire line. Never write placeholder text like "<miles>" or "<vin>" or "N/A".
- Output ONLY the refined overview text - no preamble, no code fences.

Raw overview:
{overview}""".strip()
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
        )
        refined = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.warning("ai_refine_vehicle_overview failed: %s", e)
        return overview

    # Strip trailing whitespace from every line (kills Markdown line-break spaces).
    refined = "\n".join(ln.rstrip() for ln in refined.splitlines())
    # Drop any line containing an unfilled placeholder (e.g., "<miles>", "<vin>").
    refined = "\n".join(ln for ln in refined.splitlines() if not re.search(r"<[a-z][a-z _]*>", ln))

    # Sanity check: refined output must keep the title and the Price line.
    first_line = overview.splitlines()[0].strip()
    if not refined or first_line not in refined or "Price:" not in refined:
        return overview
    return _ensure_blank_lines_before_sections(refined)


_KNOWN_SECTION_HEADERS = set(_SPEC_SECTIONS_LONG_FIRST) | {"Details"}


def _ensure_blank_lines_before_sections(text: str) -> str:
    """Guarantee a blank line before every section header.

    The AI refine pass occasionally collapses the separator between sections
    (e.g., 'Child safety door locksLights:'). This walks the output and
    re-inserts the missing newline + blank line before any known header.
    """
    # First, split glued tokens like 'door locksLights:' -> 'door locks\nLights:'
    def _split_glued(match: re.Match) -> str:
        return f"{match.group(1)}\n{match.group(2)}:"
    header_alt = "|".join(re.escape(h) for h in sorted(_KNOWN_SECTION_HEADERS, key=len, reverse=True))
    text = re.sub(rf"([a-z\)\]])({header_alt}):", _split_glued, text)

    # Then ensure each header line is preceded by a blank line.
    lines = text.split("\n")
    out: List[str] = []
    for ln in lines:
        stripped = ln.strip()
        is_header = stripped.endswith(":") and stripped[:-1] in _KNOWN_SECTION_HEADERS
        if is_header and out and out[-1].strip() != "":
            out.append("")
        out.append(ln)
    return "\n".join(out)


def ai_vehicle_full_overview(vehicle_data, dealer_phone):
    # Cap input so the model has room to summarize. Full row stays in the DB
    # for follow-up questions via ai_vehicle_detail_reply.
    if len(vehicle_data) > 5000:
        vehicle_data = vehicle_data[:5000]
    prompt = f"""You are a professional automotive sales consultant responding via SMS.

Vehicle data (use ONLY this - do not guess):
{vehicle_data}

Output EXACTLY this structure:

<year make model trim>

Price: $<price>
Mileage: <miles> mi
Color: <color>
VIN: <vin>
Stock: <stock>

Details:
Engine: <engine>
Drivetrain: <drivetrain>
Transmission: <transmission>
Fuel: <fuel>
Interior: <interior>

Features:
<For EACH feature category present in the data - Air Conditioning, Audio System, Brakes, Comfort, Convenience, Exterior, In Car Entertainment, Lights, Mirrors, Powertrain, Safety, Seats, Security, Stability and Traction, Suspension, Telematics, Wheels and Tires, Windows, etc - output ONE compact line summarizing the 2-4 most relevant items in that category. Format: "Category: item, item, item". Keep each line under 110 characters.>

Hard rules:
- Cover EVERY feature category that appears in the data. Do not drop categories.
- One line per category. Inside the line, comma-separate the most useful items only - never list every sub-field.
- Skip categories or fields that aren't in the data. Don't write "not specified", "N/A", or empty values.
- TOTAL output must stay under 1400 characters.
- No bullets, no dashes, no marketing copy, no disclaimers, no phone numbers, no closing question.""".strip()
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
        )
        result = (resp.choices[0].message.content or "").strip()
        if len(result) > 1500:
            result = result[:1490].rstrip() + "..."
        return result
    except Exception as e:
        app.logger.warning("ai_vehicle_full_overview failed: %s", e)
        return ""


def ai_policy_reply(customer_msg, topic, policy_text, dealer_phone, history, customer_name=""):
    """
    Give a natural conversational response about financing or trade-ins.
    Primary goal: gather customer info (credit score, trade-in details) so the
    dealer is well-informed before the visit. Only share links/phone numbers
    if the customer's question genuinely cannot be answered from the policy text.
    """
    convo_lines = [
        f"{'Customer' if m.get('role')=='user' else 'Consultant'}: {(m.get('content') or '').replace(chr(10), ' ').strip()}"
        for m in history[-6:] if (m.get("content") or "").strip()
    ]

    if topic == "financing":
        gather_instruction = (
            "If the customer has NOT shared their credit score yet, your reply MUST ask for it in a friendly way so the dealer can be prepared. Do NOT answer the financing question without asking for the credit score first. "
            "If they HAVE already shared their credit score (check the conversation above), acknowledge it briefly, "
            "then you MUST end your reply by asking them to schedule a visit - for example: "
            "'Would you like to come in so we can go over your financing options in person?' "
            "FORBIDDEN: do not include any URL, web link, or phone number in your reply - even if the policy text contains one - UNLESS the customer's latest message explicitly asks for a link, URL, website, or application form."
        )
    elif "warranty" in topic.lower() or "service" in topic.lower():
        gather_instruction = (
            "Briefly summarize what is offered using the policy text above (do NOT invent product names or details - only mention what is actually written). "
            "Then end your reply by asking ONE clear question: whether the customer is interested in adding warranty/service coverage to their visit "
            "(e.g. 'Is that something you'd like to look into when you come in?'). "
            "Do NOT pivot to trade-in, financing, or any other topic - keep the question focused on warranty/service interest. "
            "Do NOT send links or phone numbers unless the customer asks a specific question the policy text cannot answer."
        )
    else:  # trade-ins
        gather_instruction = (
            "If the customer has NOT shared their trade-in vehicle details yet, your reply MUST ask for them so the dealer can prepare a number. Specifically ask for: year, make, model, mileage, title status (clean/salvage/rebuilt), and overall condition. Ask for whichever pieces are still missing - if the customer has already shared some details (check the conversation above), only ask for the rest. Do NOT answer the trade-in question without asking for the missing details first. "
            "If they HAVE already provided ALL the trade-in details, acknowledge them and redirect toward scheduling a visit "
            "(e.g. 'Would you like to bring it in so we can take a look and give you a number?'). "
            "FORBIDDEN: do not include any URL, web link, or phone number in your reply - even if the policy text contains one - UNLESS the customer's latest message explicitly asks for a link, URL, website, or application form."
        )

    name_block = (
        f"Customer's first name: {customer_name}. You may address them by this name naturally."
        if customer_name else
        "You do NOT know the customer's name. Do NOT invent one, do NOT use a single letter or initial, and do NOT use any placeholder like 'Hi there' followed by a stray character. Just start the reply without a name (e.g., 'Sure - we offer...')."
    )

    prompt = f"""You are a professional automotive sales consultant responding via SMS.

The customer asked about {topic}. Here is the dealership's {topic} policy:
{policy_text}

Dealer phone (only share if truly needed): {dealer_phone or "(not listed)"}

{name_block}

Recent conversation:
{chr(10).join(convo_lines) or "(none)"}

Customer's latest message: {customer_msg}

Instructions:
- Answer naturally using the policy text above. Keep it to 2-3 sentences.
- {gather_instruction}
- Do not repeat information already covered in the conversation.
- Do not invent details not in the policy.""".strip()

    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=160,
        )
        reply = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.warning("ai_policy_reply failed: %s", e)
        return ""

    # Strip URLs and phone numbers unless the customer explicitly asked for one.
    customer_asked_for_link = bool(re.search(
        r"\b(link|url|website|web\s*site|web\s*page|webpage|application\s*form|"
        r"apply\s*online|where\s*do\s*i\s*apply|send\s*me\s*the|page|site|"
        r"phone\s*number|number\s*to\s*call|who\s*do\s*i\s*call)\b",
        (customer_msg or "").lower(),
    ))
    if not customer_asked_for_link:
        # Drop full URLs and standalone phone numbers, then tidy double spaces.
        reply = re.sub(r"https?://\S+", "", reply)
        reply = re.sub(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", "", reply)
        reply = re.sub(r"[ \t]{2,}", " ", reply).strip()
        # Clean orphaned phrases like "at ." or "this link: ."
        reply = re.sub(r"(?:at|via|through|on|here|this\s+link)\s*[:.]?\s*(?=[\.\?!]|$)", "", reply, flags=re.I).strip()
        reply = re.sub(r"\s+([\.\?!,])", r"\1", reply)

    return reply


def extract_trade_in_vehicle(history: List[Dict[str, Any]]) -> str:
    """Scan the conversation for trade-in vehicle details and return a compact
    one-line summary like '2018 Toyota Camry, 80k miles, clean title'. Returns
    empty string if the customer hasn't shared enough to identify a vehicle."""
    if not history:
        return ""
    convo_lines = [
        f"{'Customer' if m.get('role')=='user' else 'Consultant'}: {(m.get('content') or '').replace(chr(10), ' ').strip()}"
        for m in history[-12:] if (m.get("content") or "").strip()
    ]
    prompt = f"""Read the SMS conversation below and extract the customer's TRADE-IN vehicle details (the car they want to trade in, NOT the car they are looking to buy).

Conversation:
{chr(10).join(convo_lines)}

Output ONE compact line in this exact shape (omit any field the customer did not state):
<year> <make> <model>, <mileage> mi, <title status>, <condition notes>

Rules:
- Only include facts the customer EXPLICITLY stated about the car they want to trade in.
- If the customer has not mentioned a trade-in vehicle, OR has not given a year/make/model, reply with exactly: NONE
- Do NOT invent details. Do NOT include the car they are buying.
- No preamble, no quotes, no extra commentary.""".strip()
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=80,
        )
        result = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.warning("extract_trade_in_vehicle failed: %s", e)
        return ""
    if not result or result.upper().startswith("NONE"):
        return ""
    return result.splitlines()[0].strip()


def ai_cold_followup_message(history, dealer_name, customer_name="", inventory_rows=None):
    convo_lines = [
        f"{'Customer' if m.get('role')=='user' else 'Consultant'}: {(m.get('content') or '').replace(chr(10), ' ').strip()}"
        for m in history[-6:] if (m.get("content") or "").strip()
    ]
    name_instruction = (
        f"The customer's name is {customer_name}. You may address them by name naturally."
        if customer_name else
        "You do not know the customer's name. Do NOT use any placeholder like [Customer's Name] - just greet them without a name."
    )
    # Anchor the follow-up to a vehicle that was actually discussed, so the LLM
    # cannot hallucinate a different inventory item (e.g. "Ranger" -> "Range Rover Velar").
    anchor_car = ""
    if inventory_rows:
        anchor_row = _extract_car_from_last_bot_message(history, inventory_rows)
        if anchor_row:
            anchor_car = _vehicle_title(anchor_row)
    if anchor_car:
        vehicle_instruction = (
            f'If you reference a vehicle, use EXACTLY "{anchor_car}" - verbatim, no other vehicle names. '
            f"Do not introduce any other make or model."
        )
    else:
        vehicle_instruction = (
            "Do NOT name a specific vehicle. Use generic phrasing like "
            '"the vehicle you were asking about" or "any of our available vehicles".'
        )

    # If the last assistant turn asked "Do you have any specific questions about it?",
    # the customer went silent right after getting vehicle info - pivot to scheduling.
    last_assistant = next(
        (m.get("content", "") for m in reversed(history) if m.get("role") == "assistant"),
        "",
    )
    if "specific questions about it" in (last_assistant or "").lower():
        closing_instruction = (
            "The customer went silent right after you sent them vehicle details and asked if "
            "they had any specific questions. Acknowledge briefly, then ask if they would like "
            "to schedule a time to come see the vehicle in person."
        )
    else:
        closing_instruction = "End with an open question."

    prompt = f"""You are a professional automotive sales consultant following up with a customer who went silent.

Dealership: {dealer_name or "the dealership"}
{name_instruction}
{vehicle_instruction}

Recent conversation:
{chr(10).join(convo_lines) or "(No prior messages)"}

Write a single short follow-up SMS (1-2 sentences). Reference what they were asking about if possible. Be warm but professional. {closing_instruction} Do not mention they went silent.""".strip()
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.warning("ai_cold_followup_message failed: %s", e)
        return ""


# =========================
# PROMPT BUILDER
# =========================

def build_prompt(dealer, inventory_rows, history, customer_msg, dealer_phone, confirmed_appt=None, customer_name=""):
    dealer_name  = get_row_field(dealer, DEALER_NAME_ALIASES) or "the dealership"
    address      = get_row_field(dealer, DEALER_ADDRESS_ALIASES) or "(not listed)"
    hours        = get_row_field(dealer, DEALER_HOURS_ALIASES) or "(not listed)"
    financing    = get_row_field(dealer, DEALER_FINANCING_ALIASES) or "(not listed)"
    tradeins     = get_row_field(dealer, DEALER_TRADEINS_ALIASES) or "(not listed)"
    policies     = get_row_field(dealer, DEALER_POLICIES_ALIASES) or "(none)"
    dealer_phone = normalize_phone(dealer_phone)

    inv_text     = format_inventory_rows(inventory_rows)
    history_text = " ".join((m.get("content") or "") for m in history[-2:])
    appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""

    # Strip the customer's trade-in vehicle from the matching search context so
    # it can't be picked as the "car of interest" for booking/inventory display.
    trade_in_text = ""
    if isinstance(customer_name, dict):
        trade_in_text = (customer_name.get("trade_in_vehicle") or "").strip()
    cleaned_history = history_text
    cleaned_msg     = customer_msg
    if trade_in_text:
        for tok in re.findall(r"[A-Za-z0-9]+", trade_in_text):
            if len(tok) >= 3:
                pat = re.compile(rf"\b{re.escape(tok)}\b", re.I)
                cleaned_history = pat.sub("", cleaned_history)
                cleaned_msg     = pat.sub("", cleaned_msg)
    search_ctx = f"{cleaned_history} {appt_car} {cleaned_msg}".strip()

    matches = find_inventory_matches(inventory_rows, search_ctx, top_k=3, current_msg=cleaned_msg)

    anchor_row = _extract_car_from_last_bot_message(history, inventory_rows)
    anchor_title = _vehicle_title(anchor_row) if anchor_row else ""

    # If the conversation is locked onto one specific vehicle AND the customer's
    # latest message doesn't name a different make/model, scope the matching
    # details to ONLY the anchor vehicle. Prevents the LLM from drifting to a
    # different car that happens to score high on a generic keyword like
    # "automatic" or "leather" in find_inventory_matches.
    customer_named_other_vehicle = bool(anchor_row) and _body_mentions_car(cleaned_msg, inventory_rows) and not all(
        str(anchor_row.get(k, "")).strip().lower() in cleaned_msg.lower()
        for k in ("Make", "Model") if str(anchor_row.get(k, "")).strip()
    )
    if anchor_row and not customer_named_other_vehicle:
        match_details = inventory_row_details(anchor_row)
    else:
        match_details = (
            "\n\n---\n\n".join(inventory_row_details(r) for r in matches)
            if matches else "(No close vehicle match found - the vehicle the customer asked about may not be in our inventory.)"
        )

    focus_block = ""
    if anchor_title and not customer_named_other_vehicle:
        focus_block = (
            "=== FOCUS VEHICLE (CRITICAL) ===\n"
            f'The conversation has been focused on the "{anchor_title}". '
            f'The TOP MATCHING VEHICLE DETAILS section above contains ONLY this vehicle. '
            f'Your reply MUST refer to the "{anchor_title}" exactly. Do NOT mention or '
            "name any other vehicle, year, make, or model. Do NOT invent vehicles.\n\n"
        )

    convo_lines = []
    for m in history[-10:]:
        content = (m.get("content") or "").replace("\n", " ").strip()
        if content:
            convo_lines.append(f"{'Customer' if m.get('role')=='user' else 'Consultant'}: {content}")
    convo_text = "\n".join(convo_lines) or "(No prior messages)"
    current_time_str = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

    if isinstance(customer_name, dict):
        first, last, email = customer_name.get("name", ""), customer_name.get("last_name", ""), customer_name.get("email", "")
        trade_in = customer_name.get("trade_in_vehicle", "")
    else:
        first, last, email, trade_in = (customer_name or ""), "", "", ""
    known_lines = []
    if first: known_lines.append(f"- First name: {first}")
    if last:  known_lines.append(f"- Last name: {last}")
    if email: known_lines.append(f"- Email: {email}")
    if trade_in: known_lines.append(f"- Trade-in vehicle (NOT for sale, customer is trading it in): {trade_in}")
    missing = [label for val, label in
               ((first, "first name"), (last, "last name"), (email, "email address")) if not val]
    known_block = "Already collected:\n" + "\n".join(known_lines) if known_lines else "No customer details collected yet."
    missing_block = ("Still needed BEFORE confirming any appointment: " + ", ".join(missing) + ".") if missing else "All required customer details have been collected."
    trade_in_warning = (
        f"\nIMPORTANT: The customer's trade-in vehicle is \"{trade_in}\". This is the car they want to TRADE IN - it is NOT a vehicle from our inventory and is NOT what they want to buy. "
        f"NEVER use the trade-in vehicle as the car_desc in any META_JSON appointment confirmation. "
        f"NEVER offer to schedule a viewing of the trade-in vehicle. "
        f"The car they want to BUY is whatever inventory vehicle the conversation has been focused on (look at the consultant's most recent vehicle reference, not the customer's trade-in mention).\n"
        if trade_in else ""
    )
    name_section = (
        "\n=== CUSTOMER PROFILE ===\n"
        f"{known_block}\n{missing_block}\n"
        "Use the first name naturally in conversation when known. "
        "Personal info (first/last name, email) is ONLY collected during the APPOINTMENT FLOW (STEP 1's combined ask) - never earlier. "
        "Do NOT ask for it just because the customer expressed interest, said yes to a service question, or asked a general question."
        f"{trade_in_warning}"
    )

    return f"""You are a professional sales consultant representing {dealer_name}, communicating via SMS on behalf of the dealership.

=== CURRENT DATE & TIME ===
Today is {current_time_str}. Use this to calculate exact dates when a customer says things like "tomorrow" or "Friday afternoon".

=== COMMUNICATION STYLE ===
- Professional, polished, and knowledgeable.
- Keep messages CONCISE - 1 to 3 sentences per reply. This is SMS.
- Ask only ONE question per message.
- No bullet points or numbered lists.
- Avoid slang, filler words, and overly casual phrasing.
- Preferred phrasing: "Certainly", "Of course", "I'd be happy to assist", "Thank you for your interest", "We look forward to your visit."
{name_section}
=== WHAT YOU KNOW (USE ONLY THIS - DO NOT GUESS) ===
Facts come ONLY from: Dealer Info, Inventory, and Top Matching Vehicle Details below.
If a customer asks about a vehicle that is NOT in the inventory list: Clearly tell them we don't currently have that vehicle in our inventory (e.g. "We don't currently have a 2020 Toyota Camry in our inventory"). You may ask if they'd like to hear about something similar. Do NOT say you lack information - just say it's not in our inventory.
If a customer asks something else not covered by the data below: "I don't have that information readily available. Please feel free to contact us at {dealer_phone if dealer_phone else '(dealer phone not listed)'} and one of our representatives will be glad to assist you."

=== STRICT FORBIDDEN BEHAVIORS ===
- NEVER invent a phone number, address, or any fact not in the data.
- NEVER ask about monthly payment amounts.
- For service/detailing pricing, direct them to call: "For pricing on that, I'd recommend giving us a call at {dealer_phone} - they'll be able to give you an accurate quote."
- Share VIN only if it appears in TOP MATCHING VEHICLE DETAILS below.
- NEVER offer to email details or promise anything outside this conversation.
- NEVER guess vehicle condition, history, or issues.
- NEVER use bullet points.

=== BUSINESS OBJECTIVE ===
Help the customer find the right vehicle and schedule an in-person visit. The goal is a confirmed appointment.

=== NEEDS DISCOVERY ===
When the customer brings up a topic the dealer can act on - extended warranties, service contracts, GAP coverage, financing, trade-ins, detailing/tinting/ceramic coating, or other add-on services - do not just answer the surface question. Briefly answer, then ask in the same message whether they're interested so the dealer can prepare ahead of the visit.
- Examples:
  - Customer: "Do you offer warranties?" -> "Yes, we offer extended warranty coverage. Is that something you'd like to look into when you visit?"
  - Customer: "Can I get the windows tinted?" -> "Yes, we can take care of tinting. Would you like that added to your visit so we can have a quote ready?"
- Only ask once per topic - if the customer has already said yes or no, don't keep re-asking.
- This applies in addition to any other instructions; never ask more than ONE question in a single SMS reply.

=== APPOINTMENT FLOW ===
The booking flow is STREAMLINED to minimize back-and-forth. Only collect personal info (name/email) when the customer actually wants to book - never just because they expressed interest or said "yes" to a service question.

STEP 1 - Combined ask (time + missing profile, in ONE reply)
- When the customer wants to schedule/book a visit, in a SINGLE reply ask for the specific clock time AND any missing profile fields together.
- Use the CURRENT DATE & TIME above to determine what day it is. Required: a SPECIFIC CLOCK TIME (e.g. "9am", "2:30pm"). A date alone is NOT enough.
- Look at CUSTOMER PROFILE above. Identify what is missing (first name, last name, email). Ask only for what is missing.
- Phrasing examples (keep your reply ≤155 chars when possible):
  - All profile fields missing: "Sure - what time works for you, and could I get your first name, last name, and email to lock it in?"
  - Only email missing: "Sure - what time works, and could I get your email to lock it in?"
  - Profile already complete: "Sure - what time works for you?"
- If the customer answers with only some of what you asked (e.g. gives a time but no email, or a name but no time), follow up ONCE with the missing piece(s), then stay in this step.
- This applies even if hours are not listed; only reject a time if it clearly falls outside listed hours for that specific day.

STEP 2 - Confirm
- Once you have BOTH a valid clock time AND a complete profile (first name, last name, email), confirm in a single reply - do NOT ask "Is that correct?":
  "You're all set, [First Name]! Your appointment is confirmed for 3 PM today to view the 2018 Honda Accord. We look forward to seeing you!"
- At the END of that confirmation reply only (hidden from customer), add exactly:
   META_JSON: {{"confirmed": true, "visit_time": "<human readable time>", "visit_time_iso": "<YYYY-MM-DDTHH:MM:SS>", "car_desc": "<year make model>", "customer_name": "<first name>", "customer_last_name": "<last name>", "customer_email": "<email>"}}

RESCHEDULES (very important)
- A reschedule is when the customer asks to change the time of an EXISTING confirmed appointment (e.g. "can I move it to 10am instead", "reschedule for 3pm tomorrow", "an hour later").
- For a reschedule, SKIP STEP 1 entirely (the profile is already on file). Go DIRECTLY to STEP 2 with the new time.
- The reschedule confirmation reply MUST include the META_JSON marker exactly like a brand-new booking - without it, the dealer is not notified and the booking is not recorded. This is non-negotiable.
- Example reschedule reply:
  "Certainly, Evan! Your appointment is now rescheduled for 10 AM today to view the 2023 Honda Accord Hybrid. We look forward to seeing you then!
   META_JSON: {{"confirmed": true, "visit_time": "10am today", "visit_time_iso": "2026-04-25T10:00:00", "car_desc": "2023 Honda Accord Hybrid", "customer_name": "Evan", "customer_last_name": "Lee", "customer_email": "evanssc49@icloud.com"}}"

OTHER RULES
- Do NOT include META_JSON in any other message.
- If you learn any profile field outside of a confirmation, add at the very end of your reply, on its own line, exactly the markers for what you learned this turn:
   META_NAME: <first name>
   META_LAST_NAME: <last name>
   META_EMAIL: <email>

=== DEALER INFO ===
Name: {dealer_name}
Address: {address}
Hours: {hours}
Phone (use exactly, never invent): {dealer_phone if dealer_phone else "(not listed)"}
Financing: {financing}
Trade-ins: {tradeins}
Notes/Policies: {policies}

=== INVENTORY (SUMMARY) ===
Every vehicle listed below is currently available for sale.
{inv_text}

=== TOP MATCHING VEHICLE DETAILS ===
(Use ONLY these facts - do NOT guess anything not shown here)
Note: measurements in inches (e.g. 144\", 148\") refer to wheelbase. AWD/RWD/FWD/4WD indicate drivetrain.
{match_details}

=== CONFIRMED APPOINTMENT ===
{f"This customer has a confirmed appointment at {confirmed_appt['visit_time']} to see the {confirmed_appt['car_desc']}. Do NOT push for a visit - they are booked. Answer their questions naturally." if confirmed_appt else "No appointment confirmed yet."}

=== CONVERSATION SO FAR ===
{convo_text}

=== CUSTOMER'S LATEST MESSAGE ===
{customer_msg}

{focus_block}Write ONE SMS reply now.""".strip()


# =========================
# META PARSING
# =========================

def extract_meta(reply_text: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    meta = None
    marker = re.search(r"META_JSON:\s*", reply_text, re.I)
    if marker:
        json_start = marker.end()
        depth, entered, json_end = 0, False, json_start
        for i, ch in enumerate(reply_text[json_start:], start=json_start):
            if ch == "{":
                depth += 1
                entered = True
            elif ch == "}":
                depth -= 1
            if entered and depth == 0:
                json_end = i + 1
                break
        if entered:
            try:
                meta = json.loads(reply_text[json_start:json_end])
            except Exception:
                meta = None
            reply_text = (reply_text[:marker.start()] + reply_text[json_end:]).strip()

    def _pull_marker(text: str, marker: str) -> Tuple[str, str]:
        m = re.search(rf"{marker}:\s*(.+?)(?:\n|$)", text, re.I)
        if not m:
            return text, ""
        return (text[:m.start()] + text[m.end():]).strip(), m.group(1).strip()

    reply_text, extracted_name = _pull_marker(reply_text, "META_NAME")
    reply_text, extracted_last = _pull_marker(reply_text, "META_LAST_NAME")
    reply_text, extracted_email = _pull_marker(reply_text, "META_EMAIL")

    if meta:
        if meta.get("customer_name") and not extracted_name:
            extracted_name = str(meta["customer_name"]).strip()
        if meta.get("customer_last_name") and not extracted_last:
            extracted_last = str(meta["customer_last_name"]).strip()
        if meta.get("customer_email") and not extracted_email:
            extracted_email = str(meta["customer_email"]).strip()

    if extracted_name or extracted_last or extracted_email:
        if meta is None:
            meta = {}
        if extracted_name:  meta["_extracted_name"] = extracted_name
        if extracted_last:  meta["_extracted_last_name"] = extracted_last
        if extracted_email: meta["_extracted_email"] = extracted_email

    return reply_text.strip(), meta


# =========================
# SCHEDULER JOBS
# =========================

def send_appointment_reminders() -> None:
    due = get_upcoming_unreminded_appointments()
    app.logger.info("Reminder sweep: %d appointment(s) due.", len(due))
    for appt in due:
        customer_phone = appt["customer_phone"]
        twilio_number  = appt["twilio_number"]
        visit_time     = appt["visit_time"]
        car_desc       = appt["car_desc"]
        appointment_id = appt["id"]

        if normalize_phone(customer_phone) == normalize_phone(twilio_number):
            mark_reminder_sent(appointment_id)
            continue

        reminder_body = (
            f"This is a friendly reminder of your upcoming appointment at {visit_time} "
            f"to view the {car_desc}. Please reply Yes to confirm or No to cancel."
        )
        ok, err = send_sms_to_customer(customer_phone=customer_phone, from_number=twilio_number, body=reminder_body)
        if ok:
            mark_reminder_sent(appointment_id)
            save_message(customer_phone, twilio_number, "assistant", reminder_body)
            set_pending_reconfirmation(customer_phone, twilio_number, appt["dealer_notify_phone"],
                                       visit_time, car_desc, appointment_id)
            app.logger.info("Sent reminder to %s for appt #%d", customer_phone, appointment_id)
        else:
            app.logger.warning("Reminder failed for appt #%d: %s", appointment_id, err)


def send_cold_followups() -> None:
    cold = get_cold_conversations()
    app.logger.info("Cold follow-up sweep: %d conversation(s) eligible.", len(cold))
    if not cold:
        return

    try:
        dealers = read_dealers()
    except Exception as e:
        app.logger.error("Cold follow-up: sheet read failed: %s", e)
        dealers = []

    for convo in cold:
        customer_phone = convo["customer_phone"]
        twilio_number  = convo["twilio_number"]

        if get_latest_appointment(customer_phone, twilio_number):
            mark_cold_followup_sent(customer_phone, twilio_number)
            continue
        if get_pending(customer_phone, twilio_number):
            continue
        last_msg = get_last_customer_message(customer_phone, twilio_number)
        if last_msg and DISINTEREST_RE.search(last_msg):
            mark_cold_followup_sent(customer_phone, twilio_number)
            continue
        if normalize_phone(customer_phone) == normalize_phone(twilio_number):
            mark_cold_followup_sent(customer_phone, twilio_number)
            continue

        dealer        = select_dealer_for_twilio_number(dealers, twilio_number) if dealers else {}
        dealer_name   = get_row_field(dealer, DEALER_NAME_ALIASES) if dealer else ""
        customer_name = get_customer_name(customer_phone, twilio_number)
        history       = get_recent_messages(customer_phone, twilio_number, limit=10)
        try:
            inventory_rows = get_inventory_for_twilio(twilio_number)
        except Exception:
            inventory_rows = []
        followup_body = ai_cold_followup_message(history, dealer_name, customer_name, inventory_rows) or (
            "Just wanted to follow up - are you still interested in stopping by"
            + (f" {dealer_name}" if dealer_name else "")
            + "? We are happy to help with any questions."
        )

        ok, err = send_sms_to_customer(customer_phone=customer_phone, from_number=twilio_number, body=followup_body)
        if ok:
            mark_cold_followup_sent(customer_phone, twilio_number)
            save_message(customer_phone, twilio_number, "assistant", followup_body)
            app.logger.info("Sent cold follow-up to %s via %s", customer_phone, twilio_number)
        else:
            app.logger.warning("Cold follow-up failed for %s: %s", customer_phone, err)


def start_scheduler() -> None:
    scheduler = BackgroundScheduler()
    scheduler.add_job(send_appointment_reminders, "interval", minutes=5,  id="reminders",         replace_existing=True)
    scheduler.add_job(send_cold_followups,         "interval", minutes=10, id="cold_followups",    replace_existing=True)
    scheduler.add_job(refresh_all_inventory,       "interval", minutes=30, id="inventory_refresh", replace_existing=True)
    scheduler.start()
    app.logger.info("Scheduler started: reminders 5 min | cold follow-ups 10 min | inventory 30 min.")
    send_appointment_reminders()


# =========================
# TWILIO WEBHOOK
# =========================

_SPLIT_SOFT_THRESHOLD = 1500  # below this, keep as one bubble


def _split_for_sms(text: str) -> List[str]:
    """Split a long reply into multiple SMS bubbles at blank-line boundaries.

    Each bubble stays under _SPLIT_SOFT_THRESHOLD chars; splits happen between
    paragraphs (blank lines) so section headers stay glued to their content.
    Short replies are returned as a single bubble.
    """
    if not text:
        return [text]
    text = text.strip()
    if len(text) <= _SPLIT_SOFT_THRESHOLD:
        return [text]

    paragraphs = text.split("\n\n")
    bubbles: List[str] = []
    current = ""
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        candidate = f"{current}\n\n{para}" if current else para
        if len(candidate) <= _SPLIT_SOFT_THRESHOLD:
            current = candidate
            continue
        if current:
            bubbles.append(current)
        # If a single paragraph is itself over the threshold, ship it alone -
        # Twilio will concatenate as multi-segment SMS.
        current = para
    if current:
        bubbles.append(current)
    return bubbles or [text]


def _reply_twiml(reply_body: str, customer_phone: str, twilio_number: str, *, send_primer=False) -> str:
    """send_primer: True / "full" -> capability primer; "terms" -> terms-only
    primer (used when the menu already explains capabilities); False -> none.

    Also captures the reply on flask.g so the /chat (web) endpoint can read
    the reply that the SMS-style routing produced. SMS path returns TwiML;
    chat path ignores the return value and reads g.captured_reply.
    """
    # Capture for the web chat endpoint. Ignored by /sms.
    try:
        g.captured_reply = reply_body
        if send_primer == "terms":
            g.captured_primer = TERMS_ONLY_PRIMER
        elif send_primer:
            g.captured_primer = CAPABILITY_PRIMER
        else:
            g.captured_primer = None
    except RuntimeError:
        # Outside a Flask request context (e.g. scheduler jobs) - skip capture.
        pass

    twiml = MessagingResponse()
    for chunk in _split_for_sms(reply_body):
        twiml.message(chunk)
    if send_primer == "terms":
        twiml.message(TERMS_ONLY_PRIMER)
        mark_primer_sent(customer_phone, twilio_number)
    elif send_primer:  # True or "full"
        twiml.message(CAPABILITY_PRIMER)
        mark_primer_sent(customer_phone, twilio_number)
    return str(twiml)


@app.route("/sms", methods=["POST"])
def sms_webhook():
    body        = (request.form.get("Body") or "").strip()
    from_number = normalize_phone(request.form.get("From") or "")
    to_number   = normalize_phone(request.form.get("To")   or "")
    if not from_number or not to_number:
        twiml = MessagingResponse()
        twiml.message("Sorry - missing phone routing info.")
        return str(twiml)
    return _process_message(from_number, to_number, body)


def _process_message(from_number: str, to_number: str, body: str):
    """Shared routing - both /sms and /chat funnel through here. Returns TwiML
    string (used by /sms). The /chat endpoint reads g.captured_reply instead."""
    app.logger.info("Inbound from %s: %r", from_number, body)
    new_customer = not has_primer_been_sent(from_number, to_number)
    clear_cold_followup(from_number, to_number)
    save_message(from_number, to_number, "user", body)

    customer_profile = get_customer_profile(from_number, to_number)
    customer_name = customer_profile["name"]

    try:
        dealers    = read_dealers()
        dealer_row = select_dealer_for_twilio_number(dealers, to_number)
    except Exception as e:
        app.logger.error("Sheet read failed: %s", e)
        twiml = MessagingResponse()
        twiml.message(
            "We are experiencing a temporary system issue. "
            "Please try again shortly or contact us directly for assistance."
        )
        return str(twiml)

    dealer_phone   = normalize_phone(get_row_field(dealer_row, DEALER_NOTIFY_PHONE_ALIASES))
    inventory_rows = get_inventory_for_twilio(to_number)

    # ── PRIORITY 1: Pending reconfirmation (1-hr reminder response) ──────
    reconf = get_pending_reconfirmation(from_number, to_number)
    if reconf:
        visit_time, car_desc, appointment_id = reconf["visit_time"], reconf["car_desc"], reconf["appointment_id"]
        reconf_notify_phone = normalize_phone(reconf.get("dealer_notify_phone", "")) or dealer_phone

        if NO_RE.search(body):
            clear_pending_reconfirmation(from_number, to_number)
            cancel_appointment(from_number, to_number)
            notify_all_staff(dealer_row, to_number, _dealer_cancellation_body(
                customer_phone=from_number, customer_name=customer_name,
                customer_last_name=customer_profile["last_name"],
                customer_email=customer_profile["email"],
                dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
            ))
            reply = "Understood - we have removed that appointment. When would you prefer to reschedule your visit?"
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        if YES_RE.search(body):
            clear_pending_reconfirmation(from_number, to_number)
            mark_reconfirmed(appointment_id)
            notify_all_staff(dealer_row, to_number, _dealer_reconfirm_body(
                customer_phone=from_number, customer_name=customer_name,
                customer_last_name=customer_profile["last_name"],
                customer_email=customer_profile["email"],
                dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
            ))
            reply = f"Thank you for confirming. We look forward to seeing you at {visit_time}."
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        clear_pending_reconfirmation(from_number, to_number)
        # Fall through to AI

    # ── PRIORITY 2: Pending appointment confirmation ──────────────────────
    pending = get_pending(from_number, to_number)

    # Fallback: if AI forgot to emit META_JSON, recover from recent bot messages
    if not pending and YES_RE.search(body) and not NO_RE.search(body):
        recent = get_recent_messages(from_number, to_number, limit=6)
        for m in reversed(recent):
            if m.get("role") != "assistant":
                continue
            content = m.get("content", "")
            confirm_m = re.search(
                r"(?:To confirm|You'?re all set|confirmed for|appointment (?:is )?confirmed)[^.]*?at\s+(.+?)(?:\.|$)"
                r"|confirmed for\s+(.+?)(?:\s+to\s+view|\s+to\s+see|\.|$)",
                content, re.I
            )
            # Only recover if this is a booking confirmation, not a cancellation prompt
            if confirm_m and "cancel" not in content.lower():
                recovered_time = (confirm_m.group(1) or confirm_m.group(2) or "").strip()
                parsed_time, parsed_iso = parse_visit_time_from_text(recovered_time)
                if parsed_time:
                    car_m = re.search(r"\b(20\d{2}\s+\w[\w\s]{3,40}?)(?:\s+and|\.|,|$)", content, re.I)
                    recovered_car = car_m.group(1).strip() if car_m else "a vehicle"
                    set_pending(from_number, to_number, dealer_phone, parsed_time, parsed_iso, recovered_car)
                    pending = get_pending(from_number, to_number)
                    app.logger.info("Recovered pending from bot message: %s / %s", parsed_time, recovered_car)
            break  # always stop at the first assistant message

    if pending:
        # Opportunistically capture an email address from the body (the customer may
        # be replying directly to "could I get your email" - without a yes/no/time).
        email_scan = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", body)
        if email_scan and not customer_profile["email"] and is_valid_email(email_scan.group(0)):
            save_customer_profile(from_number, to_number, email=email_scan.group(0))
            customer_profile = get_customer_profile(from_number, to_number)
            customer_name = customer_profile["name"]

        if NO_RE.search(body):
            clear_pending(from_number, to_number)
            reply = "Of course - what time would work best for you?"
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        if YES_RE.search(body):
            pending_notify_phone = normalize_phone(pending.get("dealer_notify_phone", "")) or dealer_phone
            visit_time, visit_time_iso, car_desc = pending["visit_time"], pending.get("visit_time_iso", ""), pending["car_desc"]

            missing = missing_profile_field(customer_profile)
            if missing:
                # Hold the booking; ask for the missing field before logging or notifying.
                reply = (f"Almost set! Before I lock in {visit_time} for the {car_desc}, "
                         f"could I please get your {missing}?")
                save_message(from_number, to_number, "assistant", reply)
                return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

            appt_id, is_reschedule = log_appointment(from_number, to_number, pending_notify_phone, visit_time, visit_time_iso, car_desc)
            clear_pending(from_number, to_number)

            additional_info = extract_customer_insights(get_recent_messages(from_number, to_number, limit=20))
            alert_body = (
                _dealer_reschedule_body(customer_phone=from_number, customer_name=customer_name,
                                        customer_last_name=customer_profile["last_name"],
                                        customer_email=customer_profile["email"],
                                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                        additional_info=additional_info)
                if is_reschedule else
                _dealer_alert_body(customer_phone=from_number, customer_name=customer_name,
                                   customer_last_name=customer_profile["last_name"],
                                   customer_email=customer_profile["email"],
                                   dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                   additional_info=additional_info)
            )
            notify_all_staff(dealer_row, to_number, alert_body)

            reply = f"Your appointment is confirmed for {visit_time}. We look forward to seeing you."
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        corrected_time, corrected_iso = parse_visit_time_from_text(body)
        if corrected_time:
            set_pending(from_number, to_number, dealer_phone, corrected_time, corrected_iso, pending["car_desc"])
            reply = (f"Got it - updated to {corrected_time} for the {pending['car_desc']}. "
                     "Reply Yes to lock it in or No to pick a different time.")
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        # If the message looks like a name (short, no special keywords), treat it as
        # confirmation - save the name and book the appointment immediately
        is_likely_name = (
            len(body.split()) <= 4
            and not re.search(r"\d", body)
            and "@" not in body
            and not DISINTEREST_RE.search(body)
            and not CANCEL_APPT_RE.search(body)
        )
        if is_likely_name:
            tokens = [t for t in body.strip().split() if t]
            new_first = tokens[0].title() if tokens else ""
            new_last = tokens[1].title() if len(tokens) >= 2 else None
            valid_first = is_valid_name(new_first) if new_first else False
            valid_last  = is_valid_name(new_last) if new_last else None
            if valid_first or valid_last:
                save_customer_profile(
                    from_number, to_number,
                    name=new_first if valid_first else None,
                    last_name=new_last if valid_last else None,
                )
                customer_profile = get_customer_profile(from_number, to_number)
                customer_name = customer_profile["name"]

            visit_time, visit_time_iso, car_desc = pending["visit_time"], pending.get("visit_time_iso", ""), pending["car_desc"]
            missing = missing_profile_field(customer_profile)
            if missing:
                reply = (f"Thanks, {customer_name or 'and welcome'}! Before I lock in {visit_time} for the {car_desc}, "
                         f"could I please get your {missing}?")
                save_message(from_number, to_number, "assistant", reply)
                return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

            pending_notify_phone = normalize_phone(pending.get("dealer_notify_phone", "")) or dealer_phone
            appt_id, is_reschedule = log_appointment(from_number, to_number, pending_notify_phone, visit_time, visit_time_iso, car_desc)
            clear_pending(from_number, to_number)
            additional_info = extract_customer_insights(get_recent_messages(from_number, to_number, limit=20))
            alert_body = (
                _dealer_reschedule_body(customer_phone=from_number, customer_name=customer_name,
                                        customer_last_name=customer_profile["last_name"],
                                        customer_email=customer_profile["email"],
                                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                        additional_info=additional_info)
                if is_reschedule else
                _dealer_alert_body(customer_phone=from_number, customer_name=customer_name,
                                   customer_last_name=customer_profile["last_name"],
                                   customer_email=customer_profile["email"],
                                   dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                   additional_info=additional_info)
            )
            notify_all_staff(dealer_row, to_number, alert_body)
            reply = f"Perfect, {customer_name}! You're all set for {visit_time} to see the {car_desc}. We look forward to seeing you!"
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        next_missing = missing_profile_field(customer_profile)
        if next_missing:
            reply = (f"Thanks! Could I also get your {next_missing} so I can lock in "
                     f"{pending['visit_time']} for the {pending['car_desc']}?")
        else:
            reply = (f"To confirm - shall I keep your appointment at {pending['visit_time']} "
                     f"for the {pending['car_desc']}? Reply Yes or No.")
        save_message(from_number, to_number, "assistant", reply)
        return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 2.5: Pending cancellation ───────────────────────────────
    pending_cancel = get_pending_cancellation(from_number, to_number)
    if pending_cancel:
        visit_time, car_desc = pending_cancel["visit_time"], pending_cancel["car_desc"]
        cancel_notify_phone = normalize_phone(pending_cancel.get("dealer_notify_phone", "")) or dealer_phone

        if YES_RE.search(body):
            cancel_appointment(from_number, to_number)
            clear_pending_cancellation(from_number, to_number)
            notify_all_staff(dealer_row, to_number, _dealer_cancellation_body(
                customer_phone=from_number, customer_name=customer_name,
                customer_last_name=customer_profile["last_name"],
                customer_email=customer_profile["email"],
                dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
            ))
            reply = "Your appointment has been cancelled. If you would like to reschedule at any time, feel free to reach out."
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        if NO_RE.search(body):
            clear_pending_cancellation(from_number, to_number)
            reply = f"No problem - your appointment is still confirmed for {visit_time}. We look forward to seeing you."
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        reply = f"Just to confirm - would you like to cancel your appointment at {visit_time} for the {car_desc}? Please reply Yes or No."
        save_message(from_number, to_number, "assistant", reply)
        return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 3: Cancellation request ─────────────────────────────────
    existing_appt = get_latest_appointment(from_number, to_number)
    if existing_appt and CANCEL_APPT_RE.search(body):
        set_pending_cancellation(from_number, to_number, dealer_phone, existing_appt["visit_time"], existing_appt["car_desc"])
        reply = (f"Just to confirm - would you like to cancel your appointment at {existing_appt['visit_time']} "
                 f"for the {existing_appt['car_desc']}? Please reply Yes or No.")
        save_message(from_number, to_number, "assistant", reply)
        return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 3.5: Relative reschedule ("an hour later", "30 min earlier") ──
    if (existing_appt and existing_appt.get("visit_time_iso")
            and RESCHEDULE_INTENT_RE.search(body)):
        offset = parse_relative_offset(body)
        if offset is not None:
            try:
                current_dt = datetime.fromisoformat(existing_appt["visit_time_iso"])
                new_dt = current_dt + offset
                new_iso = new_dt.isoformat(timespec="seconds")
                new_display = format_visit_time_display(new_dt)
                set_pending(from_number, to_number, dealer_phone, new_display, new_iso, existing_appt["car_desc"])
                reply = (f"Got it - shall I move your appointment to {new_display} "
                         f"for the {existing_appt['car_desc']}? Reply Yes or No.")
                save_message(from_number, to_number, "assistant", reply)
                return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)
            except (ValueError, TypeError):
                pass

    # ── PRIORITY 4: Deterministic shortcuts ──────────────────────────────
    confirmed_appt = get_latest_appointment(from_number, to_number)

    # ── PRIORITY 4.0: Greeting / menu - bare hellos or explicit help asks ──
    _is_greeting = bool(re.match(
        r"^\s*(hi|hey|hello|yo|sup|howdy|hola|"
        r"good\s+(morning|afternoon|evening)|"
        r"what'?s\s+up|whatsup|whats\s+up)[\s!.,?]*$",
        body, re.I,
    ))
    _asks_for_menu = bool(re.search(
        r"\b(menu|menue|meneu|what\s+can\s+you\s+do|what\s+do\s+you\s+do|"
        r"what\s+are\s+(my|the|all|your)\s+options|"
        r"show\s+me\s+(my|the|all|your)\s+options)\b",
        body.lower(),
    ))
    if _is_greeting or _asks_for_menu:
        name = customer_profile.get("name", "") if isinstance(customer_profile, dict) else ""
        greeting = f"Hi {name}! " if name else "Hi there! "
        reply_text = (
            greeting + "What are you looking for? Reply with a number:\n"
            "1) Browse inventory\n"
            "2) Financing\n"
            "3) Trade-in\n"
            "4) Warranties\n"
            "5) Schedule a visit\n"
            "6) Hours / location"
        )
        save_message(from_number, to_number, "assistant", reply_text)
        # First-time customers triggering the menu get the short terms-only
        # primer. The menu itself already covers what the bot can do, so the
        # full FYI primer would be redundant. Returning customers get nothing.
        return _reply_twiml(
            reply_text, from_number, to_number,
            send_primer="terms" if new_customer else False,
        )

    # Numbered menu reply - translate to a phrase the existing handlers pick up.
    _menu_digit = re.match(r"^\s*([1-6])\s*[).!]?\s*$", body)
    if _menu_digit:
        _history_check = get_recent_messages(from_number, to_number, limit=4)
        _last_asst = next(
            (m.get("content", "") for m in reversed(_history_check) if m.get("role") == "assistant"),
            "",
        )
        if "Reply with a number" in _last_asst:
            body = {
                "1": "show me your inventory",
                "2": "do you offer financing",
                "3": "do you accept trade-ins",
                "4": "do you offer warranties",
                "5": "I'd like to schedule a visit",
                "6": "what are your hours and where are you located",
            }[_menu_digit.group(1)]
            # Menu picks aren't "first actual question" - suppress the FYI primer
            # so it fires on the customer's next freeform question instead.
            new_customer = False

    if _is_vin_question(body):
        history      = get_recent_messages(from_number, to_number, limit=14)
        history_text = " ".join((m.get("content") or "") for m in history[-2:])
        appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""
        if _body_mentions_car(body, inventory_rows):
            search_ctx = f"{history_text} {appt_car} {body}".strip()
            matches    = find_inventory_matches(inventory_rows, search_ctx, top_k=1, current_msg=body)
            match      = matches[0] if matches else _best_history_vehicle_match(inventory_rows, search_ctx)
        else:
            match = _extract_car_from_last_bot_message(history, inventory_rows) or _best_history_vehicle_match(inventory_rows, history_text)
        if match:
            vin = get_row_field(match, VIN_ALIASES).strip()
            reply_text = f"The VIN for the {_vehicle_title(match)} is {vin}." if vin else (
                f"The VIN for that vehicle is not currently on file. Please contact us at {dealer_phone} for that information."
                if dealer_phone else "The VIN for that vehicle is not currently on file. Please contact us directly."
            )
        else:
            reply_text = build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_stock_number_question(body):
        history      = get_recent_messages(from_number, to_number, limit=14)
        history_text = " ".join((m.get("content") or "") for m in history[-2:])
        if _body_mentions_car(body, inventory_rows):
            matches = find_inventory_matches(inventory_rows, f"{history_text} {body}".strip(), top_k=1, current_msg=body)
        else:
            anchor  = _extract_car_from_last_bot_message(history, inventory_rows) or _best_history_vehicle_match(inventory_rows, history_text)
            matches = [anchor] if anchor else []
        is_avail     = bool(re.search(r"\b(available|still have|in stock|still got|do you have|still available|is it available|is that available)\b", body, re.I))
        if matches:
            reply_text = (f"Yes, the {_vehicle_title(matches[0])} is currently available." if is_avail
                          else (f"The stock number is {get_row_field(matches[0], STOCK_ALIASES).strip()}." or build_unknown_answer(dealer_phone)))
        else:
            reply_text = build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_dealer_phone_question(body):
        reply_text = (f"You may reach us at {dealer_phone}." if dealer_phone
                      else "Our direct contact number is not currently on file. We will have a representative reach out shortly.")
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_financing_question(body):
        financing = get_row_field(dealer_row, DEALER_FINANCING_ALIASES)
        if financing:
            history = get_recent_messages(from_number, to_number, limit=6)
            reply_text = ai_policy_reply(body, "financing", financing, dealer_phone, history, customer_name=customer_name) or f"Regarding financing: {financing}."
        else:
            reply_text = build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if re.search(r"\btrade[- ]?ins?\b", body, re.I):
        tradeins = get_row_field(dealer_row, DEALER_TRADEINS_ALIASES)
        history = get_recent_messages(from_number, to_number, limit=12)
        # Try to capture the trade-in vehicle if the customer has shared details.
        candidate_trade_in = extract_trade_in_vehicle(history + [{"role": "user", "content": body}])
        has_trade_in_on_file = bool((customer_profile.get("trade_in_vehicle") or "").strip())

        if tradeins and not has_trade_in_on_file and not candidate_trade_in:
            # First trade-in inquiry with no details yet - answer deterministically
            # so menu option 3 and direct text both reliably collect car data.
            # The LLM-based path was sometimes giving a policy-only answer here.
            policy_clean = tradeins.rstrip(".") + "."
            reply_text = (
                f"{policy_clean} To prepare an accurate offer, could you share the year, "
                f"make, model, mileage, title status (clean/salvage/rebuilt), and overall "
                f"condition of your vehicle?"
            )
        elif tradeins:
            reply_text = ai_policy_reply(body, "trade-ins", tradeins, dealer_phone, history[-6:], customer_name=customer_name) or f"Regarding trade-ins: {tradeins}."
        else:
            reply_text = build_unknown_answer(dealer_phone)

        if candidate_trade_in and candidate_trade_in != (customer_profile.get("trade_in_vehicle") or ""):
            save_customer_profile(from_number, to_number, trade_in_vehicle=candidate_trade_in)
            app.logger.info("Recorded trade-in vehicle for %s: %s", from_number, candidate_trade_in)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_dealer_info_question(body):
        reply_text = _dealer_info_response(dealer_row, dealer_phone, body)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_pricing_policy_question(body):
        policies = get_row_field(dealer_row, DEALER_POLICIES_ALIASES)
        reply_text = (f"Our dealership policy: {policies}." if policies else build_unknown_answer(dealer_phone))
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_issue_question(body):
        history      = get_recent_messages(from_number, to_number, limit=14)
        history_text = " ".join((m.get("content") or "") for m in history[-6:])
        appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""
        if _body_mentions_car(body, inventory_rows):
            search_ctx = f"{history_text} {appt_car} {body}".strip()
            matches    = find_inventory_matches(inventory_rows, search_ctx, top_k=1, current_msg=body)
            match      = matches[0] if matches else _best_history_vehicle_match(inventory_rows, history_text)
        else:
            match = _extract_car_from_last_bot_message(history, inventory_rows) or _best_history_vehicle_match(inventory_rows, history_text)
        reply_text   = (ai_vehicle_detail_reply(body, inventory_row_details(match), dealer_phone, history) or _issue_response_for_match(match)) if match else build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_title_status_question(body):
        history      = get_recent_messages(from_number, to_number, limit=14)
        history_text = " ".join((m.get("content") or "") for m in history[-6:])
        appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""
        if _body_mentions_car(body, inventory_rows):
            search_ctx = f"{history_text} {appt_car} {body}".strip()
            matches    = find_inventory_matches(inventory_rows, search_ctx, top_k=1, current_msg=body)
            match      = matches[0] if matches else _best_history_vehicle_match(inventory_rows, history_text)
        else:
            match = _extract_car_from_last_bot_message(history, inventory_rows) or _best_history_vehicle_match(inventory_rows, history_text)
        reply_text = (ai_vehicle_detail_reply(body, inventory_row_details(match), dealer_phone, history) or _title_status_response_for_match(match)) if match else build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_vehicle_link_question(body):
        history      = get_recent_messages(from_number, to_number, limit=14)
        history_text = " ".join((m.get("content") or "") for m in history[-6:])
        appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""
        search_ctx   = f"{history_text} {appt_car} {body}".strip()
        if _body_mentions_car(body, inventory_rows):
            matches = find_inventory_matches(inventory_rows, search_ctx, top_k=1, current_msg=body)
            match   = matches[0] if matches else _best_history_vehicle_match(inventory_rows, history_text)
        else:
            match   = _extract_car_from_last_bot_message(history, inventory_rows) or _best_history_vehicle_match(inventory_rows, history_text)
        if match:
            url = str(match.get("DetailURL", "")).strip()
            reply_text = f"Here's the listing for the {_vehicle_title(match)}: {url}" if url else build_unknown_answer(dealer_phone)
        else:
            reply_text = build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_dealer_warranty_question(body):
        policies = get_row_field(dealer_row, DEALER_POLICIES_ALIASES)
        if policies:
            history = get_recent_messages(from_number, to_number, limit=6)
            reply_text = ai_policy_reply(body, "warranty and services", policies, dealer_phone, history, customer_name=customer_name) or f"Regarding our warranty and services: {policies}."
        else:
            reply_text = build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    _is_avail_q = bool(re.search(
        r"\b(still available|is it available|is that available|still have it|still in stock|"
        r"is it still|still got it|do you still have|is the .{1,40} available|is .{1,40} still)\b",
        body, re.I
    ))

    # If the customer's message contains a body/fuel/drivetrain CATEGORY filter
    # (e.g. "diesel trucks", "AWD SUVs", "convertibles"), they're asking for a
    # category listing - NOT for follow-up details about the anchored vehicle.
    # Skip the detail handler so the message reaches PRIORITY 4.7 which lists
    # the full inventory matching that category. EXCEPT when the message is
    # phrased as a property question ("is it diesel?", "does it have 4wd?") -
    # those are about the anchored vehicle, so the detail handler should run.
    _has_category_filter = bool(
        _extract_body_type(body) or _extract_fuel_type(body) or _extract_drivetrain(body)
    )
    _property_question_start = bool(re.match(
        r"\s*(is|does|has)\s+(it|that|the\s+\S+)\b",
        body, re.I,
    ))
    # Only treat the message as a category-list query if it ALSO has list-style
    # phrasing. Otherwise statements like "but that one is fwd" or "no it's
    # 4wd" route to the listing handler and dump a category list at the
    # customer instead of letting the LLM continue the conversation.
    _list_phrasing = bool(re.search(
        r"\b(what|which|show|list|any|got\s+any|how\s+many|i\s+want|i\s+need|"
        r"i'?m\s+looking|looking\s+for|need\s+an?|do\s+you\s+have|got\s+any|"
        r"you\s+have\s+any|are\s+there\s+any)\b",
        body, re.I,
    ))
    _has_category_filter = _has_category_filter and not _property_question_start and _list_phrasing

    if (_is_avail_q or _is_vehicle_detail_question(body)) and not _has_category_filter:
        history  = get_recent_messages(from_number, to_number, limit=14)
        appt_car = confirmed_appt["car_desc"] if confirmed_appt else ""

        if _body_mentions_car(body, inventory_rows):
            matches = find_inventory_matches(inventory_rows, f"{appt_car} {body}".strip(), top_k=1, current_msg=body)
        else:
            last_mentioned = _extract_car_from_last_bot_message(history, inventory_rows)
            matches = [last_mentioned] if last_mentioned else []

        reply_text = None
        if matches:
            match = matches[0]
            # Prefer the LLM-driven detail reply when the message has BOTH an
            # availability check AND a feature question (e.g. "if you still
            # have it, is it awd?"). Pure availability checks ("is the GLB
            # still available?") still take the fast deterministic path.
            _detail_q = _is_vehicle_detail_question(body)
            if _is_avail_q and not _detail_q:
                # Verify the match actually belongs to the make/model the customer asked about
                match_make = str(match.get("Make", "")).strip().lower()
                body_l = body.lower()
                canonical_asked = next(
                    (canonical for alias, canonical in _MAKE_ALIASES.items() if alias in body_l),
                    None
                )
                make_asked = canonical_asked or next(
                    (str(r.get("Make", "")).strip().lower() for r in inventory_rows
                     if str(r.get("Make", "")).strip().lower() in body_l),
                    None
                )
                if not make_asked or match_make == make_asked or match_make.startswith(make_asked):
                    title = _vehicle_title(match)
                    reply_text = f"Yes, the {title} is currently available. Would you like to schedule a time to come see it?"
            else:
                reply_text = ai_vehicle_detail_reply(body, inventory_row_details(match), dealer_phone, history) or inventory_row_details(match)

        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)
        # No vehicle identified or wrong make matched - fall through to AI

    # ── PRIORITY 4.65: Deterministic make-filtered listing ──────────────
    # The LLM was dropping vehicles when asked "any Toyotas?" - it would name
    # one and miss the rest. Filter and format in code so the listing is
    # provably complete. Honors price/year/body/fuel/drivetrain qualifiers
    # in the same message ("Ford trucks", "any AWD Toyotas under 20k") and
    # supports compound queries ("any Toyotas or Hondas under 15k").
    # When the current message has only a make and no other qualifier,
    # inherits price/year/feature filters from the immediately prior user
    # message ("any toyotas or hondas under 15k" -> "what about hondas"
    # carries the under-15k forward).
    _makes_asked = _extract_make_filters(body, inventory_rows)
    if _makes_asked:
        _min_p_m, _max_p_m = _extract_price_range(body)
        _year_m_match = re.search(r"\b(19|20)\d{2}\b", body)
        _year_m = _year_m_match.group(0) if _year_m_match else None
        _body_m = _extract_body_type(body)
        _fuel_m = _extract_fuel_type(body)
        _drive_m = _extract_drivetrain(body)
        # Inherit any missing filters from the prior user message
        if (_min_p_m is None and _max_p_m is None and not _year_m
                and not _body_m and not _fuel_m and not _drive_m):
            _hist_for_inherit = get_recent_messages(from_number, to_number, limit=8)
            inh = _inherit_filters_from_prior(body, _hist_for_inherit)
            _min_p_m  = inh.get("min_p", _min_p_m)
            _max_p_m  = inh.get("max_p", _max_p_m)
            _year_m   = inh.get("year",  _year_m)
            _body_m   = inh.get("body",  _body_m)
            _fuel_m   = inh.get("fuel",  _fuel_m)
            _drive_m  = inh.get("drive", _drive_m)
        reply_text = _format_make_listing(
            inventory_rows, _makes_asked, _min_p_m, _max_p_m, _year_m,
            body_type=_body_m, fuel_type=_fuel_m, drivetrain=_drive_m,
        )
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.7: Deterministic feature-filtered listing ────────────
    # "Diesel trucks", "any AWD SUVs", "convertibles", "trucks under 10k".
    # No make in the message, so 4.65 didn't fire - but the LLM was dropping
    # cars (e.g. surfaced 1 of 9 diesel vehicles). Filter inventory by
    # body/fuel/drivetrain in code so the listing is complete. Evaluated
    # BEFORE the price-only block so combined filters ("trucks under 10k")
    # correctly narrow by both axes instead of falling to price-only.
    _min_p, _max_p = _extract_price_range(body)
    _body_f = _extract_body_type(body)
    _fuel_f = _extract_fuel_type(body)
    _drive_f = _extract_drivetrain(body)
    if _body_f or _fuel_f or _drive_f:
        _year_f_match = re.search(r"\b(19|20)\d{2}\b", body)
        _year_f = _year_f_match.group(0) if _year_f_match else None
        reply_text = _format_feature_listing(
            inventory_rows,
            body_type=_body_f, fuel_type=_fuel_f, drivetrain=_drive_f,
            min_p=_min_p, max_p=_max_p, year=_year_f,
        )
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.75: Deterministic price-filtered listing ──────────────
    # The LLM was dropping cars from filtered lists ("under 10k" -> 5 of 11)
    # and occasionally including over-budget rows. Filter and format in code
    # so the listing is provably complete and accurate.
    if _min_p is not None or _max_p is not None:
        reply_text = _format_price_listing(inventory_rows, _min_p, _max_p)
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.8: Listing-continuation question ──────────────────────
    # After a make/price/year listing, customers ask "is there anymore",
    # "is that all", "what else". The LLM was extending from its own prior
    # reply (which often dropped cars) instead of re-querying inventory.
    # Re-derive the filter from history and answer deterministically.
    if _is_more_question(body):
        _hist_for_more = get_recent_messages(from_number, to_number, limit=14)
        reply_text = _handle_more_question(body, _hist_for_more, inventory_rows)
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.9: Generic inventory browse ───────────────────────────
    # "Show me your inventory", menu option 1, "what do you have", "what's
    # available". Without a make/price/year filter the LLM was inventing
    # vehicles (e.g. a 2020 Chrysler Voyager that doesn't exist). List the
    # newest top-N straight from the database instead.
    if _is_generic_listing_query(body):
        reply_text = _format_generic_listing(inventory_rows)
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 5: Full AI conversation ─────────────────────────────────
    history  = get_recent_messages(from_number, to_number, limit=14)

    _is_list_q = bool(re.search(
        r"\b(list|show|what do you have|what cars|what vehicles|what.?s available|"
        r"what are your|under \$?[\d,]+|over \$?[\d,]+|less than|more than|"
        r"between \$?[\d,]|all your|everything (under|over|you have))\b",
        body, re.I
    ))

    _is_vehicle_info_q = (
        _is_general_info_question(body)
        or bool(re.search(
            r"\b(info|information|details|specs|describe|rundown|overview|"
            r"break down|learn more|what.?s the deal|tell me)\b",
            body, re.I,
        ))
    ) and _body_mentions_car(body, inventory_rows)

    # Detect the "no more questions" follow-up: the bot just asked "Do you have
    # any specific questions about it?" and the customer answered with a closure.
    last_assistant = next(
        (m.get("content", "") for m in reversed(history) if m.get("role") == "assistant"),
        "",
    )
    _bot_just_asked_for_questions = "specific questions about it" in (last_assistant or "").lower()
    _is_no_more_questions = bool(re.search(
        r"^\s*(no|nope|nah|not really|i.?m good|im good|that.?s it|thats it|"
        r"all good|no more|nothing else|that.?s all|thats all|good for now|"
        r"no thanks|i.?m all set|im all set)\b",
        body.strip(), re.I,
    ))

    prompt   = build_prompt(dealer_row, inventory_rows, history, body, dealer_phone, confirmed_appt, customer_profile)
    if _is_list_q:
        prompt += "\n\n=== LISTING REQUEST ===\nThe customer is asking for a list of vehicles. You MAY list multiple vehicles on separate lines. Include year, make, model, and price for each. List ALL matching vehicles, not just a few."
    if _is_vehicle_info_q:
        prompt += "\n\n=== VEHICLE INFO REQUEST ===\nThe customer is asking for information about a specific vehicle. Give a brief, professional summary of the vehicle's key features (color, interior, engine, drivetrain, notable options) in 2-4 sentences. Do NOT push to schedule a visit. END the reply with exactly this sentence: \"Do you have any specific questions about it?\""
    if _bot_just_asked_for_questions and _is_no_more_questions:
        prompt += "\n\n=== READY TO SCHEDULE ===\nThe customer just confirmed they have no more questions about the vehicle. Acknowledge briefly in one short sentence, then ask if they would like to schedule a time to come see it. Keep the whole reply to 1-2 sentences."

    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600 if _is_list_q else 300,
        )
        raw_reply = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.error("OpenAI call failed: %s", e)
        raw_reply = ""

    if not raw_reply:
        raw_reply = "Thank you for reaching out. How may I assist you with your vehicle search today?"

    reply_text, meta = extract_meta(raw_reply)

    if should_force_unknown_answer(reply_text):
        reply_text = build_unknown_answer(dealer_phone)

    if meta:
        save_kwargs: Dict[str, Any] = {}
        if meta.get("_extracted_name") and is_valid_name(meta["_extracted_name"]) and not customer_profile["name"]:
            save_kwargs["name"] = meta["_extracted_name"]
        if meta.get("_extracted_last_name") and is_valid_name(meta["_extracted_last_name"]) and not customer_profile["last_name"]:
            save_kwargs["last_name"] = meta["_extracted_last_name"]
        if meta.get("_extracted_email") and is_valid_email(meta["_extracted_email"]) and not customer_profile["email"]:
            save_kwargs["email"] = meta["_extracted_email"]
        if save_kwargs:
            save_customer_profile(from_number, to_number, **save_kwargs)
            customer_profile = get_customer_profile(from_number, to_number)
            customer_name = customer_profile["name"]

    if meta and (meta.get("confirmed") or meta.get("need_confirmation")):
        visit_time     = str(meta.get("visit_time",     "")).strip()
        visit_time_iso = _validate_iso(str(meta.get("visit_time_iso", "")).strip())
        car_desc       = str(meta.get("car_desc",       "")).strip()

        if not visit_time_iso and visit_time:
            _, visit_time_iso = parse_visit_time_from_text(visit_time)

        if visit_time and not has_clock_time(visit_time):
            # AI tried to confirm with just a date (no clock time). Reject and re-ask.
            reply_text = "Of course - what specific time of day works best for your visit?"
            app.logger.info("Held auto-book: visit_time has no clock time (%r)", visit_time)
            visit_time = ""  # short-circuit the rest of this block

        if visit_time:
            missing = missing_profile_field(customer_profile)
            if missing:
                # AI tried to confirm but profile is incomplete. Hold as pending and override the reply.
                set_pending(from_number, to_number, dealer_phone, visit_time, visit_time_iso, car_desc or "a vehicle")
                reply_text = (f"I have your appointment for {visit_time} on hold. "
                              f"Could I please get your {missing} so I can lock it in?")
                app.logger.info("Held auto-book for missing profile field: %s", missing)
            elif meta.get("confirmed"):
                # Auto-book immediately - no pending confirmation needed
                appt_id, is_reschedule = log_appointment(
                    from_number, to_number, dealer_phone, visit_time, visit_time_iso, car_desc or "a vehicle"
                )
                additional_info = extract_customer_insights(get_recent_messages(from_number, to_number, limit=20))
                alert_body = (
                    _dealer_reschedule_body(
                        customer_phone=from_number, customer_name=customer_name,
                        customer_last_name=customer_profile["last_name"],
                        customer_email=customer_profile["email"],
                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc or "a vehicle",
                        additional_info=additional_info,
                    )
                    if is_reschedule else
                    _dealer_alert_body(
                        customer_phone=from_number, customer_name=customer_name,
                        customer_last_name=customer_profile["last_name"],
                        customer_email=customer_profile["email"],
                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc or "a vehicle",
                        additional_info=additional_info,
                    )
                )
                notify_all_staff(dealer_row, to_number, alert_body)
                app.logger.info("Auto-booked appt #%d", appt_id)
            else:
                # Legacy need_confirmation flow - keep for fallback
                set_pending(from_number, to_number, dealer_phone, visit_time, visit_time_iso, car_desc or "a vehicle")

    save_message(from_number, to_number, "assistant", reply_text)
    return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)


# =========================
# WEB CHAT (widget) - serves the same routing logic as /sms but as a JSON API
# backed by a browser UI. Each browser session gets a unique pseudo-phone
# (web:<session>) so the existing customer_phone-keyed tables (messages,
# appointments, primer_sent, etc.) work without any schema changes.
# =========================
import uuid as _uuid

WIDGET_DEALER_TWILIO_NUM = os.getenv("WIDGET_DEALER_TWILIO_NUM", "")
WIDGET_DEALER_NAME       = os.getenv("WIDGET_DEALER_NAME", "Auto District Indy")

app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")


def _session_to_phone(session_id: str) -> str:
    """Map a browser session id to a stable phone-like identifier so the
    existing routing logic (which keys everything on customer_phone) works
    without modification."""
    cleaned = re.sub(r"[^a-zA-Z0-9]", "", session_id)[:18]
    return f"+web{cleaned}"


@app.route("/")
def widget_home():
    return render_template(
        "index.html",
        dealer_name=WIDGET_DEALER_NAME,
        terms_url=PRIMER_TERMS_URL,
    )


@app.route("/chat", methods=["POST"])
def chat_webhook():
    if not WIDGET_DEALER_TWILIO_NUM:
        return jsonify({"error": "WIDGET_DEALER_TWILIO_NUM not configured"}), 500

    data = request.get_json(silent=True) or {}
    user_message = (data.get("message") or "").strip()
    session_id   = (data.get("session_id") or "").strip() or session.get("sid")

    if not session_id:
        session_id = _uuid.uuid4().hex
        session["sid"] = session_id

    if not user_message:
        return jsonify({"error": "empty message"}), 400

    from_number = _session_to_phone(session_id)
    to_number   = WIDGET_DEALER_TWILIO_NUM

    g.captured_reply  = None
    g.captured_primer = None

    try:
        _process_message(from_number, to_number, user_message)
    except Exception as e:
        app.logger.error("chat _process_message failed: %s", e)
        return jsonify({"error": "processing error"}), 500

    reply = g.get("captured_reply") or "Sorry, I had trouble processing that. Could you try again?"
    primer = g.get("captured_primer")

    return jsonify({
        "reply": reply,
        "primer": primer,
        "session_id": session_id,
    })


@app.route("/health")
def health():
    return jsonify({"ok": True, "dealer": WIDGET_DEALER_NAME})


@app.route("/debug/inventory")
def debug_inventory():
    """Quick diagnostic: shows what twilio_numbers have inventory and the
    count for the widget's configured dealer. Helps detect mismatches between
    what the scraper saved vs what the chat handler is looking up."""
    try:
        conn = _db()
        all_groups = conn.execute(
            "SELECT twilio_number, COUNT(*) as n FROM inventory GROUP BY twilio_number"
        ).fetchall()
        widget_count = conn.execute(
            "SELECT COUNT(*) FROM inventory WHERE twilio_number=?",
            (WIDGET_DEALER_TWILIO_NUM,),
        ).fetchone()[0]
        sample = conn.execute(
            "SELECT year, make, model FROM inventory WHERE twilio_number=? LIMIT 5",
            (WIDGET_DEALER_TWILIO_NUM,),
        ).fetchall()
        conn.close()
        return jsonify({
            "widget_dealer_twilio_num": WIDGET_DEALER_TWILIO_NUM,
            "rows_for_widget_dealer": widget_count,
            "all_dealer_groups": [{"twilio_number": tn, "count": n} for tn, n in all_groups],
            "sample_rows_for_widget_dealer": [
                {"year": y, "make": mk, "model": md} for y, mk, md in sample
            ],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =========================
# MODULE-LEVEL INIT (runs whether started via `python app.py` or gunicorn)
# Render hosts via gunicorn so __main__ never executes - we need tables and
# the scheduler set up at import time.
# =========================
init_db()
if os.getenv("DEV_CLEAR_DB", "0") == "1":
    try:
        with _db() as _conn:
            _conn.execute("DELETE FROM primer_sent")
        app.logger.info("DEV_CLEAR_DB=1 - cleared primer_sent on startup.")
    except Exception as _e:
        app.logger.warning("Could not clear primer_sent: %s", _e)

# Start the scheduler unless explicitly disabled (useful for unit tests).
if os.getenv("DISABLE_SCHEDULER", "0") != "1":
    try:
        start_scheduler()
    except Exception as _e:
        app.logger.warning("Scheduler failed to start: %s", _e)

# Kick off an immediate inventory scrape in a background thread so the web
# server starts replying instantly while inventory loads in parallel. Without
# this, a fresh deploy has empty inventory until the scheduler's first run
# (~30 min later). Skip when running locally via __main__ (handled there) or
# if SKIP_STARTUP_SCRAPE is set.
def _background_initial_scrape():
    try:
        app.logger.info("Module-level startup: kicking off background inventory scrape...")
        refresh_all_inventory(max_vehicles=0)
        app.logger.info("Background inventory scrape complete.")
    except Exception as _e:
        app.logger.warning("Background inventory scrape failed: %s", _e)


if __name__ != "__main__" and os.getenv("SKIP_STARTUP_SCRAPE", "0") != "1":
    import threading as _threading
    _threading.Thread(target=_background_initial_scrape, daemon=True).start()


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    # Dev prompt: limit vehicles loaded at startup
    # Enter = load all | number = load that many | 0 = skip scan (use cached DB)
    try:
        _dev_input = input("Load how many vehicles? (Enter=all, number=limit, 0=skip scan): ").strip()
        if _dev_input == "0":
            DEV_MAX_VEHICLES = 0
            _skip_scan = True
            print("[DEV] Skipping inventory scan - using cached database.")
        elif _dev_input.isdigit() and int(_dev_input) > 0:
            DEV_MAX_VEHICLES = int(_dev_input)
            _skip_scan = False
            print(f"[DEV] Will load first {DEV_MAX_VEHICLES} vehicles only.")
        else:
            DEV_MAX_VEHICLES = 0
            _skip_scan = False
            print("[DEV] Loading all vehicles.")
    except Exception:
        DEV_MAX_VEHICLES = 0
        _skip_scan = False

    # init_db() and start_scheduler() already ran at module-level above.
    if not _skip_scan:
        app.logger.info("Running initial inventory scan on startup...")
        refresh_all_inventory(max_vehicles=DEV_MAX_VEHICLES)
    else:
        app.logger.info("Skipped inventory scan - using cached data.")
    port = int(os.getenv("PORT", "5001"))  # Render sets PORT, default for local
    app.logger.info("Widget running for %s on http://0.0.0.0:%s", WIDGET_DEALER_NAME, port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

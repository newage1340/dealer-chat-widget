# twilio-bot2/app.py
import os
import re
import json
import sqlite3
import logging
import threading
import time
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore

import gspread
from flask import Flask, request, g, jsonify, render_template, session
from twilio.twiml.messaging_response import MessagingResponse
from twilio.twiml.voice_response import VoiceResponse, Gather
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
# Gmail SMTP — used to email dealers/staff in addition to SMS, when the
# dealer sheet has email columns filled in. GMAIL_APP_PASSWORD must be a
# Google App Password (not the account password); requires 2FA on the account.
GMAIL_USER                   = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD           = os.getenv("GMAIL_APP_PASSWORD", "")
NOTIFY_FROM_EMAIL            = os.getenv("NOTIFY_FROM_EMAIL", "") or GMAIL_USER
# Dealer's local timezone — used when formatting "current time" for the LLM
# prompt and when parsing customer-supplied appointment times. On Render the
# server clock is UTC, but the dealers are in Indianapolis, so without this
# the LLM thinks "now" is 4-5 hours later than it really is and rejects
# valid same-day appointment times. Override per deployment via DEALER_TZ.
DEALER_TZ_NAME               = os.getenv("DEALER_TZ", "America/Indiana/Indianapolis")
try:
    DEALER_TZ = ZoneInfo(DEALER_TZ_NAME) if ZoneInfo else None
except Exception:
    DEALER_TZ = None


def _now_local() -> datetime:
    """Current wall-clock time in the dealer's timezone, returned tz-naive
    so it composes with the rest of the codebase's naive-datetime arithmetic."""
    if DEALER_TZ is None:
        return datetime.now()
    return datetime.now(DEALER_TZ).replace(tzinfo=None)


REMINDER_LEAD_MINUTES        = 60
COLD_FOLLOWUP_AFTER_MINUTES  = 30
COLD_FOLLOWUP_MAX_AGE_HOURS  = 72
MAX_MESSAGES_PER_CHAT        = 40
PURGE_MESSAGES_OLDER_THAN_DAYS = 30
# Dev mode: set DEV_CLEAR_DB=1 to wipe appointments/conversations on startup
DEV_CLEAR_DB      = os.getenv("DEV_CLEAR_DB", "0") == "1"
# Dev mode: set DEV_MAX_VEHICLES=5 to only load first N vehicles (0 = no limit)
DEV_MAX_VEHICLES  = int(os.getenv("DEV_MAX_VEHICLES", "0"))

# SMS abuse filter: cap inbound SMS per (customer phone, twilio number) pair.
# In-memory only — resets when the bot restarts. Counts messages 1..8 normally.
# Message 9 returns a "call the dealer" notice. Messages 10+ get no reply.
SMS_ABUSE_LIMIT              = 8
SMS_ABUSE_NOTICE             = (
    "You have reached the message limit for this number. "
    "Please call the dealer directly if you have any more questions."
)
_sms_abuse_counts: Dict[Tuple[str, str], int] = {}
_sms_abuse_lock = threading.Lock()

PRIMER_TERMS_URL = os.getenv(
    "PRIMER_TERMS_URL",
    "https://inventiq.net/terms.html",
)
PRIMER_PRIVACY_URL = os.getenv(
    "PRIMER_PRIVACY_URL",
    "https://inventiq.net/privacy.html",
)
CAPABILITY_PRIMER = (
    "FYI - I can help with inventory, vehicles, financing, or scheduling a visit. "
    "By texting this number you agree to our Terms of Service. "
    "Replies are AI-assisted. Msg frequency varies, msg & data rates may apply. "
    "Reply MENU for options, HELP for help, STOP to opt out. "
    f"Terms: {PRIMER_TERMS_URL}"
)
# Sent on a customer's FIRST message when that message triggers the menu/
# greeting path. The menu itself already explains what the bot does, so the
# capability primer would be redundant - just include the terms/consent piece.
TERMS_ONLY_PRIMER = (
    "By texting this number you agree to our Terms of Service. "
    "Replies are AI-assisted. Msg frequency varies, msg & data rates may apply. "
    "Reply HELP for help, STOP to opt out anytime. "
    f"Terms: {PRIMER_TERMS_URL}"
)

# =========================
# DEMO DEALER (hardcoded — for the dealer-prospect demo widget)
# =========================
# A self-contained "dealer" with fixed inventory + a fixed sheet-style row.
# Bypasses the Google Sheet, the scraper, the DB inventory query, and all
# outbound staff/customer notifications — so a dealer-prospect can chat with
# the widget on the demo page without any real SMS/email getting sent.
# Survives Render restarts because everything is in code.
DEMO_DEALER_SLUG    = "inventiq-demo"
DEMO_DEALER_TWILIO  = "+15555550000"

# Only Auto District Indy uses the "every car on our lot is thoroughly
# inspected before being listed" reassurance clause. Other dealers haven't
# committed to that intake promise, so we don't put those words in their
# mouth.
AUTO_DISTRICT_INDY_TWILIO = "+18882810403"

# Sheet-style row — keys match the Google Form column headers so the
# existing alias-based helpers (get_row_field etc.) pick the right values.
_DEMO_DEALER_ROW: Dict[str, Any] = {
    "dealership name": "InventIQ Demo",
    "twilio number given to dealer (leave this blank)": DEMO_DEALER_TWILIO,
    "slug": DEMO_DEALER_SLUG,
    "brand color": "#c8221c",
    "logo url": "",
    "dealer phone number": "",
    "dealer address": "123 Demo Lane, Indianapolis, IN 46201",
    "dealer hours": "Monday-Friday: 9am to 6pm, Saturday: 9am to 5pm, Sunday: closed",
    "do you offer financing?": "Yes, we offer financing through multiple lenders and work with all credit types. You can apply online at https://inventiq.net/apply",
    "do you accept trade-ins? (feel free to be as detailed as you like)": "Yes, we accept trade-ins. A firm offer requires an in-person inspection.",
    'any dealership policies the ai should know? (ex: "no deposits" or "prices are firm")': "Prices firm, no deposits required to hold a vehicle.",
    "salesman phone numbers": "",
    "dealer email": "",
    "salesman emails": "",
    "website url": "",
}


def _demo_vehicle(year, make, model, trim, color, price, mileage, stock, description=""):
    """Construct a vehicle dict in the shape get_inventory_for_twilio returns."""
    return {
        "Year": str(year), "Make": make, "Model": model, "Trim": trim,
        "Color": color, "Price": str(price), "Mileage": str(mileage),
        "VIN": f"DEMO{stock}{year}", "Stock": f"D{stock}",
        "Description": description, "CarfaxURL": "", "DetailURL": "",
    }


_DEMO_INVENTORY: List[Dict[str, Any]] = [
    _demo_vehicle(2022, "BMW", "X7 Xdrive40I", "4-Door Suv", "Carbon Black Metallic",
                  45000, 38500, "001",
                  "Engine: 3.0L Turbo I6 | Transmission: 8-speed automatic | "
                  "Drive: xDrive AWD | Interior: Cognac Vernasca leather || "
                  "Powertrain ;; Turbocharged inline-six ;; xDrive intelligent AWD | "
                  "Comfort Features ;; Heated front seats ;; Panoramic moonroof ;; "
                  "Harman/Kardon audio | Safety ;; Active driving assistant ;; "
                  "Lane departure warning ;; Blind-spot monitoring"),
    _demo_vehicle(2023, "Honda", "Accord Hybrid", "Ex-L 4-Door Sedan", "Crystal Black Pearl",
                  19800, 24100, "002",
                  "Engine: 2.0L Hybrid I4 | Transmission: e-CVT | Drive: FWD | "
                  "Interior: Black leather || Powertrain ;; Two-motor hybrid ;; "
                  "204 combined hp | Comfort Features ;; Heated front seats ;; "
                  "Wireless phone charger | Safety ;; Honda Sensing suite ;; "
                  "Adaptive cruise control"),
    _demo_vehicle(2021, "Ford", "Ranger Xlt", "4-Door Truck", "Velocity Blue",
                  17000, 41500, "003",
                  "Engine: 2.3L EcoBoost I4 | Transmission: 10-speed automatic | "
                  "Drive: 4WD | Interior: Ebony cloth || Powertrain ;; "
                  "Turbocharged inline-four ;; Part-time 4WD with electronic locking | "
                  "Convenience Features ;; Tow/haul mode ;; FX4 Off-Road Package | "
                  "Safety ;; Pre-Collision Assist ;; Lane-Keeping System"),
    _demo_vehicle(2020, "Audi", "Q5 45", "Premium Plus Quattro 4-Door Suv", "Glacier White Metallic",
                  17900, 52000, "004",
                  "Engine: 2.0L TFSI I4 | Transmission: 7-speed S tronic | "
                  "Drive: Quattro AWD | Interior: Black leather || "
                  "Powertrain ;; Turbocharged inline-four ;; quattro all-wheel drive | "
                  "Comfort Features ;; Heated front seats ;; Panoramic sunroof ;; "
                  "Bang & Olufsen audio | Safety ;; Audi Pre Sense ;; Lane departure warning"),
    _demo_vehicle(2017, "Jeep", "Wrangler Unlimited", "Rubicon Recon 4-Door Suv", "Granite Crystal Metallic",
                  25989, 78000, "005",
                  "Engine: 3.6L V6 | Transmission: 5-speed automatic | Drive: 4WD | "
                  "Interior: Black leather || Powertrain ;; Pentastar V6 ;; "
                  "Rock-Trac 4WD ;; Electronic locking differentials | "
                  "Exterior Features ;; Removable hard top ;; 17-inch beadlock-capable wheels | "
                  "Convenience Features ;; Uconnect infotainment"),
    _demo_vehicle(2022, "GMC", "Sierra 3500", "Pro 4-Door Truck", "Summit White",
                  41000, 22500, "006",
                  "Engine: 6.6L Duramax Turbo-Diesel V8 | Transmission: Allison 10-speed | "
                  "Drive: 4WD | Interior: Jet Black cloth || Powertrain ;; "
                  "Turbo-diesel V8 ;; Allison automatic | Towing ;; "
                  "Up to 36,000 lb conventional tow rating ;; Integrated trailer brake controller | "
                  "Safety ;; HD Surround Vision ;; Trailering camera"),
    _demo_vehicle(2019, "Mercedes-Benz", "Glc Glc", "63 Amg 4Matic 4-Door Suv", "Selenite Grey Metallic",
                  45000, 47000, "007",
                  "Engine: 4.0L AMG biturbo V8 | Transmission: AMG SPEEDSHIFT MCT 9G | "
                  "Drive: 4MATIC+ AWD | Interior: Black Nappa leather || "
                  "Powertrain ;; Hand-built AMG V8 ;; 503 hp | "
                  "Performance ;; AMG RIDE CONTROL+ ;; Burmester surround sound | "
                  "Safety ;; Active Brake Assist ;; Blind Spot Assist"),
    _demo_vehicle(2018, "Volvo", "Xc90 T6", "Momentum 4-Door Suv", "Onyx Black Metallic",
                  16629, 89000, "008",
                  "Engine: 2.0L Turbo+Supercharged I4 | Transmission: 8-speed automatic | "
                  "Drive: AWD | Interior: Charcoal leather || Powertrain ;; "
                  "Twin-charged inline-four ;; All-wheel drive | "
                  "Seats ;; Three-row seating ;; Heated front seats | "
                  "Safety ;; City Safety auto-brake ;; Run-off road mitigation"),
    _demo_vehicle(2020, "Hyundai", "Palisade Sel", "4-Door Suv", "Steel Graphite",
                  22880, 38500, "009",
                  "Engine: 3.8L V6 | Transmission: 8-speed automatic | Drive: FWD | "
                  "Interior: Black cloth || Powertrain ;; Atkinson-cycle V6 ;; "
                  "Front-wheel drive | Seats ;; Three-row seating for 8 ;; "
                  "Heated front seats | Safety ;; Forward Collision-Avoidance Assist ;; "
                  "Blind-Spot Collision-Avoidance Assist"),
    _demo_vehicle(2017, "Cadillac", "Xt5 Luxury", "4-Door Suv", "Crystal White Tricoat",
                  17200, 65000, "010",
                  "Engine: 3.6L V6 | Transmission: 8-speed automatic | Drive: AWD | "
                  "Interior: Jet Black leather || Powertrain ;; Direct-injected V6 ;; "
                  "Intelligent AWD | Comfort Features ;; Heated and ventilated front seats ;; "
                  "Panoramic sunroof ;; Bose Centerpoint audio | Safety ;; "
                  "Forward Collision Alert ;; Lane Keep Assist"),
    _demo_vehicle(2015, "Land Rover", "Rover Range", "Rover Hse 4-Door Suv", "Santorini Black Metallic",
                  17000, 95000, "011",
                  "Engine: 3.0L Supercharged V6 | Transmission: 8-speed ZF automatic | "
                  "Drive: 4WD | Interior: Ebony leather || Powertrain ;; "
                  "Supercharged V6 ;; Terrain Response 2 | "
                  "Comfort Features ;; Heated front and rear seats ;; Panoramic roof ;; "
                  "Meridian audio | Safety ;; Lane Departure Warning"),
    _demo_vehicle(2018, "Mitsubishi", "Outlander Sport", "Se 4-Door Wagon", "Mercury Gray Metallic",
                  10299, 71000, "012",
                  "Engine: 2.4L I4 | Transmission: CVT | Drive: AWD | "
                  "Interior: Black cloth || Powertrain ;; MIVEC inline-four ;; "
                  "All-Wheel Control | Convenience Features ;; Heated front seats ;; "
                  "Touchscreen infotainment | Safety ;; Forward Collision Mitigation ;; "
                  "Lane Departure Warning"),
]


def _is_demo_twilio(twilio_number: str) -> bool:
    """True if the given twilio number is the hardcoded demo dealer's."""
    return normalize_phone(twilio_number) == DEMO_DEALER_TWILIO


def _dealer_uses_inspection_clause(twilio_number: str = "", dealer_row: Optional[Dict[str, Any]] = None) -> bool:
    """Only Auto District Indy may have the bot use the 'every car on our lot
    is thoroughly inspected before being listed' phrase, surface the price
    breakdown with ADI-specific doc fees, or use any other ADI-only voice.
    Other dealers get neutral replacements so we don't make intake claims on
    their behalf.

    Prefers dealer_row name/slug matching when provided — needed because
    during local testing multiple dealers may share a twilio number (the
    user only has one Twilio number provisioned), and the twilio-number
    check alone would mis-attribute non-ADI dealers as ADI. Falls back to
    twilio_number comparison for callers that don't have the dealer row
    handy (build_prompt-derived flows, the SMS scheduler jobs)."""
    if dealer_row is not None:
        name = (get_row_field(dealer_row, DEALER_NAME_ALIASES) or "").strip().lower()
        slug = (_normalize_slug(get_row_field(dealer_row, SLUG_ALIASES)) or _normalize_slug(name))
        return slug == "auto-district-indy" or name == "auto district indy"
    return normalize_phone(twilio_number) == AUTO_DISTRICT_INDY_TWILIO


# =========================
# APP + CLIENTS
# =========================
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
_log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.log")
_file_handler = logging.FileHandler(_log_file_path, mode="a", encoding="utf-8")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_file_handler)
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
    """Return the dealer whose 'Twilio number' column matches the inbound
    number. Returns {} if no row matches. Previously fell back to the last
    dealer in the sheet, which silently routed every unmatched number to
    whichever row happened to be last — causing all numbers to land on one
    dealer when others were removed or weren't yet provisioned."""
    tn = normalize_phone(twilio_to)
    if not tn:
        return {}
    # Demo dealer short-circuit (hardcoded, not in the sheet).
    if tn == DEMO_DEALER_TWILIO:
        return dict(_DEMO_DEALER_ROW)
    for d in reversed(dealers):
        if normalize_phone(get_row_field(d, TWILIO_NUMBER_ALIASES)) == tn:
            return d
    return {}


def _normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def select_dealer_for_slug(dealers: List[Dict[str, Any]], slug: str) -> Dict[str, Any]:
    """Find the dealer row matching the given slug. Falls back to deriving a
    slug from the dealer name if the slug column is empty, so the bot still
    works for dealers who haven't filled in the slug yet."""
    target = _normalize_slug(slug)
    if not target:
        return {}
    # Demo dealer short-circuit (hardcoded, not in the sheet).
    if target == DEMO_DEALER_SLUG:
        return dict(_DEMO_DEALER_ROW)
    for d in reversed(dealers):
        explicit = _normalize_slug(get_row_field(d, SLUG_ALIASES))
        if explicit and explicit == target:
            return d
    # Fallback: match by slugified dealer name
    for d in reversed(dealers):
        name_slug = _normalize_slug(get_row_field(d, DEALER_NAME_ALIASES))
        if name_slug and name_slug == target:
            return d
    return {}


def get_widget_branding(dealer: Dict[str, Any]) -> Dict[str, str]:
    """Return the dealer's widget branding fields with safe defaults."""
    name        = get_row_field(dealer, DEALER_NAME_ALIASES) or "Dealer"
    twilio_num  = normalize_phone(get_row_field(dealer, TWILIO_NUMBER_ALIASES))
    brand_color = (get_row_field(dealer, BRAND_COLOR_ALIASES) or "").strip()
    logo_url    = (get_row_field(dealer, LOGO_URL_ALIASES) or "").strip()
    slug        = _normalize_slug(get_row_field(dealer, SLUG_ALIASES)) \
                  or _normalize_slug(name)
    if not brand_color:
        brand_color = "#4a90e2"  # default accent
    return {
        "name": name,
        "twilio_number": twilio_num,
        "brand_color": brand_color,
        "logo_url": logo_url,
        "slug": slug,
    }


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
DEALER_NOTIFY_EMAIL_ALIASES = {
    "dealer email", "dealership email", "dealer email address",
    "dealership email address", "email", "email address",
    "notification email", "primary email",
}
SALESMAN_EMAILS_ALIASES = {
    "salesman emails", "salesman email", "salesman email addresses",
    "staff emails", "staff email", "staff email addresses",
    "notification emails",
}
WEBSITE_URL_ALIASES = {
    "website url", "website", "dealer website", "dealership website",
    "inventory website", "url", "site url",
}
SLUG_ALIASES = {
    "slug", "widget slug", "url slug", "dealer slug", "dealership slug",
    "short name", "shortname", "id", "dealer id",
}
BRAND_COLOR_ALIASES = {
    "brand color", "brand colour", "color", "colour", "primary color",
    "primary colour", "accent color", "widget color", "theme color",
    "brand color hex color code", "brand color hex code",
    "brand colour hex colour code", "hex color", "hex color code",
}
LOGO_URL_ALIASES = {
    "logo url", "logo", "logo image", "logo link", "brand logo",
    "dealer logo", "dealership logo",
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


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(e: str) -> str:
    e = (e or "").strip().strip("<>").strip()
    return e if _EMAIL_RE.match(e) else ""


def get_dealer_email(dealer_row: Dict[str, Any]) -> str:
    return normalize_email(get_row_field(dealer_row, DEALER_NOTIFY_EMAIL_ALIASES))


def get_salesman_emails(dealer_row: Dict[str, Any]) -> List[str]:
    raw = get_row_field(dealer_row, SALESMAN_EMAILS_ALIASES)
    if not raw:
        return []
    parts = re.split(r"[,;\s]+", raw)
    return [normalize_email(p) for p in parts if normalize_email(p)]


# =========================
# SQLITE - INIT
# =========================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _db() -> sqlite3.Connection:
    # timeout=30: how long a query waits for a write lock before raising
    # "database is locked". Default is 5s, which isn't enough during the
    # initial inventory scrape (lots of inserts back-to-back).
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _db()
    # PRAGMA journal_mode=WAL MUST run outside a transaction — SQLite silently
    # ignores the change if executed inside one (which is what `with conn:`
    # opens). WAL mode lets readers run concurrently with writers; without it
    # the DB falls back to default DELETE mode, where any chat request that
    # happens during a scrape's row-by-row writes will hit "database is
    # locked" once the 30s busy timeout expires.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
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
                        "trade_in_vehicle TEXT NOT NULL DEFAULT ''",
                        "real_phone TEXT NOT NULL DEFAULT ''"):
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
            CREATE TABLE IF NOT EXISTS terms_acceptance_log (
                real_phone TEXT PRIMARY KEY,
                first_name TEXT NOT NULL DEFAULT '',
                last_name TEXT NOT NULL DEFAULT '',
                dealer_name TEXT NOT NULL DEFAULT '',
                twilio_number TEXT NOT NULL DEFAULT '',
                accepted_at TEXT NOT NULL
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dealer_fees (
                twilio_number  TEXT PRIMARY KEY,
                doc_fee        TEXT NOT NULL DEFAULT '',
                title_tag_fee  TEXT NOT NULL DEFAULT '',
                updated_at     TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS voice_sessions (
                call_sid TEXT PRIMARY KEY,
                twilio_number TEXT NOT NULL,
                customer_phone TEXT NOT NULL,
                turns INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)

        if DEV_CLEAR_DB:
            app.logger.warning("DEV_CLEAR_DB=1 - wiping appointments, pending, messages, cold_followups, customer_names, terms_acceptance_log")
            conn.execute("DELETE FROM appointments")
            conn.execute("DELETE FROM pending_appointments")
            conn.execute("DELETE FROM pending_reconfirmations")
            conn.execute("DELETE FROM pending_cancellations")
            conn.execute("DELETE FROM cold_followups")
            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM customer_names")
            conn.execute("DELETE FROM terms_acceptance_log")
            # Also wipe the human-readable text log so dev testing starts fresh.
            try:
                if os.path.exists(TERMS_LOG_PATH):
                    os.remove(TERMS_LOG_PATH)
            except Exception as e:
                app.logger.warning("DEV_CLEAR_DB: could not remove %s: %s", TERMS_LOG_PATH, e)

    conn.close()


# =========================
# SQLITE - INVENTORY
# =========================

def save_dealer_fees(twilio_number: str, doc_fee: str, title_tag_fee: str) -> None:
    """Upsert this dealer's scraped fees. Empty strings overwrite nothing — they
    let us refresh just one field without wiping the other when only that one
    was found on the page."""
    tn = normalize_phone(twilio_number)
    if not tn:
        return
    if not doc_fee and not title_tag_fee:
        return
    conn = _db()
    with conn:
        existing = conn.execute(
            "SELECT doc_fee, title_tag_fee FROM dealer_fees WHERE twilio_number=?", (tn,)
        ).fetchone()
        new_doc = doc_fee if doc_fee else (existing["doc_fee"] if existing else "")
        new_tt  = title_tag_fee if title_tag_fee else (existing["title_tag_fee"] if existing else "")
        conn.execute(
            "INSERT INTO dealer_fees (twilio_number, doc_fee, title_tag_fee, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(twilio_number) DO UPDATE SET "
            "doc_fee=excluded.doc_fee, title_tag_fee=excluded.title_tag_fee, updated_at=excluded.updated_at",
            (tn, new_doc, new_tt, _utc_now_iso()),
        )
    conn.close()


def get_dealer_fees(twilio_number: str) -> Dict[str, float]:
    """Return {'doc_fee': float, 'title_tag_fee': float} for a dealer (zeros if not configured)."""
    tn = normalize_phone(twilio_number)
    out = {"doc_fee": 0.0, "title_tag_fee": 0.0}
    if not tn:
        return out
    conn = _db()
    row = conn.execute(
        "SELECT doc_fee, title_tag_fee FROM dealer_fees WHERE twilio_number=?", (tn,)
    ).fetchone()
    conn.close()
    if not row:
        return out
    try:
        out["doc_fee"] = float(row["doc_fee"]) if row["doc_fee"] else 0.0
    except (ValueError, TypeError):
        pass
    try:
        out["title_tag_fee"] = float(row["title_tag_fee"]) if row["title_tag_fee"] else 0.0
    except (ValueError, TypeError):
        pass
    return out


def get_inventory_for_twilio(twilio_number: str) -> List[Dict[str, Any]]:
    tn = normalize_phone(twilio_number)
    # Demo dealer has hardcoded inventory baked into the codebase — no DB.
    if tn == DEMO_DEALER_TWILIO:
        return [dict(v) for v in _DEMO_INVENTORY]
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
        # Doc fee + title/tag are the same across all this dealer's cars, so any
        # detail page that exposes them updates the dealer-level cache. Silent
        # no-op if both are empty.
        save_dealer_fees(tn, v.get("DocFee", ""), v.get("TitleTagFee", ""))

    vehicles = scrape_dealer_inventory(
        website_url,
        max_vehicles=max_vehicles,
        on_vehicle_scraped=_save_one,
        should_skip=_should_skip,
    )

    # Prune stale rows: anything whose scraped_at is older than the resume
    # cutoff (15 min before this attempt started) is from a previous full
    # scrape cycle and is now stale - vehicle was sold or removed. Rows
    # within the resume window (saved by an earlier crashed attempt this
    # cycle) are kept, since they're part of the same scrape session.
    if vehicles:
        try:
            conn = _db()
            with conn:
                conn.execute(
                    "DELETE FROM inventory WHERE twilio_number=? AND scraped_at < ?",
                    (tn, resume_cutoff),
                )
            conn.close()
        except Exception as e:
            app.logger.warning("Stale-row prune failed for %s: %s", tn, e)

    return len(vehicles)


def refresh_all_inventory(max_vehicles: int = 0) -> None:
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

    # Scrape dealers sequentially so only one Chromium instance is alive at a
    # time. Parallel scraping spikes memory past Render Starter's 512MB cap and
    # OOM-kills the worker mid-scrape. Per-dealer try/except so one failure
    # doesn't abort the rest of the cycle.
    for twilio_number, website_url, dealer_name in tasks:
        try:
            count = refresh_inventory_for_twilio(
                twilio_number, website_url, max_vehicles=max_vehicles
            )
            app.logger.info(
                "Inventory refreshed for %s (%s): %d vehicles",
                dealer_name, twilio_number, count,
            )
        except Exception as e:
            app.logger.error("Inventory refresh failed for %s: %s", dealer_name, e)


# =========================
# SQLITE - CUSTOMER NAMES
# =========================

def get_customer_profile(customer_phone: str, twilio_number: str) -> Dict[str, str]:
    conn = _db()
    row = conn.execute(
        "SELECT name, last_name, email, trade_in_vehicle, real_phone "
        "FROM customer_names WHERE customer_phone=? AND twilio_number=?",
        (customer_phone, twilio_number),
    ).fetchone()
    conn.close()
    if not row:
        return {"name": "", "last_name": "", "email": "", "trade_in_vehicle": "", "real_phone": ""}
    return {
        "name": (row["name"] or "").strip(),
        "last_name": (row["last_name"] or "").strip(),
        "email": (row["email"] or "").strip(),
        "trade_in_vehicle": (row["trade_in_vehicle"] or "").strip(),
        "real_phone": (row["real_phone"] or "").strip(),
    }


def save_customer_profile(customer_phone: str, twilio_number: str, *,
                          name: Optional[str] = None,
                          last_name: Optional[str] = None,
                          email: Optional[str] = None,
                          trade_in_vehicle: Optional[str] = None,
                          real_phone: Optional[str] = None) -> None:
    """Upsert customer profile. Only fields passed (non-None) are updated; existing values are preserved."""
    current = get_customer_profile(customer_phone, twilio_number)
    new_name = (name if name is not None else current["name"]).strip()
    new_last = (last_name if last_name is not None else current["last_name"]).strip()
    new_email = (email if email is not None else current["email"]).strip()
    new_trade = (trade_in_vehicle if trade_in_vehicle is not None else current["trade_in_vehicle"]).strip()
    new_phone = (real_phone if real_phone is not None else current["real_phone"]).strip()
    conn = _db()
    conn.execute(
        "INSERT OR REPLACE INTO customer_names "
        "(customer_phone, twilio_number, name, last_name, email, trade_in_vehicle, real_phone) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (customer_phone, twilio_number, new_name, new_last, new_email, new_trade, new_phone),
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
    """Return a human-readable label for the next missing/invalid field, or None if profile is complete.
    Last name is intentionally NOT required - it's optional metadata that should
    never block a booking."""
    if not profile.get("name"):
        return "first name"
    if not is_valid_email(profile.get("email", "")):
        return "email address"
    return None


# Path to the human-readable terms-acceptance log. Sits next to the SQLite DB.
TERMS_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "terms_acceptance.log")


def log_terms_acceptance(*, real_phone: str, first_name: str = "", last_name: str = "",
                         dealer_name: str = "", twilio_number: str = "") -> bool:
    """Record that the customer with this phone has accepted the terms and
    submitted their phone number. Once-per-phone (across all dealers) using
    INSERT OR IGNORE on terms_acceptance_log; only appends to the human-
    readable text file on the FIRST acceptance for a given phone. Returns
    True if newly logged, False if this phone was already on file."""
    rp = normalize_phone(real_phone or "")
    if not rp or not rp.startswith("+"):
        return False
    accepted_at = datetime.now().isoformat(timespec="seconds")
    conn = _db()
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO terms_acceptance_log "
            "(real_phone, first_name, last_name, dealer_name, twilio_number, accepted_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (rp, (first_name or "").strip(), (last_name or "").strip(),
             (dealer_name or "").strip(), (twilio_number or "").strip(), accepted_at),
        )
        conn.commit()
        was_new = cur.rowcount > 0
    except Exception as e:
        app.logger.warning("log_terms_acceptance DB write failed: %s", e)
        return False
    finally:
        conn.close()

    if not was_new:
        return False

    # Append a human-readable line to the text log. Best-effort - any IO
    # failure here is logged but does not raise.
    line = (
        f"[{accepted_at}] phone={rp} "
        f"name={first_name or '?'} {last_name or '?'} "
        f"dealer={dealer_name or '?'} twilio={twilio_number or '?'}\n"
    )
    try:
        os.makedirs(os.path.dirname(TERMS_LOG_PATH) or ".", exist_ok=True)
        with open(TERMS_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception as e:
        app.logger.warning("log_terms_acceptance file write failed: %s", e)
    return True


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


def mark_all_sessions_followed_up(real_phone: str, twilio_number: str) -> None:
    """When a cold follow-up goes out, mark EVERY session that shares the
    same real_phone (i.e. the same human) as already followed-up. Without
    this, a customer who used the widget multiple times — each clear/restart
    creates a fresh session_id — would receive one cold follow-up per
    abandoned session, all firing at once when the scheduler ticks."""
    if not real_phone or not twilio_number:
        return
    try:
        conn = _db()
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cold_followups (customer_phone, twilio_number, sent_at)
                SELECT customer_phone, ?, ?
                FROM customer_names
                WHERE real_phone=? AND twilio_number=?
                """,
                (twilio_number, _utc_now_iso(), real_phone, twilio_number),
            )
        conn.close()
    except Exception as e:
        app.logger.warning("mark_all_sessions_followed_up failed for %s: %s", real_phone, e)


def has_followup_for_real_phone(real_phone: str, twilio_number: str) -> bool:
    """Has ANY session belonging to this real phone already received a cold
    follow-up? Lets us dedupe across sessions: a customer who chatted,
    abandoned, came back in a fresh session and abandoned again should NOT
    receive a second SMS — unless they explicitly hit Clear Chat, which
    wipes the cold_followups history for that real phone."""
    if not real_phone or not twilio_number:
        return False
    try:
        conn = _db()
        row = conn.execute(
            """
            SELECT 1
            FROM cold_followups cf
            JOIN customer_names cn
              ON cf.customer_phone = cn.customer_phone
             AND cf.twilio_number  = cn.twilio_number
            WHERE cn.real_phone    = ?
              AND cn.twilio_number = ?
            LIMIT 1
            """,
            (real_phone, twilio_number),
        ).fetchone()
        conn.close()
        return bool(row)
    except Exception as e:
        app.logger.warning("has_followup_for_real_phone lookup failed for %s: %s", real_phone, e)
        return False


def clear_followup_history_for_real_phone(real_phone: str, twilio_number: str) -> None:
    """Delete every cold_followups row tied to any session of this real
    phone. Called from the Clear Chat endpoint so a customer who resets is
    eligible for a fresh follow-up / lead notification on their next visit."""
    if not real_phone or not twilio_number:
        return
    try:
        conn = _db()
        with conn:
            conn.execute(
                """
                DELETE FROM cold_followups
                WHERE twilio_number = ?
                  AND customer_phone IN (
                      SELECT customer_phone FROM customer_names
                      WHERE real_phone=? AND twilio_number=?
                  )
                """,
                (twilio_number, real_phone, twilio_number),
            )
        conn.close()
    except Exception as e:
        app.logger.warning("clear_followup_history_for_real_phone failed for %s: %s", real_phone, e)


def clear_cold_followup(customer_phone, twilio_number):
    conn = _db()
    with conn:
        conn.execute("DELETE FROM cold_followups WHERE customer_phone=? AND twilio_number=?",
                     (customer_phone, twilio_number))
    conn.close()


def get_upcoming_unreminded_appointments() -> List[Dict[str, Any]]:
    now = _now_local()
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
    now = now or _now_local()
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
    now = now or _now_local()
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
        price_part = f"${price}" if price else "Call for price"
        extras = [x for x in [color, f"{mileage} mi" if mileage else "", price_part] if x]
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
        matching = sum(1 for t in tokens if t in q_words)
        if matching:
            # Base weight for any match + additional bump per extra token
            # matched. Lets a 3-token-matched "Odyssey Touring Elite" beat a
            # 1-token-matched "Odyssey Ex-L" when the customer types "odyssey
            # touring", instead of both tying at the binary base weight.
            bonus += weight + (matching - 1) * (weight * 0.5)
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
        # Body-type / vehicle-class words appear inside the model field
        # (e.g. "Camry Se 4-Door Sedan") but are NOT unique model identifiers.
        # Without this exclusion, customer phrases like "out the door" or
        # "any sedans" falsely match every row whose model contains those words.
        _BODY_TYPE_TOKENS = {
            "door", "sedan", "coupe", "wagon", "hatchback", "convertible",
            "crossover", "minivan", "suv", "truck", "van", "pickup",
            "cabriolet", "roadster", "fastback",
        }
        for tok in re.sub(r"[^a-z0-9]", " ", model).split():
            if tok in _BODY_TYPE_TOKENS:
                continue
            if tok in b_words and (
                len(tok) >= 3
                or (len(tok) == 2 and any(c.isdigit() for c in tok) and any(c.isalpha() for c in tok))
            ):
                return True
        # Also match the model's nameplate with all non-alphanumeric stripped
        # (e.g. "Cr-V" → "crv", "F-150" → "f150"). Customers type the squashed
        # form ("crv") that the dash-separated version doesn't match.
        for nameplate in model.split():
            squashed = re.sub(r"[^a-z0-9]", "", nameplate)
            if squashed and len(squashed) >= 3 and squashed in b_words:
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
    # Negated forms: "not under 20k" → min=20k, "not over 30k" → max=30k.
    # Evaluated first so the plain regexes below don't capture them backwards.
    neg_under_m = re.search(rf"\bnot\s+(?:under|less than|below|cheaper than)\s+{_PRICE_TOKEN}", b)
    if neg_under_m:
        min_p = _parse_price_token(neg_under_m.group(1), neg_under_m.group(2))
    neg_over_m  = re.search(rf"\bnot\s+(?:over|more than|above)\s+{_PRICE_TOKEN}", b)
    if neg_over_m:
        max_p = _parse_price_token(neg_over_m.group(1), neg_over_m.group(2))
    # Plain forms — skip matches that were already captured by a "not" prefix.
    if min_p is None and max_p is None:
        under_m = re.search(rf"(?<!not\s)\b(?:under|less than|below|cheaper than|max(?:imum)?|up to|no more than|<=?)\s+{_PRICE_TOKEN}", b)
        if under_m:
            max_p = _parse_price_token(under_m.group(1), under_m.group(2))
        over_m = re.search(rf"(?<!not\s)\b(?:over|more than|above|at least|min(?:imum)?|>=?)\s+{_PRICE_TOKEN}", b)
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


_MOTORCYCLE_MAKES = {
    "harleydavidson", "harley", "ducati", "indianmotorcycle", "aprilia",
    "ktm", "vespa", "motoguzzi", "buell", "royalenfield", "victorymotorcycles",
    "huskvarna", "husqvarna", "bimota", "mvagusta",
}
_CAR_BODY_TRIM_RE = re.compile(
    r"\b(4-?door|sedan|suv|hatchback|coupe|truck|van|wagon|convertible|crew\s*cab|double\s*cab|supercrew|extended\s*cab)\b",
    re.I,
)
_MOTORCYCLE_TRIM_RE = re.compile(
    r"\b(cruiser|sportbike|sport\s*bike|street\s*bike|dirt\s*bike|dual\s*sport|motorcycle|moped|scooter)\b",
    re.I,
)


def _is_motorcycle(r: Dict[str, Any]) -> bool:
    """Detect motorcycles so they can be filtered out of 'cars' queries."""
    make = re.sub(r"[^a-z]+", "", str(r.get("Make", "")).lower())
    if make in _MOTORCYCLE_MAKES:
        return True
    trim = str(r.get("Trim", "")).lower()
    # If the trim names a car body, it's a car (even if "Cruiser" appears in model name like PT Cruiser).
    if _CAR_BODY_TRIM_RE.search(trim):
        return False
    if _MOTORCYCLE_TRIM_RE.search(trim):
        return True
    return False


def _find_exact_year_make_match(body: str, rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """If the body explicitly names a year AND a make (or first model word) that
    appear together in a single inventory row, return that row. Beats the fuzzy
    matcher when the customer is unambiguous ('2019 ram', '2023 honda accord')."""
    if not body or not rows:
        return None
    b = body.lower()
    year_m = re.search(r"\b(19|20)\d{2}\b", b)
    if not year_m:
        return None
    year = year_m.group(0)
    # Strip hyphens from body and model tokens so the customer's "f250"
    # matches inventory's "f-250" (and "cr-v" matches "crv", "rx-350" matches
    # "rx350", etc.). Without this, hyphenated model nameplates silently fall
    # through to the fuzzy matcher, which sometimes picks a different year of
    # the same model (e.g. customer asks about 2008 F-250, fuzzy picks 2010).
    b_nohyphen = b.replace("-", "")
    candidates: List[Dict[str, Any]] = []
    for r in rows:
        if str(r.get("Year", "")).strip() != year:
            continue
        make_lower = str(r.get("Make", "")).strip().lower()
        model_lower = str(r.get("Model", "")).strip().lower()
        make_first = make_lower.split()[0] if make_lower else ""
        model_first = model_lower.split()[0] if model_lower else ""
        make_first_squash = make_first.replace("-", "")
        model_first_squash = model_first.replace("-", "")
        make_hit = bool(make_first) and (
            re.search(rf"\b{re.escape(make_first)}\b", b)
            or (make_first_squash and re.search(rf"\b{re.escape(make_first_squash)}\b", b_nohyphen))
        )
        model_hit = bool(model_first) and (
            re.search(rf"\b{re.escape(model_first)}\b", b)
            or (model_first_squash and re.search(rf"\b{re.escape(model_first_squash)}\b", b_nohyphen))
        )
        if make_hit or model_hit:
            candidates.append(r)
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # Multiple rows match year+make (e.g. two 2019 RAMs, or two 2008 F-250
    # trims). Prefer one whose secondary model/trim token ALSO appears in
    # the body. Same hyphen-strip handling so "xlt" / "x-l" / "ex-l" match.
    for r in candidates:
        model_full = str(r.get("Model", "")).strip().lower()
        trim_full = str(r.get("Trim", "")).strip().lower()
        secondary_tokens = [t for t in (model_full.split()[1:] + trim_full.split()) if len(t) >= 2]
        for t in secondary_tokens:
            t_squash = t.replace("-", "")
            if re.search(rf"\b{re.escape(t)}\b", b) or (t_squash and re.search(rf"\b{re.escape(t_squash)}\b", b_nohyphen)):
                return r
    return candidates[0]


_RELATIVE_CHEAPER_RE = re.compile(
    r"\b(?:cheaper(?:\s+than\s+(?:that|this|the\s+\w+|it))?|"
    r"less\s+expensive(?:\s+than\s+(?:that|this|the\s+\w+|it))?|"
    r"more\s+affordable|"
    r"something\s+cheaper|anything\s+cheaper|anything\s+less\s+expensive|"
    r"below\s+(?:that|that\s+price))\b",
    re.I,
)

_RELATIVE_PRICIER_RE = re.compile(
    r"\b(?:more\s+expensive(?:\s+than\s+(?:that|this|the\s+\w+|it))?|"
    r"pricier(?:\s+than\s+(?:that|this|the\s+\w+|it))?|"
    r"something\s+more\s+expensive|anything\s+more\s+expensive|"
    r"higher[\s-]?(?:priced|end))\b",
    re.I,
)


def _extract_relative_price_filter(body: str, history: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    """Translate 'cheaper than that' or 'more expensive than that' into a (min_p,
    max_p) filter, using the most recent price mentioned in history as the
    anchor. Returns (None, None) when no relative qualifier or no anchor."""
    if not body:
        return (None, None)
    is_cheaper = bool(_RELATIVE_CHEAPER_RE.search(body))
    is_pricier = bool(_RELATIVE_PRICIER_RE.search(body))
    if not (is_cheaper or is_pricier):
        return (None, None)
    if not history:
        return (None, None)
    ref_price = None
    for msg in reversed(history):
        if msg.get("role") != "assistant":
            continue
        content = msg.get("content") or ""
        prices = re.findall(r"\$\s*([\d,]+)", content)
        if prices:
            try:
                ref_price = int(prices[0].replace(",", ""))
                break
            except ValueError:
                continue
    if ref_price is None:
        return (None, None)
    if is_cheaper:
        return (None, ref_price - 1)
    return (ref_price + 1, None)


_RELATIVE_NEWER_RE = re.compile(
    r"\b(?:not\s+(?:as|that|too)\s+old|newer(?:\s+than\s+(?:that|this|the\s+\w+|it))?|"
    r"more\s+recent(?:\s+than\s+(?:that|this|the\s+\w+|it))?|"
    r"less\s+old|fairly\s+new|something\s+newer|anything\s+newer|"
    r"a\s+(?:bit\s+)?newer|(?:bit\s+)?more\s+modern)\b",
    re.I,
)


def _extract_relative_year_floor(body: str, history: List[Dict[str, Any]]) -> Optional[int]:
    """Translate relative qualifiers like 'not as old' or 'newer than that' into
    a year floor by looking back at the most recent vehicle year mentioned in
    history. Returns None when there's no qualifier or no reference year."""
    if not body:
        return None
    if not _RELATIVE_NEWER_RE.search(body):
        return None
    # Explicit reference year in the body wins (e.g. "newer than 2015").
    explicit = re.search(
        r"\b(?:newer\s+than|after|past|since)\s+(?:the\s+)?(19\d{2}|20\d{2})\b",
        body, re.I,
    )
    if explicit:
        return int(explicit.group(1)) + 1
    if not history:
        return None
    # Otherwise use the most recently mentioned (assistant) reference year — the
    # vehicle the customer is comparing AGAINST. Pick the OLDEST year in that
    # message so "not as old" raises the floor above the oldest one shown.
    for msg in reversed(history):
        if msg.get("role") != "assistant":
            continue
        content = msg.get("content") or ""
        years = re.findall(r"\b(19\d{2}|20\d{2})\b", content)
        if years:
            ref_year = min(int(y) for y in years)
            return ref_year + 5
    return None


_EXCLUSION_NEGATION_RE = re.compile(
    r"\b(?:not|except|besides|other\s+than|anything\s+but|excluding|"
    r"don'?t\s+want|aside\s+from|no(?:\s+more)?)\s+"
    r"(?:that\s+|the\s+|a\s+|an\s+|those\s+|these\s+|any\s+)?"
    r"(?:cheap\s+|expensive\s+|old\s+|new\s+|small\s+|big\s+)?"
    r"(?:[a-z]+\s+){0,2}([a-z][\w-]+)",
    re.I,
)


def _extract_exclude_makes(body: str, rows: List[Dict[str, Any]]) -> List[str]:
    """Identify makes the customer explicitly wants EXCLUDED ('not that jeep',
    'anything but Honda', 'except Toyota'). Returns canonical inventory make
    strings to filter out. Empty list when no negation pattern is found."""
    if not body or not rows:
        return []
    b = body.lower()
    inv_makes = {str(r.get("Make", "")).strip().lower() for r in rows if r.get("Make")}
    first_to_full: Dict[str, str] = {}
    for m in inv_makes:
        if m:
            first_to_full.setdefault(m.split()[0], m)
    excludes: List[str] = []
    for match in _EXCLUSION_NEGATION_RE.finditer(b):
        token = match.group(1).lower()
        full = first_to_full.get(token)
        if full and full not in excludes:
            excludes.append(full)
    return excludes


_SUPERLATIVE_PATTERNS = [
    (r"\b(cheapest|least\s+expensive|lowest[\s-]?priced?)\b", ("price", True,  "cheapest")),
    (r"\b(most\s+expensive|priciest|highest[\s-]?priced?)\b",  ("price", False, "most expensive")),
    (r"\b(newest|most\s+recent|latest)\b",                     ("year",  False, "newest")),
    (r"\b(oldest)\b",                                          ("year",  True,  "oldest")),
    (r"\b(lowest\s+mileage|fewest\s+miles|least\s+miles|lowest\s+miles)\b",
                                                               ("mileage", True,  "lowest-mileage")),
    (r"\b(highest\s+mileage|most\s+miles|highest\s+miles)\b",
                                                               ("mileage", False, "highest-mileage")),
]


def _extract_superlative_query(body: str):
    """Return (sort_field, ascending, label) if the body uses a superlative
    like 'cheapest', 'newest', 'lowest mileage'. None otherwise."""
    if not body:
        return None
    b = body.lower()
    for pat, info in _SUPERLATIVE_PATTERNS:
        if re.search(pat, b):
            return info
    return None


_BUDGET_INTENT_RE = re.compile(
    r"\b(on\s+a\s+(?:tight\s+|low\s+|small\s+)?budget|tight\s+budget|low\s+budget|"
    r"small\s+budget|budget[\s-]?friendly|anything\s+(?:cheap|affordable|inexpensive)|"
    r"something\s+(?:cheap|affordable|inexpensive)|can'?t\s+afford\s+much|"
    r"not\s+much\s+(?:money|to\s+spend)|don'?t\s+have\s+much\s+(?:money|to\s+spend)|"
    r"(?:affordable|cheap|inexpensive)\s+(?:car|vehicle|option|"
    r"suvs?|trucks?|pickups?|sedans?|vans?|minivans?|coupes?|"
    r"hatchbacks?|wagons?|convertibles?|crossovers?))\b",
    re.I,
)


def _is_budget_intent(body: str) -> bool:
    """Vague budget intent ('on a budget', 'anything affordable') with no specific $ amount.
    Returns False when a price filter is in the message — those go through the price handler."""
    if not body:
        return False
    if re.search(r"\b(under|less\s+than|below|max(?:imum)?|up\s+to|"
                 r"between|over|more\s+than|above|min(?:imum)?)\s*\$?\s*\d", body, re.I):
        return False
    return bool(_BUDGET_INTENT_RE.search(body))


def _wants_cars_only(body: str) -> bool:
    """True when the customer explicitly asks for 'car' / 'cars' (intent: exclude motorcycles)."""
    return bool(body) and bool(re.search(r"\bcars?\b", body, re.I))


def _format_price_listing(rows: List[Dict[str, Any]], min_p: Optional[int], max_p: Optional[int],
                          cars_only: bool = False, exclude_makes: Optional[List[str]] = None) -> str:
    """Deterministic, complete listing of inventory rows that match a price filter."""
    exclude_set = {m.lower() for m in (exclude_makes or [])}
    matching = []
    for r in rows:
        if cars_only and _is_motorcycle(r):
            continue
        if exclude_set and str(r.get("Make", "")).strip().lower() in exclude_set:
            continue
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
    # When there's a max budget, show the 5 highest-priced vehicles within range —
    # the picks closest to the customer's stated budget. A customer saying "under
    # $20k" usually has $20k to spend; leading with the top of their budget is more
    # relevant than the cheapest in inventory. Sorted descending so the cap shows
    # first. Customers can ask "any more?" / "what else?" to page through the rest.
    if max_p is not None and len(matching) > LIST_LIMIT:
        picks = list(reversed(matching[-LIST_LIMIT:]))
    else:
        picks = matching[:LIST_LIMIT]
    lines = [header]
    for p, r in picks:
        year  = str(r.get("Year",  "")).strip()
        make  = str(r.get("Make",  "")).strip()
        model = str(r.get("Model", "")).strip()
        title = " ".join(s for s in [year, make, model] if s)
        lines.append(f"- {title}: ${p:,}")
    lines.append("")
    if len(matching) > len(picks):
        lines.append(f"...and {len(matching) - len(picks)} more in this range. Tell me a make, year, or anything else and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


# ── Body/fuel/drivetrain feature filtering ──────────────────────────────────
# Customers ask "diesel trucks", "any AWD SUVs", "Ford trucks". Without these
# filters the LLM was either hallucinating or dropping cars from the list.

_BODY_TYPE_QUERY = {
    # IMPORTANT: "commercial" must come BEFORE "truck" and "van" because dict
    # iteration is insertion-ordered, and we want a "box truck" / "cargo van"
    # query to match "commercial" instead of falling into the broader truck/van
    # buckets. Otherwise commercial vehicles get mixed with consumer pickups.
    "commercial": (
        r"\b("
        r"commercial(?:\s+(?:vehicles?|trucks?|vans?))?|"
        r"box[\s-]?trucks?|box[\s-]?vans?|cube[\s-]?trucks?|cube[\s-]?vans?|"
        r"cargo[\s-]?vans?|cargo[\s-]?trucks?|step[\s-]?vans?|"
        r"work[\s-]?trucks?|work[\s-]?vans?|"
        r"delivery[\s-]?trucks?|delivery[\s-]?vans?|"
        r"fleet[\s-]?vehicles?|cutaways?|stake[\s-]?beds?|flatbeds?|"
        r"dump[\s-]?trucks?|tow[\s-]?trucks?"
        r")\b"
    ),
    "truck": r"\b(trucks?|pickups?(?:\s+trucks?)?)\b",
    "suv":   r"\b(suvs?|crossovers?)\b",
    "sedan": r"\b(sedans?)\b",
    "van":   r"\b(vans?|minivans?)\b",
    "coupe": r"\b(coupes?)\b",
    "hatchback":   r"\b(hatchbacks?)\b",
    "wagon":       r"\b(wagons?)\b",
    "convertible": r"\b(convertibles?|drop[- ]?tops?)\b",
}


# Always-commercial makes: any row with these makes is a commercial vehicle
# regardless of model. Hino, Freightliner, etc. don't make consumer vehicles.
_COMMERCIAL_MAKES = {
    "hino", "freightliner", "international", "mack", "peterbilt", "kenworth",
}
# Commercial-specific model name fragments. Used in conjunction with any make.
# Catches commercial Isuzu (NPR/NQR/FRR/FTR), Mitsubishi Fuso, and bodies-only
# rows (e.g. a Ford F-450 cutaway or Chevy Express cargo van).
# Body-type fragments (apply to any make)
_COMMERCIAL_BODY_HINTS = [
    "box truck", "box van", "cube truck", "cube van", "cargo van",
    "step van", "cutaway", "stake bed", "flatbed", "dump truck",
    "tow truck", "delivery van", "delivery truck",
    "work van", "work truck", "chassis cab",
]
# Specific commercial-only model names. Listed lowercase, matched with word
# boundaries so e.g. "Express" alone (Chevy commercial van) and "ProMaster"
# (Ram commercial van) get tagged. Ford F-450 and above are heavy-duty
# commercial chassis; Silverado 4500/5500/6500 likewise.
_COMMERCIAL_MODEL_NAMES = [
    # Isuzu commercial line
    "npr", "nqr", "nrr", "frr", "ftr", "fxr",
    # Mitsubishi Fuso (the "Fuso" name is the signal)
    "fuso",
    # Ford commercial vans / heavy-duty chassis
    "e-150", "e150", "e-250", "e250", "e-350", "e350", "e-450", "e450", "e-series",
    "f-450", "f450", "f-550", "f550", "f-650", "f650", "f-750", "f750",
    "transit connect", "transit cargo", "transit chassis",
    "transit 150", "transit 250", "transit 350",
    # Nissan commercial van line (NV)
    "nv200", "nv1500", "nv2500", "nv3500",
    # Chevy / GMC commercial
    "express cargo", "express van", "express 2500", "express 3500",
    "low cab forward", "lcf",
    "silverado 4500", "silverado 5500", "silverado 6500",
    "savana cargo", "savana 2500", "savana 3500",
    # Ram commercial
    "promaster",
    # Mercedes / Freightliner Sprinter (commercial van)
    "sprinter",
]
# Combined list used by _is_commercial_row. Body hints are general phrases;
# model names are exact-ish substrings that mark a specific commercial unit.
_COMMERCIAL_MODEL_HINTS = _COMMERCIAL_BODY_HINTS + _COMMERCIAL_MODEL_NAMES


def _commercial_subtype_prefix(body: str) -> str:
    """When the customer asked for a specific commercial sub-type (box truck,
    cargo van, work truck, cutaway, etc.) rather than generic "commercial",
    return a one-line disclaimer letting them know we group those together.
    Empty string when the customer used the generic "commercial" label or
    didn't use a sub-type at all."""
    if not body:
        return ""
    if re.search(
        r"\b(box[\s-]?trucks?|box[\s-]?vans?|cube[\s-]?trucks?|cube[\s-]?vans?|"
        r"cargo[\s-]?vans?|cargo[\s-]?trucks?|step[\s-]?vans?|"
        r"work[\s-]?trucks?|work[\s-]?vans?|"
        r"delivery[\s-]?trucks?|delivery[\s-]?vans?|"
        r"cutaways?|stake[\s-]?beds?|flatbeds?|"
        r"dump[\s-]?trucks?|tow[\s-]?trucks?)\b",
        body, re.I,
    ):
        return ("Just a heads up — we group box trucks, cargo vans, work trucks, "
                "and similar units together. ")
    return ""


def _is_commercial_row(r: Dict[str, Any]) -> bool:
    """Return True if the inventory row is a commercial vehicle (box truck,
    cargo van, cutaway, etc.) — NOT a consumer pickup or minivan.

    Only inspects Model + Trim, NOT Description. Description prose ("perfect
    work truck for contractors", "spacious cargo area") was triggering false
    positives on consumer vehicles like the Hyundai Santa Cruz and Santa Fe."""
    make = str(r.get("Make", "")).strip().lower()
    if make in _COMMERCIAL_MAKES:
        return True
    haystack = (
        str(r.get("Model", "")) + " " +
        str(r.get("Trim", ""))
    ).lower()
    return any(hint in haystack for hint in _COMMERCIAL_MODEL_HINTS)

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


# Fallback model-name → body-type hints. Some scrapers don't include the body
# type in titles/descriptions (e.g., DealerCarSearch lists "2019 Honda Civic
# Sport" with no "sedan" anywhere), so without these hints body-type filters
# would silently miss obvious matches. Keys are lowercase model substrings
# matched against the Model field. Order doesn't matter; multiple hits OK.
_MODEL_BODY_TYPE_HINTS: Dict[str, List[str]] = {
    "sedan": [
        # Honda / Acura
        "civic", "accord", "insight", "tlx", "ilx", "rlx", "tsx", "tl",
        # Toyota / Lexus
        "camry", "corolla", "avalon", "yaris", "is 250", "is 300", "is 350",
        "es 300", "es 330", "es 350", "gs 350", "gs 450", "ls 400", "ls 460",
        "ls 500",
        # Chevy / GM / Buick / Cadillac / Pontiac / Saturn
        "malibu", "cruze", "impala", "sonic", "spark", "regal", "lacrosse",
        "verano", "cts", "ats", "xts", "ct5", "ct6", "dts", "sts", "catera",
        "g6", "g8", "grand prix", "bonneville", "aura", "ion",
        # Chrysler / Dodge / Plymouth
        "200", "300", "charger", "avenger", "stratus", "neon",
        # Ford / Lincoln / Mercury
        "fusion", "taurus", "focus sedan", "mkz", "mks", "continental",
        "sable",
        # Hyundai / Kia / Genesis
        "sonata", "elantra", "accent", "azera", "g70", "g80", "g90",
        "optima", "forte", "rio", "k5", "cadenza", "stinger",
        # Nissan / Infiniti
        "altima", "sentra", "maxima", "versa sedan", "q50", "q60", "q70",
        "m37", "m45", "g35", "g37",
        # Volkswagen
        "jetta", "passat", "arteon", " cc ",
        # BMW (3, 5, 7 series)
        "320i", "328i", "330i", "335i", "340i", "m340", "525", "528", "530",
        "535", "540", "550", "m5", "740", "745", "750", "760", "m760",
        # Mercedes-Benz (A, C, E, S, CLA, CLS)
        "a-class", "a220", "a250", "c-class", "c220", "c230", "c240", "c250",
        "c280", "c300", "c320", "c350", "c400", "c450", "c63",
        "e-class", "e300", "e320", "e350", "e400", "e500", "e550", "e63",
        "s-class", "s400", "s450", "s500", "s550", "s560", "s580", "s600",
        "s63", "s65", "cla", "cls",
        # Audi
        "a3", "a4", "a5 sedan", "a6", "a7", "a8", "s3", "s4", "s5 sedan",
        "s6", "s7", "s8",
        # Volvo
        "s40", "s60", "s80", "s90",
        # Mazda / Subaru
        "mazda3 sedan", "mazda6", "legacy", "impreza sedan",
        # Mitsubishi / Saab / Tesla
        "lancer", "galant", "mirage", "9-3", "9-5", "model s", "model 3",
        # Recent EVs and additions (post-2020)
        "lucid air", "polestar 2", "ioniq 6", "i4", "i5", "i7", "e-tron gt",
        "crown", "mirai", "ct4",
    ],
    "suv": [
        # Honda / Acura
        "cr-v", "crv", "pilot", "hr-v", "hrv", "passport", "rdx", "mdx",
        "zdx",
        # Toyota / Lexus
        "rav4", "highlander", "4runner", "sequoia", "land cruiser", "venza",
        "c-hr", "rx ", "rxl", "gx ", "lx ", "nx ", "ux ",
        # Chevy / GM / Buick / Cadillac
        "equinox", "tahoe", "suburban", "trax", "traverse", "blazer",
        "trailblazer", "acadia", "yukon", "terrain", "envoy", "envision",
        "encore", "enclave", "rendezvous", "srx", "xt4", "xt5", "xt6",
        "escalade",
        # Ford / Lincoln
        "escape", "explorer", "edge", "bronco", "expedition", "flex",
        "ecosport", "mkc", "mkx", "corsair", "nautilus", "aviator",
        "navigator",
        # Hyundai / Kia / Genesis
        "tucson", "santa fe", "palisade", "kona", "venue", "nexo",
        "sportage", "sorento", "telluride", "soul", "niro", "seltos",
        "gv70", "gv80",
        # Nissan / Infiniti
        "rogue", "murano", "pathfinder", "armada", "kicks", "xterra",
        "qx30", "qx50", "qx55", "qx56", "qx60", "qx70", "qx80",
        # Jeep
        "wrangler", "cherokee", "grand cherokee", "compass", "patriot",
        "renegade", "liberty", "commander",
        # Volkswagen / Audi / Porsche
        "tiguan", "atlas", "touareg", "taos", "id.4", "q3", "q5", "q7",
        "q8", "e-tron", "cayenne", "macan",
        # BMW
        "x1", "x2", "x3", "x4", "x5", "x6", "x7",
        # Mercedes-Benz
        "gla", "glb", "glc", "gle", "gls", "g-class", "g550", "g63", "ml",
        " gl ", "eqb", "eqc",
        # Subaru / Mazda / Mitsubishi
        "forester", "outback", "ascent", "crosstrek", "cx-3", "cx-30",
        "cx-5", "cx-50", "cx-9", "outlander", "eclipse cross", "asx",
        # Dodge / Ram
        "durango", "journey", "nitro",
        # Land Rover / Range Rover
        "range rover", "discovery", "defender", "velar", "evoque",
        "freelander", "lr2", "lr3", "lr4",
        # Volvo
        "xc40", "xc60", "xc70", "xc90",
        # Lincoln (older)
        "navigator",
        # Tesla / Jaguar / Maserati / Bentley / Lambo / Porsche
        "model x", "model y", "f-pace", "e-pace", "i-pace", "levante",
        "bentayga", "urus",
        # Recent EVs and additions (post-2020)
        "r1s", "envista", "grand highlander", "ioniq 5", "ev9", "ev6", "gv60",
        "ix", "ix1", "ix3", "ix5", "bz4x", "ariya", "solterra",
        "grand wagoneer", "wagoneer", "mach-e", "mache", "bronco sport",
        "lyriq",
    ],
    "truck": [
        # Ford
        "f-150", "f150", "f-250", "f250", "f-350", "f350", "f-450", "f450",
        "f-550", "f550", "f-650", "f650", "f-750", "f750", "ranger",
        "maverick", "lightning",
        # Chevy / GMC
        "silverado", "colorado", "avalanche", "s-10", "s10", "sierra",
        "canyon", "hummer ev",
        # Toyota / Nissan / Honda
        "tacoma", "tundra", "frontier", "titan", "ridgeline",
        # Ram / Dodge
        "ram 1500", "ram 2500", "ram 3500", "ram 4500", "ram 5500", "dakota",
        # Hyundai
        "santa cruz",
        # Jeep
        "gladiator",
        # Lincoln
        "mark lt",
        # Commercial / box trucks
        "hino ", "npr", "nqr", "international 4",
        # Recent EV trucks
        "r1t", "cybertruck",
    ],
    "van": [
        # Minivans
        "odyssey", "sienna", "pacifica", "town & country", "town and country",
        "caravan", "grand caravan", "sedona", "carnival", "quest", "routan",
        "vanagon", "eurovan", "mpv",
        # Cargo / passenger vans
        "transit", "e-150", "e-250", "e-350", "e150", "e250", "e350",
        "sprinter", "metris", "nv200", "nv1500", "nv2500", "nv3500",
        "express", "astro", "savana", "promaster", "promaster city",
        # Recent additions
        "id buzz", "id.buzz", "voyager",
    ],
    "coupe": [
        "camaro", "corvette", "challenger", "mustang", "350z", "370z",
        "gt-r", "supra", " 86 ", "brz", "genesis coupe", "prelude", "s2000",
        "nsx", "rc 350", "rc 300", "rc 200", "lc 500", "lc 600",
        "slk", " sl ", "amg gt", "cle", " cl ", "tt", "r8", "911",
        "718 cayman", "718 boxster", "rx-7", "rx-8",
        "solstice", "sky", "eclipse", "3000gt", "talon", "stealth", "viper",
        "cougar", "riviera", "m2", "m4", "i8",
    ],
    "hatchback": [
        "prius", "fit", "civic hatch", "fiesta", "focus hatch",
        "golf", "rabbit", "gti", "r32",
        "mazda3 hatch", "veloster", "accent hatch", "elantra gt",
        "rio hatch", "soul", "forte hatch",
        "cooper", "hardtop", "clubman", "versa note", "leaf", "i3",
        "sonic hatch", "aveo hatch",
        # Older / less common hatchbacks
        "yaris hatch", "matrix", "vibe",
    ],
    "wagon": [
        "v60", "v70", "v90", "sportwagen", "allroad", "a4 avant",
        "a6 avant", "outback", "pt cruiser",
        # Performance wagons
        "rs4 avant", "rs6 avant", "magnum",
    ],
    "convertible": [
        "miata", "mx-5", "z3", "z4", "z8", "boxster", "eos", "beetle convertible",
        "spyder", "cabriolet", "cabrio", "roadster", "drop-top", "drop top",
    ],
}


def _normalize_for_hint_match(text: str) -> str:
    """Insert a space between letters/digits so 'GLS450' becomes 'gls 450',
    'ML350' becomes 'ml 350', etc. Lets word-boundary matching hit prefix
    style model names without false positives like 's450' matching 'gls450'."""
    s = (text or "").lower()
    s = re.sub(r"([a-z])(\d)", r"\1 \2", s)
    s = re.sub(r"(\d)([a-z])", r"\1 \2", s)
    return s


def _hint_matches_word(hint: str, normalized_haystack: str) -> bool:
    """Word-boundary match of a hint against the normalized haystack."""
    norm_hint = _normalize_for_hint_match(hint)
    return bool(re.search(r"\b" + re.escape(norm_hint) + r"\b", normalized_haystack))


def _model_hint_matches(model_field: str, body_type: str) -> bool:
    """True if the vehicle's model field contains a known hint for this body
    type. Used as a fallback when the literal body-type word isn't in the
    scraped data."""
    hints = _MODEL_BODY_TYPE_HINTS.get(body_type, [])
    if not hints:
        return False
    normalized = _normalize_for_hint_match(model_field)
    return any(_hint_matches_word(h, normalized) for h in hints)


def _row_matches_body_type(r: Dict[str, Any], body_type: str) -> bool:
    if not body_type:
        return True
    # Commercial bucket: ONLY commercial vehicles (Hino, box trucks, cargo
    # vans, cutaways, etc.) match. Consumer pickups don't.
    if body_type == "commercial":
        return _is_commercial_row(r)
    # Trucks: exclude commercial vehicles so a customer asking for "trucks"
    # doesn't get a Hino mixed in with their F-150s. Vans are NOT excluded —
    # a Ram Promaster, Ford Transit, Sprinter etc. are still vans regardless
    # of work/cargo configuration, and customers asking "what vans do you
    # have?" want to see them.
    if body_type == "truck" and _is_commercial_row(r):
        return False
    # Two-door cars are coupes — even when the dealer scrapes them as
    # "2-Door Sedan", "2-Door Hatchback", or "2-Door Convertible" (a 2-door
    # convertible like a BMW 335i shows up under Coupes AND Convertibles).
    # Exceptions:
    # - utility vehicles (trucks/SUVs/vans) where 2-door is still that type
    #   (a 2-door Wrangler is an SUV, a regular-cab pickup is a truck)
    # - motorcycles (Harley Softail Deluxe is tagged "2-Door Cruiser" but
    #   is a motorcycle, not a coupe)
    _model_trim = (str(r.get("Model", "")) + " " + str(r.get("Trim", ""))).lower()
    _is_two_door = bool(re.search(r"\b2[-\s]?door\b", _model_trim))
    _is_utility = bool(re.search(
        r"\b(truck|pickup|suv|van|minivan|crossover|wagon|cruiser|motorcycle)\b",
        _model_trim,
    ))
    if _is_two_door and not _is_utility and not _is_motorcycle(r):
        if body_type == "coupe":
            return True
        if body_type in ("sedan", "hatchback"):
            return False
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
    # Strongly conflicting body words. Each query's excluders are body words
    # that DEFINITELY mean "this row isn't that body type."
    #
    # NOTE: "wagon" is intentionally NOT a strong excluder for SUV queries —
    # some compact crossovers (Rogue Sport, Crosstrek, Outlander Sport, Venue)
    # get tagged "Wagon" by dealer feeds even though they're SUVs, and
    # customers asking for SUVs DO want to see them.
    _STRONG_EXCLUDERS = {
        "suv":         {"sedan", "truck", "pickup", "van", "minivan", "coupe", "convertible"},
        "truck":       {"sedan", "van", "minivan", "coupe", "convertible", "hatchback", "suv", "crossover", "wagon"},
        "sedan":       {"truck", "pickup", "van", "minivan", "suv", "crossover", "convertible", "coupe", "wagon"},
        "van":         {"sedan", "truck", "pickup", "coupe", "convertible", "wagon"},
        "coupe":       {"sedan", "truck", "pickup", "van", "minivan", "convertible", "hatchback", "wagon", "suv", "crossover"},
        "hatchback":   {"truck", "pickup", "van", "minivan", "coupe", "convertible", "suv", "crossover", "wagon"},
        "wagon":       {"truck", "pickup", "van", "minivan", "coupe", "convertible"},
        "convertible": {"sedan", "truck", "pickup", "van", "minivan", "coupe", "hatchback", "wagon", "suv", "crossover"},
    }
    excluders = _STRONG_EXCLUDERS.get(body_type, set())
    # Apply excluders FIRST, and ONLY against model + trim — never against
    # description prose. Marketing descriptions often mention multiple body
    # types ("coupe-like styling on this sedan", "4-door coupe-inspired SUV"),
    # which would otherwise either false-positive (a GLC SUV showing up under
    # Coupes because the description says "coupe-style") or false-negative
    # (a real coupe getting rejected because the description mentions
    # "sedan comfort"). Trim + model are the authoritative classifier.
    model_trim = (str(r.get("Model", "")) + " " + str(r.get("Trim", ""))).lower()
    if excluders and any(re.search(r"\b" + re.escape(w) + r"\b", model_trim) for w in excluders):
        return False
    # Now check the full haystack (model + trim + description) for the
    # query's own alias words.
    h = _row_haystack(r)
    if any(re.search(r"\b" + re.escape(a) + r"\b", h) for a in aliases):
        return True
    # Fallback: check the model name against known-model hints. Catches cars
    # whose scraped data doesn't include the body type word (e.g., Civic,
    # Malibu, Chrysler 200 listed without "sedan" in the title).
    model_field = str(r.get("Model", ""))
    return _model_hint_matches(model_field, body_type)


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
    "convertible": "convertibles", "commercial": "commercial vehicles",
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
    # Detail-about-a-specific-car phrasing ("more about the bmw",
    # "tell me more about that toyota", "info about this jeep") is NOT a
    # listing query even though it contains listing trigger words. The
    # singular determiner ("the"/"that"/"this") right after "about" is
    # the giveaway. Bail out so the message routes to the single-vehicle
    # / AI path instead of listing every car of that make.
    detail_about_singular = bool(re.search(
        r"\b(?:"
        r"more\s+(?:(?:info(?:rmation)?|details?)\s+)?about|"
        r"(?:info(?:rmation)?|details?)\s+about|"
        r"tell\s+me\s+(?:more\s+)?about|"
        r"know\s+more\s+about|"
        r"learn\s+more\s+about"
        r")\s+(?:the|that|this)\b",
        b,
    ))
    if detail_about_singular:
        return []
    listing_intent = bool(re.search(
        r"\b(any|other|more|what|which|list|show|all|got|"
        r"do you have|are there|is there|carry|stock|"
        r"got any|have you got|"
        r"looking\s+for|interested\s+in|want|wanting|need|"
        r"i\s+(?:want|need|like|would\s+like)|find\s+me|"
        r"show\s+me|how\s+about|what\s+about)\b",
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
    # Drop any makes the customer explicitly negated ("not that jeep",
    # "anything but Honda"). Otherwise "I want something under 8k but not that
    # jeep" would still filter TO Jeeps.
    excluded = set(_extract_exclude_makes(body, rows))
    if excluded:
        return [c for _, c in hits if c not in excluded]
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
        price_str = f" for ${p:,}" if p > 0 else " — call for price"
        return (f"Yes - we have the {title}{price_str}. "
                f"Would you like more details or to schedule a visit?")

    LIST_LIMIT = 5
    lines = [f"Here are our {label}{label_noun}{price_qual}:"]
    for p, r in matching[:LIST_LIMIT]:
        title = _vehicle_title(r)
        price_str = f": ${p:,}" if p > 0 else ": Call for price"
        lines.append(f"- {title}{price_str}")
    lines.append("")
    if len(matching) > LIST_LIMIT:
        lines.append(f"...and {len(matching) - LIST_LIMIT} more. Tell me a price range, year, or anything else and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


def _format_superlative_listing(rows: List[Dict[str, Any]], body: str,
                                 field: str, ascending: bool, label: str) -> Optional[str]:
    """Return a deterministic answer for superlative queries like 'cheapest SUV',
    'newest truck', 'lowest mileage Toyota'. Respects body_type / fuel /
    drivetrain / make filters extracted from the body. Returns None when no
    candidate has a valid value for the sort field."""
    body_type = _extract_body_type(body)
    fuel = _extract_fuel_type(body)
    drive = _extract_drivetrain(body)
    cars_only = _wants_cars_only(body)
    # When the customer says "car" (singular or plural) but doesn't name a
    # specific body type, treat it as "passenger car" — exclude trucks, SUVs,
    # vans, and motorcycles. EXCEPT when the customer signals broad scope with
    # phrases like "in general", "overall", "any kind", "regardless".
    _broad_scope = bool(re.search(
        r"\b(in\s+general|overall|of\s+any\s+(?:kind|type)|any\s+(?:kind|type)|regardless)\b",
        body, re.I,
    ))
    _passenger_car_only = cars_only and body_type is None and not _broad_scope
    b_lower = body.lower()
    # Direct make detection — _extract_make_filters needs a "listing intent" word
    # like "any" or "show me", which superlative queries usually don't have
    # ("cheapest Honda", "newest Toyota"). Scan inventory makes against the body
    # directly so we still filter by make when it's mentioned.
    makes = set()
    for r in rows:
        m_norm = str(r.get("Make", "")).strip().lower()
        if not m_norm:
            continue
        first = m_norm.split()[0]
        if len(first) >= 3 and re.search(rf"\b{re.escape(first)}\b", b_lower):
            makes.add(m_norm)

    # Pick up any price filters too ("cheapest SUV not under 20k", "newest
    # truck under 30k"). The superlative still controls which row wins; the
    # price filter just narrows the pool first.
    s_min_p, s_max_p = _extract_price_range(body)

    candidates = []
    for r in rows:
        if cars_only and _is_motorcycle(r):
            continue
        if _passenger_car_only:
            rtrim = str(r.get("Trim", "")).lower()
            if re.search(r"\b(suv|truck|van|crossover|crew\s*cab|pickup)\b", rtrim):
                continue
        if not _row_matches_features(r, body_type, fuel, drive):
            continue
        if makes:
            rmake = str(r.get("Make", "")).strip().lower()
            if rmake not in makes:
                continue
        rp = _row_price_int(r)
        if s_min_p is not None and rp < s_min_p:
            continue
        if s_max_p is not None and rp > s_max_p:
            continue
        if field == "price":
            v = _row_price_int(r)
            if v <= 0:
                continue
        elif field == "year":
            try:
                v = int(str(r.get("Year", "")).strip())
            except (ValueError, TypeError):
                continue
        elif field == "mileage":
            mi = re.sub(r"[^\d]", "", str(r.get("Mileage", "")))
            if not mi:
                continue
            try:
                v = int(mi)
            except ValueError:
                continue
        else:
            continue
        candidates.append((v, r))

    if not candidates:
        return None
    candidates.sort(key=lambda t: t[0], reverse=not ascending)
    top = candidates[0][1]
    year_s = str(top.get("Year", "")).strip()
    make_s = str(top.get("Make", "")).strip()
    model_s = str(top.get("Model", "")).strip()
    title = " ".join(s for s in [year_s, make_s, model_s] if s)
    p = _row_price_int(top)
    price_str = f"${p:,}" if p > 0 else "Call for price"
    mileage_str = ""
    mi = re.sub(r"[^\d]", "", str(top.get("Mileage", "")))
    if mi:
        try:
            mileage_str = f"{int(mi):,} miles"
        except ValueError:
            pass

    scope = ""
    if body_type:
        scope = " " + _BODY_TYPE_LABEL.get(body_type, body_type + "s").rstrip("s")
    header_phrases = {
        "cheapest":         f"Our cheapest{scope}",
        "most expensive":   f"Our most expensive{scope}",
        "newest":           f"Our newest{scope}",
        "oldest":           f"Our oldest{scope}",
        "lowest-mileage":   f"Our lowest-mileage{scope}",
        "highest-mileage":  f"Our highest-mileage{scope}",
    }
    header = header_phrases.get(label, f"Top match{scope}")
    detail = f"the {title} at {price_str}"
    if mileage_str and field != "price":
        detail += f" with {mileage_str}"
    elif mileage_str:
        detail += f" ({mileage_str})"
    return f"{header} is {detail}. Want more details or to schedule a visit?"


def _format_budget_listing(rows: List[Dict[str, Any]], body: str,
                            year_min: Optional[int] = None) -> Optional[str]:
    """Return the cheapest 5 vehicles matching any body_type/make hint from
    the body. Used for vague budget queries like 'on a budget', 'anything cheap'.
    year_min lets relative qualifiers ('not as old') raise the year floor."""
    body_type = _extract_body_type(body)
    cars_only = _wants_cars_only(body) or body_type is None
    makes = _extract_make_filters(body, rows) or []
    excludes = {m.lower() for m in _extract_exclude_makes(body, rows)}
    candidates = []
    for r in rows:
        if cars_only and _is_motorcycle(r):
            continue
        if body_type and not _row_matches_body_type(r, body_type):
            continue
        if makes:
            rmake = str(r.get("Make", "")).strip().lower()
            if not any(rmake == t or rmake.startswith(t + " ") or rmake.startswith(t + "-") for t in makes):
                continue
        if excludes and str(r.get("Make", "")).strip().lower() in excludes:
            continue
        if year_min is not None:
            try:
                if int(str(r.get("Year", "")).strip()) < year_min:
                    continue
            except (ValueError, TypeError):
                continue
        p = _row_price_int(r)
        if p <= 0:
            continue
        candidates.append((p, r))
    if not candidates:
        return None
    candidates.sort(key=lambda t: t[0])
    LIST_LIMIT = 5
    picks = candidates[:LIST_LIMIT]
    scope = ""
    if body_type:
        scope = " " + _BODY_TYPE_LABEL.get(body_type, body_type + "s")
    header = f"Here are our most affordable{scope}:"
    lines = [header]
    for p, r in picks:
        year_s = str(r.get("Year", "")).strip()
        make_s = str(r.get("Make", "")).strip()
        model_s = str(r.get("Model", "")).strip()
        title = " ".join(s for s in [year_s, make_s, model_s] if s)
        lines.append(f"- {title}: ${p:,}")
    lines.append("")
    if len(candidates) > len(picks):
        lines.append(f"...and {len(candidates) - len(picks)} more. Tell me a budget or feature and I'll narrow it down.")
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
        price_str = f" for ${p:,}" if p > 0 else " — call for price"
        return (f"Yes - we have the {title}{price_str}. "
                f"Would you like more details or to schedule a visit?")

    LIST_LIMIT = 5
    lines = [f"Here are our {label}{label_noun}{price_qual}:"]
    for p, r in matching[:LIST_LIMIT]:
        title = _vehicle_title(r)
        price_str = f": ${p:,}" if p > 0 else ": Call for price"
        lines.append(f"- {title}{price_str}")
    lines.append("")
    if len(matching) > LIST_LIMIT:
        lines.append(f"...and {len(matching) - LIST_LIMIT} more. Tell me a price range, year, or anything else and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


def _format_new_arrivals_listing(rows: List[Dict[str, Any]]) -> str:
    """List vehicles that don't have a public price yet — these are typically
    just-arrived units the dealer hasn't priced. Used for 'new arrivals' /
    'what's new' style queries."""
    new_arrivals = [r for r in rows if _row_price_int(r) <= 0]
    # Sort newest year first so genuinely new units bubble up.
    def _key(r):
        try:
            yi = int(str(r.get("Year", "")).strip())
        except ValueError:
            yi = 0
        return -yi
    new_arrivals.sort(key=_key)

    if not new_arrivals:
        return ("We don't have any new arrivals listed at the moment - "
                "everything currently in inventory has a posted price. "
                "Want me to show you what we have?")

    LIST_LIMIT = 8
    lines = ["Here are our latest arrivals (call for pricing):"]
    for r in new_arrivals[:LIST_LIMIT]:
        title = _vehicle_title(r)
        lines.append(f"- {title}")
    lines.append("")
    if len(new_arrivals) > LIST_LIMIT:
        lines.append(f"...and {len(new_arrivals) - LIST_LIMIT} more. Ask about any specific one and I'll share the details.")
    else:
        lines.append("Want details on any of these or to schedule a visit to see one?")
    return "\n".join(lines)


def _is_new_arrivals_question(body: str) -> bool:
    """Detect 'what's new', 'any new arrivals', 'new inventory', 'just got
    in' style questions. Distinct from asking for brand-new (current year)
    cars - this is about freshly-arrived stock. Patterns are written to
    tolerate common typos: 'arrivals' / 'arivals' / 'arrivels' / 'arrival'
    all match via the 'arr?iv\\w*' fragment."""
    s = (body or "").lower()
    # Reusable typo-tolerant fragment for arrivals/arrival/arived/arrived/etc.
    arr = r"arr?iv\w*"
    if not re.search(
        rf"\b(new\s+{arr}|new\s+inventory|new\s+stock|new\s+to\s+the\s+lot|"
        rf"new\s+(cars?|vehicles?|trucks?|suvs?|sedans?)|"
        rf"recen?tly\s+arr?ived?|recen?t\s+{arr}|"
        rf"just\s+(got|came)\s+in|just\s+arr?ived?|"
        rf"lat[ei]st\s+{arr}|fresh\s+inventory|fresh\s+{arr}|"
        rf"what'?s\s+new|whats\s+new|anything\s+new|got\s+anything\s+new)\b",
        s,
    ):
        return False
    # Don't fire if customer is asking specifically about a make/model that
    # happens to include "new" in some other sense.
    if re.search(r"\bnew\s+(toyota|honda|ford|chevy|bmw|mercedes)\b", s):
        return False
    return True


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
        rf"^what(?:'?s|\s+is|\s+are)\s+(?:the|some|any)\s+(?:other|others?|rest|remaining)(?:\s+\d+|\s+ones?|\s+(?:cars?|vehicles?|trucks?|suvs?|sedans?|vans?|coupes?|wagons?|hatchbacks?|convertibles?|minivans?))?{suffix}$",
        rf"^what(?:'?s|\s+is)\s+the\s+other\s+one{suffix}$",
        rf"^(?:show|list|tell)\s+(?:me\s+)?(?:the\s+)?(?:other|others|rest|remaining)(?:\s+\d+|\s+ones?)?{suffix}$",
        rf"^show\s+(me\s+)?more{suffix}$",
        rf"^can\s+(?:you|i)\s+(?:see|get|have|view)\s+(?:some\s+|any\s+)?more{suffix}$",
        rf"^can\s+you\s+(?:show|list|tell|give|send)\s+(?:me\s+)?(?:some\s+|any\s+|a\s+few\s+)?more{suffix}$",
        rf"^(?:list|show|give|send)\s+(?:me\s+)?(?:some\s+|a\s+few\s+|any\s+)?more{suffix}$",
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
    (year + at least one >=3-char model token both present on the SAME line).

    Per-line matching matters: listings put one vehicle per line, and the
    LLM sometimes co-locates a year from one row with a model token from
    another inside the same paragraph. Requiring same-line co-occurrence
    prevents falsely flagging a 2021 Ford Ranger as "already listed" just
    because some other line had "2021" and another line had "xlt"."""
    if not text or not candidate_rows:
        return []
    # Split on newlines AND on bullet markers so single-line lists like
    # "- A - B - C" still treat each entry separately.
    lines = re.split(r"[\n\r]+|(?:(?<=\S)\s+-\s+(?=\S))", text.lower())
    # Generic body/structure words that appear in nearly every model field
    # (e.g. "4-Door Sedan" → tokens include "door"). Without filtering them
    # out, ANY row whose Model contains "Door" gets falsely flagged as
    # "already listed" just because a sibling line also has "4-Door" — that
    # silently drops legit unmentioned vehicles like BMW 330I / 550I from
    # "rest of" listings.
    _GENERIC_MODEL_TOKENS = {
        "door", "sedan", "truck", "pickup", "van", "minivan", "coupe",
        "hatchback", "wagon", "convertible", "suv", "crossover",
    }
    listed = []
    for r in candidate_rows:
        year  = str(r.get("Year",  "")).strip()
        model = str(r.get("Model", "")).strip().lower()
        if not year:
            continue
        model_tokens = [
            tok for tok in re.sub(r"[^a-z0-9]", " ", model).split()
            if len(tok) >= 3 and tok not in _GENERIC_MODEL_TOKENS
        ]
        for line in lines:
            if year not in line:
                continue
            if not model_tokens:
                listed.append(r)
                break
            if any(re.search(rf"\b{re.escape(tok)}\b", line) for tok in model_tokens):
                listed.append(r)
                break
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
        price_str = f": ${p:,}" if p > 0 else ": Call for price"
        lines.append(f"- {title}{price_str}")

    lines.append("")
    if len(valid) > limit:
        lines.append(f"...and {len(valid) - limit} more. Tell me a make, model, or price range and I'll narrow it down.")
    else:
        lines.append("Would you like more details on any of these, or to schedule a visit?")
    return "\n".join(lines)


def _extract_car_from_last_bot_message(history: List[Dict[str, Any]], inventory_rows: List[Dict[str, Any]]):
    """Find the vehicle the bot most recently discussed. Walks back through
    assistant messages until one references a single vehicle. Returns None if
    we hit a listing (2+ year+make pairs) before finding a single-vehicle
    message — listings are ambiguous, so the caller should route through the
    LLM rather than guess.

    Walking back is critical: when the bot's most recent reply was a generic
    fallback like "I don't have that information", the customer's next
    follow-up ("what about a vin") would otherwise lose all vehicle context."""
    for msg in reversed(history):
        if msg.get("role") != "assistant":
            continue
        content = (msg.get("content") or "").lower()
        # Detect listing context: 2+ "<year> <make>" mentions usually means
        # the bot was enumerating vehicles, not talking about one.
        year_make_pairs = set(re.findall(r"\b(19[5-9]\d|20[0-2]\d)\s+([a-z][a-z\-]+)", content))
        if len(year_make_pairs) >= 2:
            return None
        # Build a hyphen-stripped word set from content so "F-250" (which
        # tokenizes to ["f", "250"] under default splitting) still matches a
        # row whose model nameplate is "F-250" -> "f250".
        content_words = set(re.split(
            r"[^a-z0-9]+",
            content.replace("-", ""),
        ))
        best_row, best_score = None, 0
        for r in inventory_rows:
            year  = str(r.get("Year",  "")).strip().lower()
            make  = str(r.get("Make",  "")).strip().lower()
            model = str(r.get("Model", "")).strip().lower()
            trim  = str(r.get("Trim",  "")).strip().lower()
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
            # Tiebreaker for same-nameplate variants: credit for secondary
            # model tokens / trim words present in the message. Lets Odyssey
            # Touring Elite beat Odyssey Ex-L when the bot was talking about
            # the former specifically.
            secondary_tokens = set(
                re.sub(r"[^a-z0-9]", "", t)
                for t in (model.split()[1:] + trim.split())
            )
            secondary_tokens.discard("")
            for tok in secondary_tokens:
                if len(tok) >= 2 and tok in content_words:
                    score += 2
            if score > best_score:
                best_score, best_row = score, r
        if best_row:
            return best_row
        # No vehicle in this assistant message (likely a generic fallback or
        # acknowledgment) — keep walking back to an earlier one that did
        # discuss a specific car.
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


def _format_vehicle_essentials(r: Dict[str, Any], prior_reply: str, dealer_row: Optional[Dict[str, Any]] = None) -> str:
    """Build a deterministic essentials sentence (price, mileage, issues) for vehicle-info
    replies. Skips any item already covered in the bot's immediately-prior reply so the
    customer doesn't see the same numbers twice. Returns '' when everything was already
    covered. Caller is expected to follow this with an LLM-generated features blurb.

    The "no known issues and a clean CARFAX" hardcoded claim is ADI-only voice
    (we have no way to actually verify the CARFAX is clean for any vehicle).
    For other dealers we use a neutral "doesn't have any issues listed" phrasing
    that doesn't make a CARFAX-cleanliness claim we can't back up."""
    if not r:
        return ""
    prior = (prior_reply or "").lower()

    parts: List[str] = []

    price = _row_price_int(r)
    prior_has_price = bool(re.search(r"\$\s*\d|internet\s*price|priced\s+at", prior))
    if price > 0 and not prior_has_price:
        parts.append(f"is priced at ${price:,}")

    mileage_raw = re.sub(r"[^\d]", "", str(r.get("Mileage", "")))
    prior_has_mileage = bool(re.search(r"\d[\d,]*\s*miles?\b", prior))
    if mileage_raw and not prior_has_mileage:
        try:
            mileage_int = int(mileage_raw)
            if mileage_int > 0:
                parts.append(f"has {mileage_int:,} miles")
        except ValueError:
            pass

    prior_has_issues = bool(re.search(
        r"\b(carfax|no\s+(?:known\s+)?issues?|known\s+issues?|reconditioned|"
        r"disclosed\s+concerns?|clean\s+title)\b", prior
    ))
    if not prior_has_issues:
        issues = " | ".join(get_row_field_values(r, ISSUE_NOTE_HEADER_ALIASES)).strip()
        if issues:
            parts.append(f"has the following disclosed concerns: {issues}")
        elif _dealer_uses_inspection_clause(dealer_row=dealer_row):
            parts.append("comes with no known issues and a clean CARFAX")
        else:
            parts.append("doesn't have any issues listed")

    if not parts:
        return ""
    title = _vehicle_title(r)
    if len(parts) == 1:
        return f"The {title} {parts[0]}."
    if len(parts) == 2:
        return f"The {title} {parts[0]} and {parts[1]}."
    return f"The {title} {parts[0]}, {parts[1]}, and {parts[2]}."


def _issue_response_for_match(r, twilio_number: str = "", dealer_row: Optional[Dict[str, Any]] = None):
    title   = _vehicle_title(r)
    issues  = " | ".join(get_row_field_values(r, ISSUE_NOTE_HEADER_ALIASES)).strip()
    service = " | ".join(get_row_field_values(r, MAINT_WORK_HEADER_ALIASES)).strip()
    if issues:
        return f"Regarding the {title} - disclosed concerns: {issues}." + (f" Features/highlights: {service}." if service else "")
    if _dealer_uses_inspection_clause(twilio_number, dealer_row=dealer_row):
        no_issues_clause = f"There aren't any issues listed for the {title}, but every car on our lot is thoroughly inspected before being listed."
    else:
        no_issues_clause = f"There aren't any issues listed for the {title} — the dealer team can walk you through anything they know about it in person."
    if service:
        return f"{no_issues_clause} Features/highlights: {service}."
    return f"{no_issues_clause} Would you like to set up a time to come see it?"


def _title_status_response_for_match(r):
    title        = _vehicle_title(r)
    title_status = " | ".join(get_row_field_values(r, TITLE_STATUS_ALIASES)).strip()
    return f"The {title} carries a {title_status} title." if title_status else f"Title status information is not currently on file for the {title}."


_WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_DAY_ALIASES = {
    "mon": "Monday", "monday": "Monday",
    "tue": "Tuesday", "tues": "Tuesday", "tuesday": "Tuesday",
    "wed": "Wednesday", "weds": "Wednesday", "wednesday": "Wednesday",
    "thu": "Thursday", "thur": "Thursday", "thurs": "Thursday", "thursday": "Thursday",
    "fri": "Friday", "friday": "Friday",
    "sat": "Saturday", "saturday": "Saturday",
    "sun": "Sunday", "sunday": "Sunday",
}


def _parse_hours_string(hours_str: str) -> Dict[str, str]:
    """Parse common hours-string formats into {weekday: 'closed' or 'X to Y'}.
    Handles: 'Monday-Saturday: 9am to 6pm, Sunday: closed', 'Mon-Fri 8-5',
    'Open daily 9-5', etc. Unknown days default to absent."""
    result: Dict[str, str] = {}
    if not hours_str:
        return result
    # Split on common segment separators
    # Split on standard separators (comma, semicolon, pipe, slash, newline) AND
    # on whitespace that sits right before a day name when the preceding token
    # was a time or paren — handles dealer hours strings like
    # "Mon - Sat : 9:00 AM - 6:00 PM Sun : Closed" (no comma between the open
    # range and the Sunday-closed clause). Without this split, "Closed" leaks
    # into the Mon-Sat segment and marks every weekday as closed too.
    segments = re.split(
        r"[,;|/\n]+"
        r"|(?<=[)\sap]m)\s+(?=(?:mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)\b)",
        hours_str,
        flags=re.I,
    )
    for seg in segments:
        seg = seg.strip().rstrip(".")
        if not seg:
            continue
        is_closed = bool(re.search(r"\bclosed\b", seg, re.I))
        # Detect "daily" / "every day" → all 7 days
        if re.search(r"\b(daily|every\s*day|all\s*week)\b", seg, re.I):
            day_indices = list(range(7))
        else:
            # Day range like "Mon-Sat" or "Monday - Saturday"
            range_match = re.search(
                r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)\s*[-–to]+\s*(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)\b",
                seg, re.I,
            )
            if range_match:
                start = _DAY_ALIASES.get(range_match.group(1).lower())
                end   = _DAY_ALIASES.get(range_match.group(2).lower())
                if start in _WEEKDAYS and end in _WEEKDAYS:
                    s, e = _WEEKDAYS.index(start), _WEEKDAYS.index(end)
                    day_indices = list(range(s, e + 1)) if s <= e else (
                        list(range(s, 7)) + list(range(0, e + 1))
                    )
                else:
                    continue
            else:
                # Single day(s) - find any day mention(s)
                singles = re.findall(
                    r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)\b",
                    seg, re.I,
                )
                day_indices = []
                for d in singles:
                    canonical = _DAY_ALIASES.get(d.lower())
                    if canonical and canonical in _WEEKDAYS:
                        day_indices.append(_WEEKDAYS.index(canonical))
                if not day_indices:
                    continue
        # Pull the open/close time pair if not closed
        hours_text = "closed"
        if not is_closed:
            time_match = re.search(
                r"(\d{1,2}(?::\d{2})?\s*(?:am|pm|a\.?m\.?|p\.?m\.?)?)\s*(?:-|–|to)\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm|a\.?m\.?|p\.?m\.?)?)",
                seg, re.I,
            )
            if time_match:
                hours_text = f"{time_match.group(1).strip()} to {time_match.group(2).strip()}"
            else:
                continue  # no parseable hours and not closed → skip
        for idx in day_indices:
            result[_WEEKDAYS[idx]] = hours_text
    return result


def _detect_day_in_message(msg: str) -> Optional[str]:
    """Return the weekday name the customer is asking about (e.g. 'Monday'),
    or None if the question is general."""
    if not msg:
        return None
    s = msg.lower()
    today = _now_local()
    if re.search(r"\btomorrow\b", s):
        return (today + timedelta(days=1)).strftime("%A")
    if re.search(r"\b(today|tonight|right now|now)\b", s):
        return today.strftime("%A")
    # Specific named day - support "Monday", "next Monday", "this Friday", etc.
    for raw, canonical in _DAY_ALIASES.items():
        if re.search(rf"\b{raw}\b", s):
            return canonical
    return None


def _hours_response_for_day(hours_str: str, asked_day: str) -> Optional[str]:
    """Return a tailored response for a specific day, or None if we can't
    parse hours for that day."""
    parsed = _parse_hours_string(hours_str)
    status = parsed.get(asked_day)
    if status is None:
        return None
    today_name = _now_local().strftime("%A")
    tomorrow_name = (_now_local() + timedelta(days=1)).strftime("%A")
    if asked_day == today_name:
        day_phrase = f"today ({asked_day})"
    elif asked_day == tomorrow_name:
        day_phrase = f"tomorrow ({asked_day})"
    else:
        day_phrase = asked_day
    if status == "closed":
        return f"We're closed {day_phrase}."
    return f"Yes, we're open {day_phrase} from {status}."


def _dealer_info_response(dealer: Dict[str, Any], dealer_phone: str, msg: str = "") -> str:
    msg_lower    = (msg or "").lower()
    dealer_name  = get_row_field(dealer, DEALER_NAME_ALIASES) or "the dealership"
    dealer_phone = normalize_phone(dealer_phone)

    asks_address   = bool(re.search(r"\b(address|location|where|located|directions)\b", msg_lower))
    asks_hours     = bool(re.search(r"\b(hour|hours|open|close|operation)\b", msg_lower))
    asks_financing = bool(re.search(r"\b(financ\w*)\b", msg_lower))
    asks_tradeins  = bool(re.search(r"\b(trade[- ]?in)\b", msg_lower))
    asks_policy    = bool(re.search(r"\b(polic|rules|restrictions)\b", msg_lower))
    matched_count  = sum([asks_address, asks_hours, asks_financing, asks_tradeins, asks_policy])

    # Single-topic question - preserve the original concise response.
    if matched_count == 1:
        if asks_address:
            return f"We are located at {get_row_field(dealer, DEALER_ADDRESS_ALIASES) or '(not listed)'}."
        if asks_hours:
            hours_str = get_row_field(dealer, DEALER_HOURS_ALIASES) or "(not listed)"
            asked_day = _detect_day_in_message(msg)
            if asked_day:
                day_response = _hours_response_for_day(hours_str, asked_day)
                if day_response:
                    return day_response
            return f"Our hours of operation are {hours_str}."
        if asks_financing:
            return f"Regarding financing: {get_row_field(dealer, DEALER_FINANCING_ALIASES) or '(not listed)'}."
        if asks_tradeins:
            return f"Regarding trade-ins: {get_row_field(dealer, DEALER_TRADEINS_ALIASES) or '(not listed)'}."
        if asks_policy:
            return f"Our dealership policy: {get_row_field(dealer, DEALER_POLICIES_ALIASES) or '(none listed)'}."

    # Multi-topic question - answer every part the customer asked about.
    if matched_count >= 2:
        parts: List[str] = []
        if asks_address:
            parts.append(f"we're located at {get_row_field(dealer, DEALER_ADDRESS_ALIASES) or '(not listed)'}")
        if asks_hours:
            hours_str = get_row_field(dealer, DEALER_HOURS_ALIASES) or "(not listed)"
            asked_day = _detect_day_in_message(msg)
            day_response = _hours_response_for_day(hours_str, asked_day) if asked_day else None
            if day_response:
                # Strip the leading "We're closed " / "Yes, we're open " for fragment use
                fragment = re.sub(r"^(yes,?\s*)?we'?re\s+", "", day_response, flags=re.I).rstrip(".")
                parts.append(fragment)
            else:
                parts.append(f"our hours of operation are {hours_str}")
        if asks_financing:
            parts.append(f"regarding financing, {get_row_field(dealer, DEALER_FINANCING_ALIASES) or '(not listed)'}")
        if asks_tradeins:
            parts.append(f"on trade-ins, {get_row_field(dealer, DEALER_TRADEINS_ALIASES) or '(not listed)'}")
        if asks_policy:
            parts.append(f"as for our policies, {get_row_field(dealer, DEALER_POLICIES_ALIASES) or '(none listed)'}")
        # Capitalize the first segment.
        parts[0] = parts[0][0].upper() + parts[0][1:]
        if len(parts) == 2:
            return f"{parts[0]}, and {parts[1]}."
        return ", ".join(parts[:-1]) + f", and {parts[-1]}."

    # No specific topic matched - return the catch-all overview.
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

def _is_have_any_question(msg: str) -> Optional[str]:
    """Detect 'do you have any X' / 'got any X' / 'are there any X' style
    queries and return the search term X (lowercased). Returns None for
    generic terms like 'cars'/'vehicles' (those go through the listing
    handlers) or when the pattern doesn't match.

    Used to deterministically search the full inventory text for sub-brand
    or trim words like AMG / TRD / SRT — words that live inside the model
    or description string and that the LLM tends to miss when scanning
    inventory by year/make/model alone."""
    msg = (msg or "").strip().lower()
    msg = re.sub(r"[?.!,]+$", "", msg).strip()
    if not msg:
        return None
    patterns = [
        r"^(?:do\s+(?:you|ya|y'?all)|d'?ya)\s+(?:have|got|carry)\s+any\s+(.+)$",
        r"^(?:got|have|carrying|carry)\s+any\s+(.+)$",
        r"^(?:are\s+there|is\s+there)\s+any\s+(.+)$",
        r"^any\s+(.+?)\s+(?:in\s+stock|available|left|on\s+the\s+lot|on\s+(?:your|the)\s+lot)$",
    ]
    for pat in patterns:
        m = re.match(pat, msg)
        if not m:
            continue
        term = m.group(1).strip()
        term = re.sub(
            r"\s+(in\s+stock|available|left|here|on\s+the\s+lot|on\s+(?:your|the)\s+lot|right\s+now|currently)$",
            "",
            term,
        ).strip()
        # Generic listing terms — let the existing listing/inventory handlers
        # deal with these (price filters, generic browse, etc).
        generic = {
            "cars", "car", "vehicles", "vehicle", "trucks", "truck",
            "suvs", "suv", "sedans", "sedan", "vans", "van",
            "stuff", "options", "rides", "wheels", "things",
        }
        if not term or term in generic:
            return None
        return term
    return None


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
    """Customer asking for the listing URL — or for photos — of a specific
    vehicle. Photo requests are routed here because the listing URL is where
    photos live; we can't send images directly over SMS reliably. Uses loose
    word matching (pic\\w*, photo\\w*) so common typos like 'picures' or
    'photoes' still route to the photo handler instead of falling to the LLM.

    NOTE: Avoid catching "come see it" / "come see them" — those mean visit
    in person, not view the listing. Photo intent uses explicit photo words
    or "show me [photos/pics/it]" phrasing only."""
    return bool(re.search(
        r"\b(link|url|web\s*page|webpage|web\s*link|"
        r"pic\w*|photo\w*|image\w*|gallery|"
        r"show\s+(?:me\s+)?(?:it|them|some|pictures?|photos?|pics?|images?))\b",
        (msg or "").lower(),
    ))


def _is_vehicle_photo_question(msg):
    """Subset of link-question: customer specifically asked for photos/pictures.
    Used so the response can say 'photos' instead of 'listing'. Loose word
    matching catches typos."""
    return bool(re.search(
        r"\b(pic\w*|photo\w*|image\w*|gallery)\b",
        (msg or "").lower(),
    ))

def _is_carfax_question(msg):
    """Customer asking for a CarFax / vehicle history report OR any history-
    related question that a CarFax report would answer: accidents, prior
    owners, ownership history, service records. Routed to a deterministic
    handler so dealers whose scraped inventory carries CarFax URLs (Gov Auto
    Sales, Auto Galaxy Sales, United Automotive — DealerCenter / DCS) can
    hand them out directly. Dealers without CarFax URLs on file fall through
    to the inspection-clause fallback (which is dealer-aware via
    `_dealer_uses_inspection_clause`)."""
    return bool(re.search(
        r"\b(carfax|car\s*fax|autocheck|auto\s*check|"
        r"vehicle\s*history(?:\s*report)?|history\s*report|accident\s*report|"
        r"accidents?|wrecks?|wrecked|"
        r"owners?|ownership|"
        r"service\s+records?|service\s+history|maintenance\s+records?|"
        r"clean\s+history|clean\s+title)\b",
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


def _is_cash_payment_question(msg):
    """Detect 'can I pay cash', 'paying with cash', 'cash only' etc. The LLM
    was hallucinating alternative payment methods (certified check / wire
    transfer) when asked about cash — a deterministic answer prevents that."""
    return bool(re.search(
        r"\b(pay\s+(?:in\s+|with\s+)?cash|paying\s+(?:in\s+|with\s+)?cash|"
        r"with\s+cash|in\s+cash|cash\s+payment|cash\s+only|"
        r"accept\s+cash|take\s+cash|cash\s+(?:ok|okay|fine|good))\b",
        (msg or "").lower(),
    ))


def _is_price_breakdown_question(msg):
    """Direct price questions that should get a full breakdown (Internet → Doc → Purchase).
    Skips budget-filter queries like 'under 20k' or 'between 10k and 15k' — those are listings."""
    m = (msg or "").lower()
    if not m:
        return False
    # Listing/filter language → not a single-vehicle price question
    if re.search(r"\b(under|less\s+than|below|cheaper\s+than|over|more\s+than|above|between|"
                 r"max(?:imum)?|min(?:imum)?|up\s+to|no\s+more\s+than|at\s+least)\b\s*\$?\d", m):
        return False
    return bool(re.search(
        r"(\bdoc(?:ument(?:ary)?)?\s*fee\b|"
        r"\bout[\s-]?the[\s-]?door\b|\bo\.?t\.?d\.?\b|\bdrive[\s-]?away\b|"
        r"\btotal\s*(?:price|cost|amount)\b|\ball[\s-]?in(?:clusive)?\b|"
        r"\bwith\s*(?:all\s*)?fees?\b|\bwhat\s*fees?\b|"
        r"\b(?:any|other|extra|additional|more)\s+(?:\w+\s+){0,2}(?:fees?|costs?|charges?)\b|"
        r"\bare\s+there\s+(?:any\s+)?(?:other\s+|extra\s+|additional\s+|hidden\s+)?(?:fees?|costs?|charges?)\b|"
        r"\bprice\s*breakdown\b|\bbreak\s*(?:it|the\s*price)?\s*down\b|"
        r"\bwhat(?:'s|\s+is|\s+would\s+be|\s+would\s+the)\s+(?:the\s+)?(?:price|cost|final\s+price|total)\b|"
        r"\bprice\s+of\s+(?:the|that|this|it)\b|"
        r"\bwhat(?:'?s|\s+is|\s+does)\s+\S+(?:\s+\S+){0,3}\s+cost\b|"
        r"\bhow\s+much\s+(?:is|are|does|for|costs?|would|will|total|do\s+you\s+want)\b|"
        r"\bis\s+(?:that|this|it|\$?\s*[\d,]+(?:\.\d{1,2})?(?:k|K)?)\s+(?:the\s+)?(?:final|total|full)(?:\s+price|\s+cost)?\b|"
        r"\bare\s+(?:those|these|they)\s+(?:the\s+)?(?:final|total|full)(?:\s+prices?|\s+costs?)?\b|"
        r"\bis\s+that\s+(?:the\s+total|the\s+full\s+price)\b|"
        r"\b(?:final|full)\s+prices?\b|"
        r"\banything\s+(?:else\s+)?(?:on\s+top|added|additional)\b)",
        m,
    ))


def _fmt_money(amount: float) -> str:
    """$2,800 if integer, $37.50 if fractional."""
    if amount == int(amount):
        return f"${int(amount):,}"
    return f"${amount:,.2f}"


def _format_price_breakdown(match: Dict[str, Any], fees: Dict[str, float]) -> Optional[str]:
    """Render a 3-line price breakdown (Internet → Doc → Purchase) plus title/tag note.
    Returns None if the dealer has no doc fee configured — caller should fall through."""
    internet_price = _row_price_int(match)
    if internet_price <= 0:
        return None
    doc_fee = fees.get("doc_fee", 0.0) or 0.0
    title_tag = fees.get("title_tag_fee", 0.0) or 0.0
    if doc_fee <= 0:
        return None
    purchase_price = internet_price + doc_fee
    year  = str(match.get("Year",  "")).strip()
    make  = str(match.get("Make",  "")).strip()
    model = str(match.get("Model", "")).strip()
    title = " ".join(s for s in [year, make, model] if s)
    lines = [
        f"Here's the price breakdown for the {title}:",
        f"- Internet Price: {_fmt_money(internet_price)}",
        f"- Doc Fee: +{_fmt_money(doc_fee)}",
        f"- Full Price: {_fmt_money(purchase_price)}",
    ]
    if title_tag > 0:
        lines.append("")
        lines.append(f"(plus {_fmt_money(title_tag)} title and tag processing)")
    return "\n".join(lines)


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


def _send_email(to: str, subject: str, body: str) -> Tuple[bool, str]:
    to = normalize_email(to)
    if not to:
        return False, "Missing recipient email"
    if not (GMAIL_USER and GMAIL_APP_PASSWORD):
        return False, "Missing Gmail credentials"
    try:
        msg = EmailMessage()
        msg["From"] = NOTIFY_FROM_EMAIL or GMAIL_USER
        msg["To"] = to
        msg["Subject"] = (subject or "Notification")[:200]
        msg.set_content(body)
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as s:
            s.starttls()
            s.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            s.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def notify_all_staff(dealer_row: Dict[str, Any], from_number: str, body: str) -> None:
    # Demo dealer: log the would-be alert but suppress real SMS/email. Lets
    # dealer-prospects walk through the booking flow on the demo widget
    # without real notifications going out.
    if _is_demo_twilio(from_number):
        app.logger.info("Demo dealer: suppressing staff notification. Body:\n%s", body)
        return
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

    # Same merge for emails
    dealer_em = get_dealer_email(dealer_row)
    salesman_emails = get_salesman_emails(dealer_row)
    emails: List[str] = []
    seen_em = set()
    for raw in ([dealer_em] if dealer_em else []) + salesman_emails:
        ne = normalize_email(raw)
        if ne and ne.lower() not in seen_em:
            seen_em.add(ne.lower())
            emails.append(ne)

    if not phones and not emails:
        app.logger.warning("No notification phones or emails found for %s - skipping",
                           get_row_field(dealer_row, DEALER_NAME_ALIASES))
        return

    if phones:
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

    if emails:
        # Subject = first non-empty line of body, body unchanged
        subject = next((ln.strip() for ln in body.splitlines() if ln.strip()),
                       "Dealership notification")
        dealer_name = get_row_field(dealer_row, DEALER_NAME_ALIASES) or "Dealership"
        subject = f"[{dealer_name}] {subject}"
        app.logger.info("Notifying %d email(s): %s", len(emails), emails)
        for email in emails:
            ok, err = _send_email(email, subject, body)
            if ok:
                app.logger.info("Staff emailed: %s", email)
            else:
                app.logger.warning("Staff email failed for %s: %s", email, err)


def send_sms_to_customer(*, customer_phone: str, from_number: str, body: str) -> Tuple[bool, str]:
    return _send_sms(customer_phone, from_number, body)


def find_widget_session_for_real_phone(real_phone: str, twilio_number: str) -> str:
    """Reverse lookup: given a real phone number that texted in via SMS,
    find the matching +web<sessionid> pseudo-phone if this customer ever
    used the widget. Lets SMS replies (e.g. reschedule/cancel) reach
    appointments that were originally booked through the widget."""
    rp = normalize_phone(real_phone or "")
    if not rp:
        return ""
    conn = _db()
    try:
        row = conn.execute(
            "SELECT customer_phone FROM customer_names "
            "WHERE twilio_number=? AND real_phone=? AND customer_phone LIKE '+web%' "
            "ORDER BY rowid DESC LIMIT 1",
            (twilio_number, rp),
        ).fetchone()
    except Exception as e:
        app.logger.warning("find_widget_session_for_real_phone failed: %s", e)
        return ""
    finally:
        conn.close()
    return row["customer_phone"] if row else ""


def resolve_outbound_customer_phone(customer_phone: str, twilio_number: str) -> str:
    """Return the phone number we can actually text the customer at.

    For SMS customers, customer_phone IS their real phone, so it works as-is.
    For widget customers, customer_phone is a "+web<sessionid>" pseudo-phone
    that Twilio can't reach - we fall back to real_phone from their profile,
    which is collected when they open the widget."""
    cp = (customer_phone or "").strip()
    if cp.startswith("+web"):
        return get_customer_profile(cp, twilio_number).get("real_phone", "")
    return cp


def build_customer_confirmation_body(*, dealer_name: str, customer_name: str,
                                     visit_time: str, car_desc: str,
                                     dealer_address: str = "",
                                     dealer_phone: str = "") -> str:
    """Friendly customer-facing appointment confirmation SMS. Distinct from
    the operational dealer/salesman alert, which has different info."""
    name_part = f"Hi {customer_name}! " if customer_name else "Hi! "
    is_general = (car_desc or "").strip().lower() in {"", "general visit", "general", "visit"}
    if is_general:
        body = (
            f"{name_part}Your appointment with {dealer_name} is confirmed for "
            f"{visit_time}."
        )
    else:
        body = (
            f"{name_part}Your appointment with {dealer_name} is confirmed for "
            f"{visit_time} to see the {car_desc}."
        )
    if dealer_address:
        body += f"\n\nAddress: {dealer_address}"
    if dealer_phone:
        body += f"\nQuestions? Call us at {dealer_phone}."
    body += "\n\nWe look forward to seeing you!"
    return body


def notify_customer_appointment(dealer_row: Dict[str, Any], *, customer_phone: str,
                                twilio_number: str, customer_name: str,
                                visit_time: str, car_desc: str,
                                action: str = "confirmed") -> None:
    """Send a friendly appointment confirmation/reschedule/cancellation text
    to the CUSTOMER. Silently no-ops if we don't have a real phone for them
    (e.g., widget customer who somehow skipped phone collection), OR if the
    current request came in via /sms (the bot's TwiML reply already reaches
    the customer's phone, so a separate SMS would duplicate)."""
    if _is_demo_twilio(twilio_number):
        app.logger.info("Demo dealer: suppressing customer appointment %s SMS", action)
        return
    if g.get("is_sms_request"):
        app.logger.info("notify_customer_appointment: skipping for SMS-origin request "
                        "(TwiML reply already covers it)")
        return
    to_phone = resolve_outbound_customer_phone(customer_phone, twilio_number)
    if not to_phone:
        app.logger.info("notify_customer_appointment: no real phone on file for %s, skipping",
                        customer_phone)
        return
    dealer_name = get_row_field(dealer_row, DEALER_NAME_ALIASES) or "the dealership"
    dealer_address = get_row_field(dealer_row, DEALER_ADDRESS_ALIASES)
    dealer_phone = normalize_phone(get_row_field(dealer_row, DEALER_NOTIFY_PHONE_ALIASES))
    if action == "rescheduled":
        is_general = (car_desc or "").strip().lower() in {"", "general visit", "general", "visit"}
        body = (
            f"Hi {customer_name}! Your appointment with {dealer_name} has been "
            f"rescheduled to {visit_time}."
            if is_general else
            f"Hi {customer_name}! Your appointment with {dealer_name} has been "
            f"rescheduled to {visit_time} to see the {car_desc}."
        )
        if dealer_address: body += f"\n\nAddress: {dealer_address}"
        if dealer_phone:   body += f"\nQuestions? Call us at {dealer_phone}."
        body += "\n\nSee you then!"
    elif action == "cancelled":
        is_general = (car_desc or "").strip().lower() in {"", "general visit", "general", "visit"}
        body = (
            f"Hi {customer_name}, your appointment with {dealer_name} for "
            f"{visit_time} has been cancelled."
            if is_general else
            f"Hi {customer_name}, your appointment with {dealer_name} for "
            f"{visit_time} to see the {car_desc} has been cancelled."
        )
        if dealer_phone: body += f"\nWant to reschedule? Call us at {dealer_phone}."
    else:
        body = build_customer_confirmation_body(
            dealer_name=dealer_name, customer_name=customer_name,
            visit_time=visit_time, car_desc=car_desc,
            dealer_address=dealer_address, dealer_phone=dealer_phone,
        )
    ok, err = _send_sms(to_phone, twilio_number, body)
    if ok:
        app.logger.info("Customer appointment %s SMS sent to %s", action, to_phone)
    else:
        app.logger.warning("Customer appointment %s SMS failed for %s: %s",
                            action, to_phone, err)


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


def ai_vehicle_detail_reply(customer_msg, vehicle_data, dealer_phone, history, twilio_number: str = "", dealer_row: Optional[Dict[str, Any]] = None):
    history_snippet = " ".join((m.get("content") or "") for m in history[-4:])
    dealer_phone_clean = normalize_phone(dealer_phone or "")
    phone_rule = (
        f"Only mention the dealership phone number ({dealer_phone_clean}) if the customer's question genuinely cannot be answered from the vehicle data above. "
        f"If you DID answer their question, do NOT add 'feel free to call' or 'contact us at...' - the chat IS the contact channel. "
        f"Never invent a phone number. If you do mention the phone, it must be exactly {dealer_phone_clean}."
        if dealer_phone_clean
        else "Do NOT include any phone number in your reply (we don't have one on file). "
             "Suggest the customer reach out via this chat or schedule a visit instead."
    )
    if _dealer_uses_inspection_clause(twilio_number, dealer_row=dealer_row):
        history_issues_rule = (
            "History / issues rule (REQUIRED — follow exactly):\n"
            "- If the customer asks about history or known issues (accidents, prior owners, service records, repair history, problems, issues, anything wrong, condition, defects, clean history, clean carfax, or anything similar) AND the vehicle data has none listed: your reply MUST contain BOTH of these in this order:\n"
            "  1. Acknowledge nothing is listed for the vehicle.\n"
            "  2. Add the EXACT phrase \"but every car on our lot is thoroughly inspected before being listed\" (use \"but\" — not \"and\").\n"
            "- Then offer to go over the details in person. Use \"car\" not \"vehicle\" in the inspection clause.\n"
            "- Example (the inspection sentence is NOT optional — include it verbatim): \"There aren't any accidents or issues listed for the 2023 Toyota Camry Se, but every car on our lot is thoroughly inspected before being listed. We'd be happy to go over the details in person — would you like to set up a time?\"\n"
            "- IMPORTANT: Only say the \"every car on our lot is thoroughly inspected\" line ONCE per conversation. If the Recent conversation above already shows you've used that exact line in a previous reply, DO NOT repeat it — just briefly acknowledge nothing is listed for the new question (e.g. \"No accidents listed either.\") and move on. On the FIRST history/issue question in a conversation, the inspection line IS required — do not skip it."
        )
    else:
        history_issues_rule = (
            "History / issues rule (REQUIRED — follow exactly):\n"
            "- If the customer asks about history or known issues (accidents, prior owners, service records, repair history, problems, issues, anything wrong, condition, defects, clean history, clean carfax, or anything similar) AND the vehicle data has none listed: acknowledge that nothing is listed for the vehicle, then offer to have the dealer team walk through what they know about it in person.\n"
            "- DO NOT claim the car was inspected, certified, vetted, or reconditioned. DO NOT use the phrase \"every car on our lot is thoroughly inspected\" — we don't make that claim for this dealer. Just say nothing's listed and pivot to scheduling a visit.\n"
            "- Example: \"There aren't any accidents or issues listed for the 2023 Toyota Camry Se — the dealer team can walk you through anything they know about it in person. Would you like to set up a time to come see it?\""
        )
    prompt = f"""You are a professional automotive sales consultant responding via SMS.

A customer asked: "{customer_msg}"

Vehicle data (use ONLY this - do not guess):
{vehicle_data}

Recent conversation: {history_snippet or "(none)"}

Write one natural, conversational SMS reply. 1-3 sentences. No bullet points. Do not reference spreadsheets or databases. Your job is to ANSWER questions in this chat - the customer chose to chat instead of call. Don't push them to call when you've already answered them.

Phone number rule:
- {phone_rule}

CarFax rules:
- If the vehicle data above contains a "CarFax report:" line with a URL, you MAY mention CarFax and you MUST include the URL in your reply (do not make the customer ask for it separately).
- If no CarFax URL is present in the vehicle data, do NOT mention or recommend CarFax at all. Either answer with the info you do have, or briefly say you don't have that detail and suggest contacting the dealership.

{history_issues_rule}""".strip()
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # If the LLM included a CarFax URL we already sent in this convo,
        # collapse it to a reference instead of repeating the 142-char URL.
        return _dedupe_carfax_in_reply(raw, history)
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
            "FORBIDDEN: NEVER ask the customer for their credit score, credit history, SSN, date of birth, income, or any other financial/credit information over chat. The dealer collects that through a secure credit application, not in this conversation. "
            "Reply in EXACTLY two short sentences in this order:\n"
            "  Sentence 1 - one line summarizing financing using the policy text above. If the policy text contains a URL, include it as a plain URL (e.g. 'You can apply at https://example.com/apply'). NEVER use markdown link syntax like [text](url) - just paste the raw URL. If there is no URL in the policy text, say 'You can apply online or in person at the dealership.'\n"
            "  Sentence 2 - a direct booking question, e.g. 'What time today or tomorrow works for you to come in and go over the options?'\n"
            "Do NOT add a third sentence. Do NOT use bullet points. Do NOT use markdown brackets `[` or `]` anywhere in the reply. "
            "FORBIDDEN: do not say 'stop by', 'feel free to visit', 'whenever you're ready', 'when you have time', or similar passive invitations - always ask for a specific time."
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
            "If the customer has NOT shared their trade-in vehicle details yet, your reply MUST ask for them so the dealer can have a ballpark range in mind before the visit (a firm number always requires an in-person inspection - never imply otherwise). Specifically ask for: year, make, model, mileage, title status (clean/salvage/rebuilt), and overall condition. Ask for whichever pieces are still missing - if the customer has already shared some details (check the conversation above), only ask for the rest. Do NOT answer the trade-in question without asking for the missing details first. "
            "If they HAVE already provided ALL the trade-in details, you MUST briefly acknowledge them in ONE short sentence, then ASK FOR A SPECIFIC TIME to schedule a visit. Do NOT use vague phrases like 'stop by anytime' or 'feel free to come in' - those are passive and lose appointments. "
            "Use a direct booking question like: 'What time works best for you to come in tomorrow or this week?' or 'Would you like to come in today or tomorrow to take a look?' - phrased so the customer's natural reply is a time/day. "
            "FORBIDDEN: do not include any URL, web link, or phone number in your reply - even if the policy text contains one - UNLESS the customer's latest message explicitly asks for a link, URL, website, or application form. "
            "FORBIDDEN: do not say 'stop by', 'feel free to visit', 'whenever you're ready', 'when you have time', or similar passive invitations - always ask for a specific time."
        )

    name_block = (
        f"Customer's first name: {customer_name}. You may address them by this name naturally."
        if customer_name else
        "You do NOT know the customer's name. Do NOT invent one, do NOT use a single letter or initial, and do NOT use any placeholder like 'Hi there' followed by a stray character. Just start the reply without a name (e.g., 'Sure - we offer...')."
    )

    dealer_phone_clean = normalize_phone(dealer_phone or "")
    if dealer_phone_clean:
        phone_rule = (
            f"If you reference a phone number in your reply, you MUST use this exact number: {dealer_phone_clean}. "
            f"Do NOT invent, guess, or make up any other phone number under any circumstances."
        )
    else:
        phone_rule = (
            "Do NOT include any phone number in your reply (we don't have one on file). "
            "Suggest the customer continue here in chat or schedule a visit instead."
        )

    prompt = f"""You are a professional automotive sales consultant responding via SMS.

The customer asked about {topic}. Here is the dealership's {topic} policy:
{policy_text}

Dealer phone (only share if truly needed): {dealer_phone_clean or "(not listed)"}

{name_block}

Recent conversation:
{chr(10).join(convo_lines) or "(none)"}

Customer's latest message: {customer_msg}

Instructions:
- Answer naturally using the policy text above. Keep it to 2-3 sentences.
- {gather_instruction}
- Do not repeat information already covered in the conversation.
- Do not invent details not in the policy.
- {phone_rule}""".strip()

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

    # Strip URLs and phone numbers unless the customer explicitly asked for one,
    # OR the topic is financing (where we WANT to share the credit application URL).
    customer_asked_for_link = bool(re.search(
        r"\b(link|url|website|web\s*site|web\s*page|webpage|application\s*form|"
        r"apply\s*online|where\s*do\s*i\s*apply|send\s*me\s*the|page|site|"
        r"phone\s*number|number\s*to\s*call|who\s*do\s*i\s*call)\b",
        (customer_msg or "").lower(),
    ))
    if not customer_asked_for_link and topic != "financing":
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


_CONDITION_TERMS_RE = re.compile(
    r"\b(excellent|great|good|decent|fair|rough|poor|nice|solid|"
    r"like\s+new|mint|pristine|beat[\s-]?up|"
    r"rust|dent|damage|scratch|"
    r"runs\s+well|drives\s+well|condition|shape)\b",
    re.I,
)

_CONDITION_PHRASE_RE = re.compile(
    r"(?:its?\s+|it'?s\s+|in\s+|runs\s+|drives\s+)?"
    r"(?:excellent|great|good|decent|fair|rough|poor|nice|solid|"
    r"like\s+new|mint|pristine|beat[\s-]?up)"
    r"(?:\s+(?:condition|shape))?"
    r"(?:\s+(?:but|with|and)\s+[^.!?\n]{0,80})?",
    re.I,
)


def _augment_trade_in_with_condition(summary: str, history: List[Dict[str, Any]]) -> str:
    """If the trade-in `summary` lacks condition info but the customer
    mentioned condition in their messages, append the condition clause.
    extract_trade_in_vehicle (LLM-based) has been observed to drop the
    condition when the customer phrases it conversationally — e.g. "its in
    good condition but has a little bit of rust" → summary stays as
    "2012 Nissan Altima, 200k mi, clean title" with no condition, so the
    dealer alert misses what the customer said."""
    if not summary:
        return summary
    if _CONDITION_TERMS_RE.search(summary):
        return summary  # already has condition info
    user_text = " ".join(
        (m.get("content") or "").strip()
        for m in (history or []) if m.get("role") == "user"
    )
    if not user_text:
        return summary
    m = _CONDITION_PHRASE_RE.search(user_text)
    if not m:
        return summary
    phrase = m.group(0).strip().rstrip(".,!?")
    # Strip leading filler so it reads cleanly after a comma
    phrase = re.sub(r"^(?:its?\s+|it'?s\s+|in\s+)", "", phrase, flags=re.I).strip()
    if not phrase or len(phrase) > 120:
        return summary
    return f"{summary}, {phrase}"


_KNOWN_TRADE_IN_MAKES = {
    "toyota", "honda", "ford", "chevy", "chevrolet", "nissan", "hyundai",
    "kia", "subaru", "mazda", "bmw", "mercedes", "mercedes-benz", "audi",
    "volkswagen", "vw", "lexus", "acura", "infiniti", "buick", "cadillac",
    "gmc", "ram", "dodge", "jeep", "chrysler", "lincoln", "volvo",
    "mitsubishi", "porsche", "tesla", "land rover", "range rover", "mini",
    "fiat", "smart", "scion", "saturn", "saab", "pontiac", "oldsmobile",
    "mercury", "plymouth", "isuzu", "suzuki", "jaguar", "bentley",
    "rolls-royce", "ferrari", "lamborghini", "maserati", "alfa romeo",
    "genesis", "polestar", "lucid", "rivian", "harley-davidson", "harley",
}


def _trade_in_missing_parts(history: List[Dict[str, Any]]) -> List[str]:
    """Return list of trade-in details the customer hasn't shared yet.
    Possible values: 'year', 'make and model', 'mileage',
    'title status (clean/salvage/rebuilt)', 'overall condition'."""
    text = " ".join(
        (m.get("content") or "").lower()
        for m in history if m.get("role") == "user"
    )
    has_year = bool(re.search(r"\b(19[5-9]\d|20[0-2]\d)\b", text))
    # If the customer has named any common car make in this conversation,
    # treat make+model as collected (model usually follows naturally).
    has_make = any(
        re.search(rf"\b{re.escape(m)}\b", text) for m in _KNOWN_TRADE_IN_MAKES
    )
    has_mileage = bool(re.search(
        r"(\d{1,3}[\s,]?\d{3}|\d{1,3}\s*k)\s*(mi\b|miles?\b)|"
        r"\b\d{2,3}\s*k\b",
        text
    ))
    has_title = bool(re.search(
        r"\b(clean|salvage|rebuilt|lien|branded|junk|rebuilt salvage)\s+title\b|"
        r"\btitle\s+is\s+(clean|salvage|rebuilt|branded)\b",
        text
    ))
    has_condition = bool(re.search(
        r"\b(excellent|great|good|decent|fair|rough|poor|bad|beat[\s-]?up|"
        r"like\s+new|mint|pristine|nice|perfect|solid|"
        r"clean(?!\s+title)(?!,\s*and\s+\w+\s+title)|"  # "clean" but NOT "clean title"
        r"pretty\s+(clean|nice|good|solid|decent)|"
        r"in\s+(excellent|great|good|decent|fair|rough|poor|nice|solid)\s+(shape|condition)|"
        r"runs\s+(well|fine|great|good|smooth|strong)|drives\s+(well|fine|great|good|smooth|straight)|"
        r"needs\s+(work|repairs?|tlc)|some\s+(rust|damage|dents?)|"
        r"no\s+(rust|damage|dents?|issues|problems|major\s+issues|major\s+problems)|"
        r"a\s+little\s+(rough|beat))\b",
        text
    ))
    missing = []
    if not has_year:      missing.append("year")
    if not has_make:      missing.append("make and model")
    if not has_mileage:   missing.append("mileage")
    if not has_title:     missing.append("title status (clean/salvage/rebuilt)")
    if not has_condition: missing.append("overall condition")
    return missing


def _format_missing_list(items: List[str]) -> str:
    """English-style join: ['a','b','c'] -> 'a, b, and c'."""
    if not items: return ""
    if len(items) == 1: return items[0]
    if len(items) == 2: return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


def deterministic_trade_in_followup(summary: str, history: List[Dict[str, Any]],
                                    confirmed_appt: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Build a fixed-template trade-in reply based on what the customer has
    shared. Always returns a response when the conversation is in trade-in
    flow - either asking for missing pieces or pivoting to scheduling.

    If the customer ALREADY has a confirmed appointment, tie the trade-in
    to that existing visit instead of asking for a new visit time."""
    missing = _trade_in_missing_parts(history)
    if missing:
        return (
            f"Thanks for sharing that. To round out the ballpark, "
            f"could you also share the {_format_missing_list(missing)}?"
        )
    summary_text = summary or "your trade-in"
    if confirmed_appt and confirmed_appt.get("visit_time"):
        # Already booked - the trade-in will be appraised at the existing visit.
        return (
            f"Got it - {summary_text}. We'll add it to your appointment at "
            f"{confirmed_appt['visit_time']} - the dealer will give you a firm "
            f"number when they take a look in person then. See you then!"
        )
    # No appointment yet - pivot directly to scheduling, no passive language.
    return (
        f"Got it - {summary_text}. The dealer will give you a firm number once "
        f"they can take a look in person. What day this week works for you "
        f"to bring it in for the appraisal?"
    )


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

Write a single short follow-up SMS (1-2 sentences). Reference what they were asking about if possible. Be warm but professional. {closing_instruction} Do not mention they went silent. Do NOT include any phone number, address, or URL in your reply - keep the customer in this conversation.""".strip()
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

_SERVICE_INTENT_RE = re.compile(
    r"\b("
    r"oil\s+change|tire\s+(?:rotat|chang|replac)|brake\s+(?:work|pad|rotor|repair)|"
    r"engine\s+(?:repair|work|rebuild|trouble)|transmission\s+(?:repair|work|fluid|rebuild)|"
    r"electrical\s+(?:repair|work|issue|problem)|"
    r"body\s+(?:work|repair|damage)|"
    r"alignment|suspension\s+(?:work|repair)|"
    r"spark\s+plug|battery\s+(?:replac|chang)|"
    r"glass\s+(?:replac|repair)|windshield\s+(?:replac|repair|chip|crack)|"
    r"tune[-\s]?up|inspection|check\s+engine|diagnostic|"
    r"mechanical\s+work|repair\s+my\s+car|fix\s+my\s+car|"
    r"service\s+(?:my|the|on\s+my)\s+(?:car|vehicle)|"
    r"get\s+(?:my\s+)?(?:car|vehicle)\s+service"
    r")\b",
    re.I,
)


def _is_service_appointment_context(history: List[Dict[str, Any]], current_body: str = "") -> bool:
    """Detect whether the current booking conversation is for SERVICE work on
    the customer's own car (oil change, brake work, engine repair, etc.) vs.
    SALES — viewing/buying a car from the dealer's inventory. Used to skip the
    sales-specific STEP 1.5 (financing/trade-in) questions and the inspection
    clause for service appointments.

    Heuristic: look at the last ~8 messages (user + assistant) for explicit
    service-intent keywords. If the conversation has been about service work,
    we treat the booking as a service appointment."""
    blob = (current_body or "")
    for m in (history or [])[-8:]:
        blob += " " + (m.get("content") or "")
    return bool(_SERVICE_INTENT_RE.search(blob))


def build_prompt(dealer, inventory_rows, history, customer_msg, dealer_phone, confirmed_appt=None, customer_name=""):
    dealer_name  = get_row_field(dealer, DEALER_NAME_ALIASES) or "the dealership"
    dealer_twilio = normalize_phone(get_row_field(dealer, TWILIO_NUMBER_ALIASES))
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
    current_time_str = _now_local().strftime("%A, %B %d, %Y at %I:%M %p")

    if isinstance(customer_name, dict):
        first, last, email = customer_name.get("name", ""), customer_name.get("last_name", ""), customer_name.get("email", "")
        trade_in = customer_name.get("trade_in_vehicle", "")
        real_phone = customer_name.get("real_phone", "")
    else:
        first, last, email, trade_in, real_phone = (customer_name or ""), "", "", "", ""
    known_lines = []
    if first: known_lines.append(f"- First name: {first}")
    if last:  known_lines.append(f"- Last name: {last}")
    if email: known_lines.append(f"- Email: {email}")
    if real_phone: known_lines.append(f"- Phone (for follow-up texts): {real_phone}")
    if trade_in: known_lines.append(f"- Trade-in vehicle (NOT for sale, customer is trading it in): {trade_in}")
    # First name + phone are collected via the JS profile form before any LLM
    # call, so they are guaranteed to be on file here. Only email is asked
    # for during the booking flow (STEP 2). Last name is NEVER asked for -
    # it's optional metadata and must not block the booking.
    missing = []
    booking_missing = [label for val, label in
                       ((email, "email address"),) if not val]
    known_block = "Already collected:\n" + "\n".join(known_lines) if known_lines else "No customer details collected yet."
    if booking_missing:
        missing_block = (
            "When the customer is ACTIVELY booking a visit (not just browsing), the booking flow will need: "
            + ", ".join(booking_missing) + ". "
            "Do NOT ask for it unless STEP 2 of the booking flow is in progress. NEVER ask for last name - it is optional and must not block the booking."
        )
    else:
        missing_block = "All required customer details have been collected."
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
        "First name and phone are ALREADY ON FILE - never ask for them again. "
        "Email is asked inline during STEP 2 of the booking flow ONLY - never proactively while the customer is browsing. "
        "LAST NAME IS NEVER REQUIRED. Do not ask for it under any circumstance, including when confirming a booking."
        f"{trade_in_warning}"
    )

    if _dealer_uses_inspection_clause(dealer_twilio, dealer_row=dealer):
        history_issues_rule = (
            "- If a customer asks about a vehicle's history OR known issues (accidents, prior accidents, prior owners, service records, repair history, clean history, clean carfax, problems, issues, anything wrong, condition, defects, or anything similar) AND the data has none listed: do NOT say \"I don't have that information.\" Instead, your reply MUST do BOTH (in this order): (1) acknowledge nothing is listed for the vehicle; (2) include the EXACT phrase \"but every car on our lot is thoroughly inspected before being listed\". Use \"but\" (not \"and\") between the two clauses. Then offer to go over the details in person. Use \"car\" not \"vehicle\" in the inspection clause. Example phrasing: \"There aren't any accidents or issues listed for the 2023 Toyota Camry Se, but every car on our lot is thoroughly inspected before being listed. We'd be happy to go over the details in person — would you like to set up a time?\" IMPORTANT: only say the \"every car on our lot is thoroughly inspected\" line ONCE per conversation. If your prior replies in this conversation ALREADY used that exact line, DO NOT repeat it — just briefly acknowledge nothing is listed for the new question (e.g. \"No accidents listed either.\") and move on. On the FIRST history/issue question, the inspection line IS required — do not skip it."
        )
    else:
        history_issues_rule = (
            "- If a customer asks about a vehicle's history OR known issues (accidents, prior accidents, prior owners, service records, repair history, clean history, clean carfax, problems, issues, anything wrong, condition, defects, or anything similar) AND the data has none listed: acknowledge nothing is listed for the vehicle, then offer to have the dealer team walk through what they know about it in person. DO NOT claim the car was inspected, certified, vetted, or reconditioned. DO NOT use the phrase \"every car on our lot is thoroughly inspected\" — we do not make that claim for this dealer. Example: \"There aren't any accidents or issues listed for the 2023 Toyota Camry Se — the dealer team can walk you through anything they know about it in person. Would you like to set up a time to come see it?\""
        )

    return f"""You are Dave, a professional sales consultant representing {dealer_name}, communicating via SMS on behalf of the dealership. If a customer asks your name, you are Dave. Never say you are an AI, bot, assistant, or anything other than Dave - you are a person on the dealership's team.

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
- NEVER include URLs, hyperlinks, or markdown links in your reply. Do NOT type "https://", "www.", or "[text](link)". If a customer asks for a link to a vehicle's listing, simply say you'll send the listing — the system sends the real URL separately. Any URL you write is a hallucination because you do not have access to real URLs.
- If a customer asks for pictures, photos, pics, or images of a vehicle, treat it the same as a link request: say you'll send over the listing where they can see the photos — the system sends the real URL separately. Do NOT promise to send images, photos, or attachments directly, and do NOT say "the system will send pictures shortly" — only the listing URL is sent. Example: "Sure — I'll send over the listing for the 2016 Honda Odyssey where you can see all the photos."
- NEVER offer to "discuss the trade-in process," "walk through the trade-in process," "explain the trade-in process," or any variant. The trade-in flow is handled by the system, which collects vehicle details (year/make/model, mileage, title status, condition) and rolls them into the visit. When a customer mentions a trade, briefly acknowledge it and let the system continue — do not pitch a separate "process" conversation.
- When the customer uses words like "that price", "that price range", "that feature", "that one", "similar", "another like that" — they're referring to the SPECIFIC vehicle you mentioned in your IMMEDIATELY PREVIOUS reply, NOT some other vehicle from earlier history. If your last reply named the Prius at $4,769, "that price range" means around $4,769, not any other price seen earlier. Anchor every relative pronoun to your most recent reply.
- NEVER invent a phone number, address, or any fact not in the data.
- NEVER ask about monthly payment amounts.
- When the customer asks about service / repair / maintenance / detailing / tinting / add-ons, answer DIRECTLY using the Dealer Info "Notes/Policies" field above — that field lists what services the dealership actually offers. If the customer is asking generally ("I want service done"), ASK them what kind of service they need. If they ask about a specific service, confirm whether it's listed and what it covers — and if it's NOT explicitly listed but related services are, mention what IS available and pivot to scheduling. Do NOT share a phone number unless either: (a) the customer explicitly asks for pricing, a quote, or a phone number, OR (b) the policies field has literally zero relevant service info to share. When neither applies, end with a follow-up like "Would you like to schedule a visit for any of those?" instead of pushing a phone number. When you DO share a phone number, use the service phone if one is listed in the policies field; otherwise use {dealer_phone}. Phrasing for the pricing escalation: "For an accurate quote on that, I'd recommend giving them a call at <number>."
- Share VIN only if it appears in TOP MATCHING VEHICLE DETAILS below.
- NEVER offer to email details or promise anything outside this conversation.
- NEVER guess vehicle condition, history, or issues.
{history_issues_rule}
- NEVER use bullet points.
- NEVER ask the customer for their credit score, credit history, social security number, date of birth, monthly income, banking details, or any other sensitive financial information. If the customer brings up financing or credit, point them to the dealer's secure credit application (online if a URL is in the policy text, otherwise in person at the dealership) and ask for a time to come in - do NOT collect any of that info in chat.
- NEVER ask "what time works", "what time would work", "what day works", or any time/day question if the customer's latest message ALREADY contains a specific clock time for the visit (e.g. "tomorrow at 3pm", "Friday at 10am", "2pm today", "I can be there tomorrow at 2pm"). Treat the time they provided as the time and proceed to the next booking step (email if missing, otherwise the confirmation). This rule overrides any phrasing example below.
- NEVER say "You're booked", "I'll confirm next", "about to confirm", "confirmation coming", or any variant that suggests the confirmation will come in a separate / future reply. The confirmation MUST happen in the same reply where you say it is happening. If you cannot include META_JSON in this reply (because something is genuinely missing), then do NOT use confirmation language - ask for whatever is missing instead.
- NEVER write any of the following without including META_JSON in the SAME reply: "your appointment is confirmed", "appointment confirmed", "you're all set", "you're booked", "you are booked", "all set". Confirmation language without META_JSON is a silent booking failure - the dealer is never notified. If you write confirmation language, META_JSON MUST appear at the end of the same reply, exactly as specified in STEP 3.

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
The booking flow is STREAMLINED. Personal info is ONLY collected when the customer actually wants to book - never just because they expressed interest or said "yes" to a service question. NOTE: First name and phone are collected via a form before you ever see the conversation, so they are always already on file. Last name and email are still collected during the booking flow when needed. The booking flow proceeds in steps:

STEP 0 - Car of interest (only when NO specific vehicle has been discussed yet)
- Before asking about time, check whether the conversation has referenced a specific vehicle from our inventory (e.g. the customer asked about a specific year/make/model, or the consultant has shown them a particular car).
- If a specific vehicle is already in context: skip STEP 0 entirely. Use that vehicle as the car of interest.
- If NO specific vehicle has been discussed AND the customer asks to schedule a visit (e.g. "I'd like to come in", "can I schedule an appointment", "what time can I stop by"): point that out and ask if they're interested in a particular vehicle, in ONE message. Phrasing example (≤200 chars): "Of course! Just so I can have it ready - is there a specific vehicle you're interested in seeing, or is this more of a general visit?"
- If the customer names a vehicle: use that as the car of interest, proceed to STEP 1.
- If the customer says no / just looking / general visit / browsing / similar: the car of interest is "general visit". Proceed to STEP 1. In STEP 3 META_JSON, set car_desc to "general visit".
- Ask STEP 0 at most ONCE per booking attempt. Never re-ask if the customer has already given a yes/no answer.

STEP 1 - Get a specific clock time (NEVER ask for email here)
- When the customer wants to schedule/book a visit, ask ONLY for a specific clock time. Do NOT bundle the email request into this ask.
- Required: a SPECIFIC CLOCK TIME (e.g. "9am", "2:30pm"). A date alone ("tomorrow", "Friday") is NOT enough - if the customer gives only a date, ask for the clock time.
- Use the CURRENT DATE & TIME above to interpret words like "tomorrow" or "Friday afternoon".
- Phrasing examples (keep your reply ≤155 chars when possible):
  - Customer gave a date but no clock time: "Sure - what time tomorrow works for you?"
  - Customer gave nothing time-related: "Sure - what time works for you?"
- This applies even if hours are not listed; only reject a time if it clearly falls outside listed hours for that specific day.
- Once a SPECIFIC CLOCK TIME is established (either in the customer's latest message or earlier in the conversation), proceed to STEP 1.5 (if a specific vehicle was chosen) or STEP 2 (if general visit).

==== TIME REFERENCE RULE (READ FIRST — applies to every booking-flow reply) ====
Whenever you reference the visit time, you MUST write it as [DAY at TIME], NOT just [TIME]. The day and the clock time must always appear together. Examples of acceptable forms: "Saturday at 2 PM", "Monday at 4 PM", "tomorrow at 5 PM", "today at 3 PM", "Friday at 10 AM".

Bare clock times ("2 PM", "4 PM", "5 PM") with no day are FORBIDDEN. If you write a bare clock time you are creating a confirmation defect because the customer cannot tell which day you booked.

The day comes from the conversation, not just the latest user message. Scan the WHOLE recent conversation when forming your reply:
- If the customer said "Monday" earlier and "4pm" now → you say "Monday at 4 PM".
- If the customer said "Saturday at 4pm" → you say "Saturday at 4 PM".
- If you (the consultant) wrote "What time on Monday..." earlier and the customer answered "4pm" → you say "Monday at 4 PM".
- Only use a bare time if NO day has been mentioned anywhere in the conversation, in which case you must first ask the customer which day.

This rule overrides any example phrasing below that omits a day. When in doubt, include the day.

STEP 1.5 - Questions / trade-in / financing check (ONLY for SALES bookings on a specific vehicle from our inventory; SKIP for general visits AND for SERVICE appointments)
- SKIP this step entirely when the booking is for a SERVICE appointment on the customer's OWN car (oil change, brake work, engine repair, spark plugs, transmission work, body work, etc.). Service appointments do NOT involve financing or trade-ins. For service bookings, go DIRECTLY to STEP 2 (email if missing) or STEP 3 (confirm). The "car of interest" for service is the customer's own vehicle (e.g. "your 2008 Chevy Malibu"), NOT a unit from our inventory. Recognize service context from prior messages mentioning repairs, maintenance, "service my car", or specific service tasks.
- Trigger (sales path only): a specific clock time has been established AND the car of interest is a specific inventory vehicle (NOT "general visit", NOT a service appointment) AND STEP 1.5 has not been asked yet in this booking attempt.
- Ask in ONE message whether the customer has any other questions about the vehicle, a trade-in they'd like the dealer to look at, or if they're interested in financing. Phrasing example (≤220 chars): "Got it - 2pm tomorrow for the [year make model]. Any other questions about it, are you interested in financing, or do you have a trade-in you'd like us to take a look at?"
- If the customer responds with questions: answer them using the inventory data, then in the SAME reply ask the email question to advance to STEP 2 (e.g. "Yes - it has all-wheel drive and a panoramic roof. To lock in 2pm tomorrow, could I get your email?"). Don't loop back to STEP 1.5 again.
- If the customer mentions a trade-in:
   - If they ALREADY specified the trade-in vehicle (a year/make/model or at least make+model, e.g. "I have a 2018 honda accord", "yes, a Tacoma"): acknowledge it briefly and ask the email question in the SAME reply (e.g. "Great - we'll have someone take a look at the Accord. To lock in 2pm tomorrow, could I get your email?"). The trade-in details are captured automatically; you don't need any markers.
   - If they only said they HAVE a trade-in WITHOUT specifying the vehicle (e.g. "yes I have a trade", "i also have a trade in", "i'd like to trade something in"): do NOT ask for email yet. First ask what vehicle they're trading in (e.g. "Got it - what vehicle would you like to trade in?"). Once they tell you, then in the next turn acknowledge it and ask for the email.
- If the customer is interested in financing: briefly confirm the dealer offers financing per the dealer info, note that the team will be ready to discuss it at the visit, then ask the email question in the SAME reply (e.g. "Yes, we offer financing - the team will go over options with you when you're here. To lock in 2pm tomorrow, could I get your email?").
- If the customer says no / nothing / they're good: proceed straight to STEP 2 in the SAME reply ("Perfect - to lock in 2pm tomorrow, could I get your email?").
- The customer may answer multiple at once (e.g. "yes I have a 2018 Accord and want financing"). Acknowledge both naturally in one reply, then advance to STEP 2 (or, if the trade-in vehicle wasn't specified, ask for it instead of email).
- Ask STEP 1.5 at most ONCE per booking attempt. Never re-ask the multi-part STEP 1.5 question once they've answered - but DO follow up on missing trade-in details if they said they have one without naming it.

STEP 2 - Ask for email (only if missing)
- Trigger: STEP 1.5 has completed (or was skipped because car_desc is "general visit") AND the customer's email is NOT yet on file (see CUSTOMER PROFILE above).
- Ask for the email and ONLY the email. Do NOT re-ask for the time, name, last name, or phone.
- Phrasing example (≤155 chars):
  - "Got it - to lock in 2pm tomorrow, could I get your email?"
- If email IS already on file, skip STEP 2 entirely and go straight to STEP 3.

STEP 3 - Confirm (in this same reply, with META_JSON)
- Trigger: a specific clock time has been established AND the customer has first name + email on file. Last name is NOT required and MUST NOT be asked for.
- Use the appropriate template phrasing for the customer-facing text - do NOT paraphrase further, do NOT use "You're booked" or "I'll confirm next" or any variant:
  - With a specific vehicle: "You're all set, [First Name]! Your appointment is confirmed for [DAY at TIME] to view the [YEAR MAKE MODEL]. We look forward to seeing you!"
  - General visit (no specific vehicle): "You're all set, [First Name]! Your appointment is confirmed for [DAY at TIME]. We look forward to seeing you!"
- [DAY at TIME] is REQUIRED to be unambiguous to a reader who hasn't seen the rest of the conversation. Examples: "Saturday at 3 PM", "tomorrow at 10 AM", "Friday May 16 at 2 PM", "today at 5 PM". NEVER write just a clock time without a day reference — "3 PM" alone is a confirmation defect because the customer cannot tell which day. If the customer's most recent message established a day (e.g. "I can be there Saturday at 3 PM" or "are you open tomorrow" → "I can be there at 3 PM"), the day MUST appear in the confirmation.
- Then, on a NEW LINE at the very END of the SAME reply (hidden from customer by the system), add EXACTLY:
   META_JSON: {{"confirmed": true, "visit_time": "<human readable time>", "visit_time_iso": "<YYYY-MM-DDTHH:MM:SS>", "car_desc": "<year make model or 'general visit'>", "customer_name": "<first name>", "customer_email": "<email>"}}
- The user-visible template AND the META_JSON line are NOT optional - both must appear in the same reply. Without META_JSON, the booking is never recorded and the dealer is never notified. This is the most important rule in the entire flow.

RESCHEDULES (very important)
- A reschedule is when the customer asks to change the time of an EXISTING confirmed appointment (e.g. "can I move it to 10am instead", "reschedule for 3pm tomorrow", "an hour later").
- For a reschedule, SKIP STEP 1 and STEP 2 entirely (the profile is already on file). Go DIRECTLY to STEP 3 with the new time.
- The reschedule confirmation reply MUST include the META_JSON marker exactly like a brand-new booking - without it, the dealer is not notified and the booking is not recorded. This is non-negotiable.
- Example reschedule reply:
  "Certainly, Evan! Your appointment is now rescheduled for 10 AM today to view the 2023 Honda Accord Hybrid. We look forward to seeing you then!
   META_JSON: {{"confirmed": true, "visit_time": "10am today", "visit_time_iso": "2026-04-25T10:00:00", "car_desc": "2023 Honda Accord Hybrid", "customer_name": "Evan", "customer_email": "evanssc49@icloud.com"}}"

OTHER RULES
- Do NOT include META_JSON in any other message.
- If you learn any profile field outside of a confirmation, add at the very end of your reply, on its own line, exactly the markers for what you learned this turn:
   META_NAME: <first name>
   META_LAST_NAME: <last name>
   META_EMAIL: <email>
   META_PHONE: <10-digit US phone number, digits only>

- First name and phone are collected via a form that pops up after the customer's first message, BEFORE you ever respond. By the time you see a conversation, those two fields are already on file. Do NOT ask for first name or phone in chat - only ask for last name and email when the booking flow requires them.

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

# Markdown link: [text](url) — capture the visible text so we can keep it after stripping the URL.
_MD_LINK_RE   = re.compile(r"\[([^\]]+)\]\((?:https?://|www\.|tel:)[^)]+\)", re.I)
_BARE_URL_RE  = re.compile(r"\b(?:https?://|www\.)\S+", re.I)


def _scrub_llm_urls(text: str) -> str:
    """Remove URLs from LLM output. Vehicle/page URLs are only safe to send from the
    deterministic link handler that reads inventory.detail_url — anything the LLM
    produces is a hallucination. Strips markdown link wrappers, keeping the link text."""
    if not text:
        return text
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _BARE_URL_RE.sub("", text)
    # Tidy up the empty parens / double spaces / trailing punctuation the strip leaves behind.
    text = re.sub(r"\(\s*\)", "", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    return text.strip()


_CARFAX_URL_RE = re.compile(r"https?://(?:www\.)?carfax\.com/\S+", re.I)
# Markdown link whose URL is a CarFax URL: [text](https://carfax.com/...)
_CARFAX_MD_LINK_RE = re.compile(
    r"\[[^\]]*\]\((https?://(?:www\.)?carfax\.com/[^\s)]+)\)",
    re.I,
)


def _dedupe_carfax_in_reply(reply: str, history: List[Dict[str, Any]]) -> str:
    """If the LLM reply contains a CarFax URL we ALREADY sent in this
    conversation, replace it with a brief 'see the CARFAX I sent earlier'
    reference so the customer doesn't get the same 142-char URL spammed at
    them across multiple turns. Different CarFax URL (different car) passes
    through untouched. Handles both bare URLs and markdown-link wrapped URLs
    (`[CarFax report](url)`) so the brackets don't end up dangling after the
    URL is stripped."""
    if not reply:
        return reply
    reply_urls = _CARFAX_URL_RE.findall(reply)
    if not reply_urls:
        return reply
    prior_urls: set = set()
    for m in history:
        if m.get("role") != "assistant":
            continue
        for u in _CARFAX_URL_RE.findall(m.get("content") or ""):
            prior_urls.add(u.rstrip(".,);:"))
    if not prior_urls:
        return reply

    REPLACEMENT = "(see the CARFAX I sent earlier)"

    # Pass 1 — replace markdown-wrapped CarFax links first so the brackets
    # get cleaned up with the URL in a single shot.
    def _md_sub(match: re.Match) -> str:
        url = match.group(1).rstrip(".,);:")
        return REPLACEMENT if url in prior_urls else match.group(0)
    new_reply = _CARFAX_MD_LINK_RE.sub(_md_sub, reply)

    # Pass 2 — handle any remaining BARE CarFax URLs. Also eat any leading
    # connector phrase ("here:", "available at:") that becomes awkward
    # without a URL trailing it.
    for url in _CARFAX_URL_RE.findall(new_reply):
        clean = url.rstrip(".,);:")
        if clean in prior_urls:
            new_reply = re.sub(
                r"(?:\s*(?:here|available|view\s+it|review\s+it|check\s+it\s+out)?\s*[:\-]?\s*)?"
                + re.escape(url),
                " " + REPLACEMENT,
                new_reply,
                count=1,
            )
    # Tidy spacing + orphaned punctuation
    new_reply = re.sub(r"[ \t]{2,}", " ", new_reply)
    new_reply = re.sub(r"\s+([,.;:!?])", r"\1", new_reply)
    return new_reply.strip()


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
    reply_text, extracted_phone = _pull_marker(reply_text, "META_PHONE")

    if meta:
        if meta.get("customer_name") and not extracted_name:
            extracted_name = str(meta["customer_name"]).strip()
        if meta.get("customer_last_name") and not extracted_last:
            extracted_last = str(meta["customer_last_name"]).strip()
        if meta.get("customer_email") and not extracted_email:
            extracted_email = str(meta["customer_email"]).strip()

    if extracted_name or extracted_last or extracted_email or extracted_phone:
        if meta is None:
            meta = {}
        if extracted_name:  meta["_extracted_name"] = extracted_name
        if extracted_last:  meta["_extracted_last_name"] = extracted_last
        if extracted_email: meta["_extracted_email"] = extracted_email
        if extracted_phone: meta["_extracted_phone"] = extracted_phone

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

        # For widget customers, customer_phone is "+web..." which Twilio
        # can't reach; resolve to the real phone collected at the welcome.
        outbound_phone = resolve_outbound_customer_phone(customer_phone, twilio_number)
        if not outbound_phone or not outbound_phone.startswith("+"):
            app.logger.warning(
                "Reminder skipped for appt #%d: no real phone for %s",
                appointment_id, customer_phone,
            )
            continue

        reminder_body = (
            f"This is a friendly reminder of your upcoming appointment at {visit_time} "
            f"to view the {car_desc}. Please reply Yes to confirm or No to cancel."
        )
        ok, err = send_sms_to_customer(customer_phone=outbound_phone, from_number=twilio_number, body=reminder_body)
        if ok:
            mark_reminder_sent(appointment_id)
            save_message(customer_phone, twilio_number, "assistant", reminder_body)
            set_pending_reconfirmation(customer_phone, twilio_number, appt["dealer_notify_phone"],
                                       visit_time, car_desc, appointment_id)
            app.logger.info("Sent reminder to %s for appt #%d", outbound_phone, appointment_id)
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

    def _safe_mark(cp, tn):
        """Mark a cold follow-up as sent. Returns True on success, False on
        DB error. Failures must NOT raise — they'd otherwise kill the loop
        and cause every remaining customer to be re-eligible next cycle,
        producing a retry storm of duplicate SMS."""
        try:
            mark_cold_followup_sent(cp, tn)
            return True
        except Exception as e:
            app.logger.warning("Cold follow-up: mark_sent failed for %s on %s: %s", cp, tn, e)
            return False

    # In-cycle dedupe: a single customer may have several sessions all
    # eligible at once (each clearChat / new browser session creates a new
    # +web<id> customer_phone). Without dedupe, all sessions sharing the
    # same real_phone would fire SMS to the same number simultaneously.
    seen_outbound: set = set()

    for convo in cold:
        customer_phone = convo["customer_phone"]
        twilio_number  = convo["twilio_number"]

        # Per-customer try/except: one customer's failure must not kill the
        # loop and re-expose every other eligible customer next cycle.
        try:
            if get_latest_appointment(customer_phone, twilio_number):
                _safe_mark(customer_phone, twilio_number)
                continue
            if get_pending(customer_phone, twilio_number):
                continue
            last_msg = get_last_customer_message(customer_phone, twilio_number)
            if last_msg and DISINTEREST_RE.search(last_msg):
                _safe_mark(customer_phone, twilio_number)
                continue
            if normalize_phone(customer_phone) == normalize_phone(twilio_number):
                _safe_mark(customer_phone, twilio_number)
                continue

            # Resolve the outbound number. For SMS customers this is the same
            # as customer_phone; for widget customers it pulls real_phone from
            # their profile (collected via the welcome gate). If we have no real
            # number to text, skip without marking - we'll retry once they
            # provide it.
            outbound_phone = resolve_outbound_customer_phone(customer_phone, twilio_number)
            if not outbound_phone or not outbound_phone.startswith("+"):
                app.logger.info(
                    "Cold follow-up: no real phone for %s via %s yet, skipping",
                    customer_phone, twilio_number,
                )
                continue

            # Sibling-session dedupe: if another session sharing this real
            # phone already fired in the current cycle, just mark this one
            # as sent (so it stops being eligible) and move on. Prevents
            # the burst of identical SMSes after a restart when a customer
            # has multiple abandoned sessions.
            if outbound_phone in seen_outbound:
                _safe_mark(customer_phone, twilio_number)
                continue

            # Cross-cycle dedupe: this real phone has already received a
            # follow-up via some prior session that pre-dates this cycle.
            # Don't send another. The only way to reset this is for the
            # customer to hit "Clear Chat", which wipes follow-up history
            # for their real phone.
            if has_followup_for_real_phone(outbound_phone, twilio_number):
                _safe_mark(customer_phone, twilio_number)
                continue

            dealer        = select_dealer_for_twilio_number(dealers, twilio_number) if dealers else {}
            dealer_name   = get_row_field(dealer, DEALER_NAME_ALIASES) if dealer else ""
            customer_profile_local = get_customer_profile(customer_phone, twilio_number)
            customer_name = customer_profile_local.get("name", "")
            customer_last = customer_profile_local.get("last_name", "")
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

            # Mark BEFORE sending so a transient DB lock after the SMS goes
            # out doesn't cause a retry storm next cycle. Trade-off: if marking
            # fails we skip this customer entirely (one missed follow-up beats
            # six duplicate SMSes).
            if not _safe_mark(customer_phone, twilio_number):
                continue
            # Persistently mark every other session sharing this real phone
            # so future cycles don't re-fire if more sibling sessions age
            # into the cold window later.
            try:
                mark_all_sessions_followed_up(outbound_phone, twilio_number)
            except Exception as e:
                app.logger.warning("Cold follow-up sibling-mark failed for %s: %s", outbound_phone, e)
            seen_outbound.add(outbound_phone)

            ok, err = send_sms_to_customer(customer_phone=outbound_phone, from_number=twilio_number, body=followup_body)
            if ok:
                try:
                    save_message(customer_phone, twilio_number, "assistant", followup_body)
                except Exception as e:
                    app.logger.warning("Cold follow-up save_message failed for %s: %s", customer_phone, e)
                app.logger.info("Sent cold follow-up to %s via %s", outbound_phone, twilio_number)

                # One-shot dealer lead notification. Fires only when the cold
                # follow-up itself fires (which is gated to once per customer
                # via cold_followups table), so the dealer is texted at most once.
                if dealer:
                    full_name = (customer_name + (" " + customer_last if customer_last else "")).strip() or "Unknown name"
                    lead_body = (
                        f"Possible lead: {full_name} ({outbound_phone}) - "
                        f"customer chatted but did not book a visit. "
                        f"Consider reaching out."
                    )
                    try:
                        notify_all_staff(dealer, twilio_number, lead_body)
                    except Exception as e:
                        app.logger.warning("Lead notify failed for %s: %s", customer_phone, e)
            else:
                app.logger.warning("Cold follow-up failed for %s: %s", customer_phone, err)
        except Exception as e:
            app.logger.error(
                "Cold follow-up iteration crashed for %s on %s: %s",
                customer_phone, twilio_number, e,
            )


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


_DAY_WORD_PATTERN = (
    r"(?:today|tonight|tomorrow|"
    r"monday|tuesday|wednesday|thursday|friday|saturday|sunday)"
)


def _augment_bare_time_with_day(text: str, from_number: str, to_number: str) -> str:
    """If `text` contains a bare clock time (no day word within ~30 chars),
    insert "DAY at " before it using the most recent day mentioned in the
    customer's conversation history. Returns the augmented (or unchanged)
    text. Pure string transform — no DB writes, no flask.g touches.

    Used for both the chat reply rewrite and for visit_time strings stored
    in pending/appointments + embedded in dealer/customer SMS/email alerts.
    Without applying it to stored visit_time, the dealer notification reads
    "Time: 3 PM" with no day."""
    if not text:
        return text
    time_re = re.compile(r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b", re.I)
    matches = list(time_re.finditer(text))
    if not matches:
        return text
    bare_idx = []
    for i, m in enumerate(matches):
        win_start = max(0, m.start() - 30)
        win_end = min(len(text), m.end() + 30)
        window = text[win_start:win_end].lower()
        if not re.search(rf"\b{_DAY_WORD_PATTERN}\b", window):
            bare_idx.append(i)
    if not bare_idx:
        return text
    history = get_recent_messages(from_number, to_number, limit=10)
    day_found = None
    for msg in reversed(history):
        if msg.get("role") != "user":
            continue
        content = (msg.get("content") or "").lower()
        m = re.search(rf"\b{_DAY_WORD_PATTERN}\b", content)
        if m:
            day_found = m.group(0)
            break
    if not day_found:
        for msg in reversed(history):
            if msg.get("role") != "assistant":
                continue
            content = (msg.get("content") or "").lower()
            m = re.search(rf"\b{_DAY_WORD_PATTERN}\b", content)
            if m:
                day_found = m.group(0)
                break
    if not day_found:
        return text
    if day_found.lower() in ("today", "tonight", "tomorrow"):
        day_pretty = day_found.lower()
    else:
        day_pretty = day_found.title()
    pieces, last_end = [], 0
    for i, m in enumerate(matches):
        pieces.append(text[last_end:m.start()])
        pieces.append(f"{day_pretty} at {m.group(1)}" if i in bare_idx else m.group(0))
        last_end = m.end()
    pieces.append(text[last_end:])
    return "".join(pieces)


def _maybe_inject_day_in_time(from_number: str, to_number: str) -> bool:
    """If the captured reply contains a clock time ("3 PM") with no day
    reference within ~30 chars before it, look in conversation history for
    the most recent day mention (from customer messages first, then assistant)
    and rewrite the reply to insert "DAY at " before the bare clock time.

    The LLM frequently strips the day when echoing a time back to the customer
    ("Got it - 3 PM for the Camry") even though the customer said "Saturday at
    3 PM". That's a confirmation defect — the customer cannot tell which day
    was booked. Returns True if the reply was modified."""
    captured = (g.get("captured_reply") or "").strip()
    if not captured:
        return False
    # Skip when the reply is reporting dealer business hours or service-shop
    # operating hours — those contain bare clock times that should NOT be
    # rewritten as "today at 9 AM". Day-injection is for BOOKING replies only.
    _captured_lower = captured.lower()
    if re.search(
        r"\b(hours? of operation|business hours|we'?re (?:open|closed)|"
        r"(?:mon|tue|wed|thu|fri|sat|sun)\s*[-–]\s*(?:mon|tue|wed|thu|fri|sat|sun)|"
        r"closed (?:today|on|sunday|saturday)|are open (?:from|today|monday|tuesday|wednesday|thursday|friday|saturday|sunday))\b",
        _captured_lower,
    ):
        return False
    new_reply = _augment_bare_time_with_day(captured, from_number, to_number)
    if new_reply == captured:
        return False
    try:
        conn = _db()
        with conn:
            row = conn.execute(
                "SELECT id FROM messages WHERE customer_phone=? AND twilio_number=? "
                "AND role='assistant' ORDER BY id DESC LIMIT 1",
                (from_number, to_number),
            ).fetchone()
            if row:
                conn.execute("UPDATE messages SET content=? WHERE id=?",
                             (new_reply, row["id"]))
        conn.close()
    except Exception as e:
        app.logger.warning("Day-in-time rewrite (DB) failed: %s", e)
    g.captured_reply = new_reply
    app.logger.info("Injected day into bare clock time for %s", from_number)
    return True


_NAME_GREETER_RE = re.compile(
    r"\b(Thanks|Thank\s+you|Hi|Hello|Sure|Certainly|Got\s+it|Of\s+course|"
    r"Perfect|Alright|Awesome|Great|Okay|Ok)"
    r"(,\s+)"
    r"([A-Z][a-z]+)"
    r"(?=[\s\.\!\,\?])"
)


def _maybe_fix_customer_name_in_reply(from_number: str, to_number: str) -> bool:
    """If the LLM addressed the customer by a different first name than the
    one stored in the profile, rewrite the reply to use the real name. The
    LLM sometimes pulls a word from the customer's most recent message as a
    name (e.g. "can i pay cash" → "Thanks, Can!") even when the customer's
    real first name is in the prompt context. Returns True if rewritten."""
    captured = (g.get("captured_reply") or "").strip()
    if not captured:
        return False
    profile = get_customer_profile(from_number, to_number)
    real_name = (profile.get("name") or "").strip()
    if not real_name or not _looks_like_real_name(real_name):
        return False
    matches = list(_NAME_GREETER_RE.finditer(captured))
    if not matches:
        return False
    real_name_lower = real_name.lower()
    rewrote = False
    new_reply = captured
    # Walk matches in reverse so earlier offsets stay valid as we splice.
    for m in reversed(matches):
        name_in_reply = m.group(3)
        if name_in_reply.lower() == real_name_lower:
            continue
        # Replace just the name token, preserve greeter + punctuation
        new_reply = (
            new_reply[:m.start(3)]
            + real_name.title()
            + new_reply[m.end(3):]
        )
        rewrote = True
    if not rewrote:
        return False
    try:
        conn = _db()
        with conn:
            row = conn.execute(
                "SELECT id FROM messages WHERE customer_phone=? AND twilio_number=? "
                "AND role='assistant' ORDER BY id DESC LIMIT 1",
                (from_number, to_number),
            ).fetchone()
            if row:
                conn.execute("UPDATE messages SET content=? WHERE id=?",
                             (new_reply, row["id"]))
        conn.close()
    except Exception as e:
        app.logger.warning("Name-fix rewrite (DB) failed: %s", e)
    g.captured_reply = new_reply
    app.logger.info("Corrected wrong customer name in reply for %s (restored %r)", from_number, real_name)
    return True


def _maybe_inject_step_1_5(from_number: str, to_number: str, *, dealer_phone: str = "") -> bool:
    """Server-side guard for STEP 1.5 of the booking flow. The LLM frequently
    jumps from STEP 1 (time) straight to STEP 2 (email) without first asking
    the customer about questions/financing/trade-in. When we detect the LLM's
    reply is asking for email AND we're booking a specific vehicle AND
    STEP 1.5 hasn't been asked yet, we rewrite the reply to the STEP 1.5
    question instead. Returns True if the reply was overridden.

    Skipped entirely for SERVICE appointments — STEP 1.5 (financing/trade-in)
    is sales-specific and doesn't apply when the customer is bringing their
    own car in for repair/maintenance."""
    captured = (g.get("captured_reply") or "").strip()
    if not captured:
        return False

    # Skip the entire STEP 1.5 enforcement chain for service-context bookings.
    _svc_history = get_recent_messages(from_number, to_number, limit=10)
    if _is_service_appointment_context(_svc_history):
        return False

    captured_lower = captured.lower()
    is_step_1_5_reply = (
        "trade-in" in captured_lower
        and "financing" in captured_lower
        and "?" in captured
    )

    # Premature STEP 3 guard: the LLM sometimes jumps straight to confirmation
    # language ("Your appointment is set / confirmed for X") without going
    # through STEP 1.5 (questions/financing/trade-in) and STEP 2 (email). If
    # the captured reply contains confirmation language but NO META_JSON
    # marker and the profile is missing email, the appointment was never
    # actually logged. Override the reply to back the flow up to whichever
    # step is missing.
    _confirmation_phrases = (
        "appointment is confirmed", "appointment is set",
        "you're all set", "youre all set",
        "you're booked", "youre booked", "you are booked",
        "all set,",
    )
    _has_confirmation_lang = any(p in captured_lower for p in _confirmation_phrases)
    _has_meta_json = "META_JSON" in captured or "meta_json" in captured_lower
    if _has_confirmation_lang and not _has_meta_json:
        _profile = get_customer_profile(from_number, to_number)
        _email = (_profile.get("email") or "").strip()
        _name_for_reply = (_profile.get("name") or "").strip() or "and welcome"
        _ri_history = get_recent_messages(from_number, to_number, limit=30)
        _step_1_5_already_asked = any(
            "trade-in" in (m.get("content") or "").lower()
            and "financing" in (m.get("content") or "").lower()
            and "?" in (m.get("content") or "")
            for m in _ri_history if m.get("role") == "assistant"
        )
        # Recover visit_time + car_desc from pending or recent assistant turns.
        _pending = get_pending(from_number, to_number)
        _vt = (_pending.get("visit_time") if _pending else "") or ""
        _cd = (_pending.get("car_desc") if _pending else "") or ""
        if not _vt or not _cd:
            for _m in _ri_history[::-1]:
                if _m.get("role") != "assistant":
                    continue
                _c = _m.get("content") or ""
                if not _cd:
                    _cm = re.search(
                        r"\b((?:19|20)\d{2}\s+[A-Za-z][\w\-]*(?:\s+[\w\-]+){0,5})",
                        _c,
                    )
                    if _cm:
                        _cd = _cm.group(1).strip().rstrip(".,!?")
                if not _vt:
                    _tm = re.search(
                        r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday))?\b",
                        _c, re.I,
                    )
                    if _tm:
                        _vt = _tm.group(0).strip()
                if _vt and _cd:
                    break
        # Decide what step we need to back up to.
        _car_phrase = f"the {_cd}" if _cd else "the vehicle"
        _time_phrase = f"{_vt} " if _vt else ""
        if not _step_1_5_already_asked:
            new_reply = (
                f"Got it - {_time_phrase}for {_car_phrase}. Any other questions about it, "
                f"are you interested in financing, or do you have a trade-in you'd like us to take a look at?"
            )
        elif not _email:
            new_reply = (
                f"Almost set! Before I lock in {_time_phrase}for {_car_phrase}, "
                f"could I please get your email address?"
            )
        else:
            # Profile is complete and STEP 1.5 was asked, but the LLM still
            # produced confirmation language without META_JSON. Leave the
            # reply alone — this should never happen, but bailing out is
            # safer than rewriting.
            new_reply = None
        if new_reply:
            # Augment bare clock time with day before storing/displaying.
            if _vt:
                _vt = _augment_bare_time_with_day(_vt, from_number, to_number)
                # Rebuild reply with the augmented time so customer sees the
                # right thing too (the day-injector below also catches this,
                # but doing it here keeps pending + reply consistent).
                _time_phrase = f"{_vt} "
                if not _step_1_5_already_asked:
                    new_reply = (
                        f"Got it - {_time_phrase}for {_car_phrase}. Any other questions about it, "
                        f"are you interested in financing, or do you have a trade-in you'd like us to take a look at?"
                    )
                elif not _email:
                    new_reply = (
                        f"Almost set! Before I lock in {_time_phrase}for {_car_phrase}, "
                        f"could I please get your email address?"
                    )
            # Persist as pending so downstream handlers (email auto-book,
            # trade-in collection, etc.) can find the booking the customer
            # just established. Without this, the email handler later finds
            # no pending row and the booking flow stalls.
            if _vt and _cd:
                _, _vt_iso = parse_visit_time_from_text(_vt)
                if not _pending:
                    try:
                        set_pending(from_number, to_number, dealer_phone or "",
                                    _vt, _vt_iso or "", _cd)
                    except Exception as _e:
                        app.logger.warning("Premature-STEP-3 set_pending failed: %s", _e)
            try:
                conn = _db()
                with conn:
                    row = conn.execute(
                        "SELECT id FROM messages WHERE customer_phone=? AND twilio_number=? "
                        "AND role='assistant' ORDER BY id DESC LIMIT 1",
                        (from_number, to_number),
                    ).fetchone()
                    if row:
                        conn.execute("UPDATE messages SET content=? WHERE id=?",
                                     (new_reply, row["id"]))
                conn.close()
            except Exception as e:
                app.logger.warning("Premature-STEP-3 rewrite (DB) failed: %s", e)
            g.captured_reply = new_reply
            app.logger.info(
                "Caught premature STEP 3 (no META_JSON, email_missing=%s, step15_asked=%s) for %s",
                not _email, _step_1_5_already_asked, from_number,
            )
            return True

    # Re-ask guard: STEP 1.5 must only be asked ONCE per booking. If the
    # LLM is generating a second STEP 1.5 question (most commonly after the
    # customer has already given trade-in details), override the reply with
    # an email-ask so the booking flow progresses instead of looping. Without
    # this the bot drops the trade-in context the customer just provided and
    # asks the same multi-part question again.
    if is_step_1_5_reply:
        # Use a wide history window — the trade-in detail flow can run for
        # 6+ turns (asking for vehicle, then mileage/title/condition), pushing
        # the original STEP 1.5 question outside a 14-message window. Without
        # enough lookback, the re-ask count comes back as 1 and we miss the
        # override that should fire when the LLM loops.
        _ri_history = get_recent_messages(from_number, to_number, limit=30)
        # Count STEP 1.5 questions in history. The just-generated reply is
        # already saved to the DB at this point, so it appears in history as
        # well. We need at least 2 occurrences (the current + an earlier one)
        # to call this a re-ask; otherwise we'd override the legitimate first
        # ask.
        _step_1_5_count = sum(
            1 for m in _ri_history if m.get("role") == "assistant"
            and "trade-in" in (m.get("content") or "").lower()
            and "financing" in (m.get("content") or "").lower()
            and "?" in (m.get("content") or "")
        )
        app.logger.info(
            "STEP1.5-debug: is_reply=%s count=%d history_size=%d for %s",
            is_step_1_5_reply, _step_1_5_count, len(_ri_history), from_number,
        )
        if _step_1_5_count >= 2:
            # Build an email-ask override using whatever visit_time + car_desc
            # we can recover from pending or recent assistant turns.
            _profile = get_customer_profile(from_number, to_number)
            _email_on_file = (_profile.get("email") or "").strip()
            _pending = get_pending(from_number, to_number)
            _vt = (_pending.get("visit_time") if _pending else "") or ""
            _cd = (_pending.get("car_desc") if _pending else "") or ""
            if not _vt or not _cd:
                for _m in _ri_history[::-1]:
                    if _m.get("role") != "assistant":
                        continue
                    _c = _m.get("content") or ""
                    if not _cd:
                        _cm = re.search(
                            r"\b((?:19|20)\d{2}\s+[A-Za-z][\w\-]*(?:\s+[\w\-]+){0,5})",
                            _c,
                        )
                        if _cm:
                            _cd = _cm.group(1).strip().rstrip(".,!?")
                    if not _vt:
                        _tm = re.search(
                            r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday))?\b",
                            _c, re.I,
                        )
                        if _tm:
                            _vt = _tm.group(0).strip()
                    if _vt and _cd:
                        break
            if _email_on_file:
                # Email already on file — close out the booking instead of
                # re-asking. The LLM will pick it up on the next turn since
                # all required pieces are captured.
                if _vt and _cd:
                    new_reply = f"Got it - locking in {_vt} for the {_cd}. I'll have it confirmed in a moment."
                elif _vt:
                    new_reply = f"Got it - locking in {_vt}. I'll have it confirmed in a moment."
                else:
                    new_reply = "Got it - I'll have your visit confirmed in a moment."
            else:
                if _vt and _cd:
                    new_reply = f"Got it - to lock in {_vt} for the {_cd}, could I get your email address?"
                elif _vt:
                    new_reply = f"Got it - to lock in {_vt}, could I get your email address?"
                elif _cd:
                    new_reply = f"Got it - to lock in your visit for the {_cd}, could I get your email address?"
                else:
                    new_reply = "Got it - to lock in your visit, could I get your email address?"
            try:
                conn = _db()
                with conn:
                    row = conn.execute(
                        "SELECT id FROM messages WHERE customer_phone=? AND twilio_number=? "
                        "AND role='assistant' ORDER BY id DESC LIMIT 1",
                        (from_number, to_number),
                    ).fetchone()
                    if row:
                        conn.execute("UPDATE messages SET content=? WHERE id=?",
                                     (new_reply, row["id"]))
                conn.close()
            except Exception as e:
                app.logger.warning("STEP 1.5 re-ask rewrite (DB) failed: %s", e)
            g.captured_reply = new_reply
            app.logger.info("Overrode LLM STEP 1.5 re-ask with email/confirm prompt for %s", from_number)
            return True

    # Is this an email-request reply?
    asks_email = bool(re.search(r"\bemail\b", captured, re.I)) and "?" in captured
    if not asks_email:
        # Even when we don't override, if the LLM just did STEP 1.5 naturally
        # we want a pending appointment record so downstream handlers
        # (especially the trade-in followup) can reference the visit time +
        # car the customer just settled on. Without this the trade-in handler
        # asks "what day works for you?" even though a time has been given.
        if is_step_1_5_reply:
            try:
                pending = get_pending(from_number, to_number)
                if not pending:
                    history = get_recent_messages(from_number, to_number, limit=10)
                    car_desc = ""
                    visit_time = ""
                    for m in history[::-1]:
                        if m.get("role") != "assistant":
                            continue
                        c = m.get("content") or ""
                        if not car_desc:
                            car_m = re.search(
                                r"\b((?:19|20)\d{2}\s+[A-Za-z][\w\-]*(?:\s+[\w\-]+){0,5})",
                                c,
                            )
                            if car_m:
                                car_desc = car_m.group(1).strip().rstrip(".,!?")
                                car_desc = re.sub(
                                    r"\s+(currently|is|are|was|were|will|would|could|should|can|may|might|must|"
                r"now|now,|available|here|today|tomorrow|only|also|just|still|"
                r"has|have|had|comes|came|features|includes|runs|drives|looks|sounds|seems|gets|"
                r"doesn|isn|wasn|weren|won|didn|couldn|shouldn|wouldn|"
                r"hasn|haven|hadn|shan|mustn|mightn|ain|don).*$",
                                    "", car_desc, flags=re.I,
                                ).strip()
                        if not visit_time:
                            t_m = re.search(
                                r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday))?\b",
                                c, re.I,
                            )
                            if t_m:
                                visit_time = t_m.group(0).strip()
                        if car_desc and visit_time:
                            break
                    if car_desc and visit_time:
                        _, visit_time_iso = parse_visit_time_from_text(visit_time)
                        visit_time = _augment_bare_time_with_day(visit_time, from_number, to_number)
                        set_pending(from_number, to_number, dealer_phone or "",
                                    visit_time, visit_time_iso or "", car_desc)
                        app.logger.info(
                            "Set pending after natural STEP 1.5: %s for %s",
                            visit_time, car_desc,
                        )
            except Exception as e:
                app.logger.warning("Set-pending after STEP 1.5 failed: %s", e)
        return False

    # Skip widget-only customers? No - this guard applies to widget too.
    # Use a wide window — the prior-STEP-1.5 check below needs to see the
    # original STEP 1.5 question even after a long trade-in detail flow has
    # pushed it past the default 14-message limit. Without this the existing
    # injector re-fires STEP 1.5 when the customer asks an unrelated question
    # mid-flow ("can you hold it") because it can't find the prior ask.
    history = get_recent_messages(from_number, to_number, limit=30)

    # If the customer mentioned a trade-in but appraisal details (mileage,
    # title status, condition) are still missing, ask for those first instead
    # of asking for email. The dealer needs the appraisal info captured
    # in-conversation so it ends up on the alert.
    _trade_keywords_re = re.compile(
        r"\b(trade|trading)[\s\-]?in\b|\bhave\s+a\s+trade\b|\btrade\s+something|\bgot\s+a\s+trade\b",
        re.I,
    )
    _trade_mentioned_by_customer = any(
        _trade_keywords_re.search(m.get("content") or "")
        for m in history if m.get("role") == "user"
    )
    if _trade_mentioned_by_customer:
        # Count how many trade-in detail follow-ups we've already asked so we
        # vary phrasing and stop after 3 rounds (avoids a regex-mismatch loop
        # while still pursuing the missing piece across a couple of turns).
        _ask_phrases = ("round out the ballpark", "got it - and what", "got it - one more", "and the ")
        followup_count = sum(
            1 for m in history if m.get("role") == "assistant"
            and any(p in (m.get("content") or "").lower() for p in _ask_phrases)
        )
        trade_missing_parts = _trade_in_missing_parts(history)
        if trade_missing_parts and followup_count < 3:
            # Try to capture the trade-in vehicle first so the profile is
            # up-to-date when the dealer alert fires later.
            try:
                candidate = extract_trade_in_vehicle(history)
                if candidate:
                    existing = (get_customer_profile(from_number, to_number).get("trade_in_vehicle") or "").strip()
                    if candidate != existing:
                        save_customer_profile(from_number, to_number, trade_in_vehicle=candidate)
            except Exception as e:
                app.logger.warning("trade-in capture during email-gate failed: %s", e)
            if followup_count == 0:
                new_reply = (
                    f"Thanks for sharing that. To round out the ballpark, could "
                    f"you also share the {_format_missing_list(trade_missing_parts)}?"
                )
            elif followup_count == 1:
                if len(trade_missing_parts) == 1:
                    new_reply = f"Got it - and what's the {trade_missing_parts[0]}?"
                else:
                    new_reply = f"Got it - one more thing - the {_format_missing_list(trade_missing_parts)}?"
            else:
                if len(trade_missing_parts) == 1:
                    new_reply = f"And the {trade_missing_parts[0]}?"
                else:
                    new_reply = f"And the {_format_missing_list(trade_missing_parts)}?"
            try:
                conn = _db()
                with conn:
                    row = conn.execute(
                        "SELECT id FROM messages WHERE customer_phone=? AND twilio_number=? "
                        "AND role='assistant' ORDER BY id DESC LIMIT 1",
                        (from_number, to_number),
                    ).fetchone()
                    if row:
                        conn.execute("UPDATE messages SET content=? WHERE id=?",
                                     (new_reply, row["id"]))
                conn.close()
            except Exception as e:
                app.logger.warning("trade-in followup rewrite (DB) failed: %s", e)
            g.captured_reply = new_reply
            app.logger.info("Injected trade-in detail followup (overrode LLM email ask) for %s", from_number)
            return True

    # Already asked STEP 1.5? Look for the "trade-in" + "financing" combo in
    # any recent assistant message.
    for m in history:
        if m.get("role") == "assistant":
            c = (m.get("content") or "").lower()
            if "trade-in" in c and "financing" in c:
                return False

    # Pull car_desc + visit_time. Source priority:
    #   1. pending row (if already set)
    #   2. the LLM's just-produced reply ("captured"), which is the email-ask
    #      we are about to override — it was built with the booking context
    #      and is the authoritative source for THIS booking's time
    #   3. recent bot history (fallback only — risky because unrelated bot
    #      messages like the dealer's hours response can contain stray times,
    #      e.g. "we're open from 9am to 6pm" was grabbing 9am over the
    #      customer's actual 3pm)
    pending = get_pending(from_number, to_number)
    car_desc = ""
    visit_time = ""
    visit_time_iso = ""
    if pending:
        car_desc = (pending.get("car_desc") or "").strip()
        visit_time = (pending.get("visit_time") or "").strip()
        visit_time_iso = (pending.get("visit_time_iso") or "").strip()

    # Source 2: the LLM's reply we're about to override. The LLM saw the
    # customer's booking message and produced something like
    # "Got it - to lock in 3pm tomorrow for the F-250, could I get your email?"
    # Pulling time + car from THIS captures the right context.
    if not visit_time and captured:
        t_m = re.search(
            r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday))?\b",
            captured, re.I,
        )
        if t_m:
            visit_time = t_m.group(0).strip()
    if not car_desc and captured:
        car_m = re.search(
            r"\b((?:19|20)\d{2}\s+[A-Za-z][\w\-]*(?:\s+[\w\-]+){0,5})",
            captured,
        )
        if car_m:
            car_desc = car_m.group(1).strip().rstrip(".,!?")
            car_desc = re.sub(
                r"\s+(currently|is|are|was|were|will|would|could|should|can|may|might|must|"
                r"now|now,|available|here|today|tomorrow|only|also|just|still|"
                r"has|have|had|comes|came|features|includes|runs|drives|looks|sounds|seems|gets|"
                r"doesn|isn|wasn|weren|won|didn|couldn|shouldn|wouldn|"
                r"hasn|haven|hadn|shan|mustn|mightn|ain|don).*$",
                "", car_desc, flags=re.I,
            ).strip()

    if not car_desc or not visit_time:
        for m in history[::-1]:
            if m.get("role") != "assistant":
                continue
            c = m.get("content") or ""
            # Skip bot replies that are hours-of-operation statements — they
            # contain stray times (e.g. "open from 9am to 6pm") that aren't
            # the booking time. Without this skip, "i can be there at 3
            # tomorrow" right after a "we're open 9am-6pm" reply gets stored
            # as 9am.
            cl = c.lower()
            if re.search(r"\b(?:we'?re\s+open|hours?\s+of\s+operation|business\s+hours|open\s+(?:from|tomorrow|today|on)|closed)\b", cl):
                continue
            if not car_desc:
                # Look for ANY year+make+model pattern in the bot's recent
                # messages, not just "for the YEAR ...". The LLM phrasing
                # varies ("see the 2022 BMW X7", "the 2022 BMW X7 is...").
                car_m = re.search(
                    r"\b((?:19|20)\d{2}\s+[A-Za-z][\w\-]*(?:\s+[\w\-]+){0,5})",
                    c,
                )
                if car_m:
                    car_desc = car_m.group(1).strip().rstrip(".,!?")
                    # Trim trailing words that are clearly not part of the car
                    # name (e.g. "currently available", "is available").
                    car_desc = re.sub(
                        r"\s+(currently|is|are|was|were|will|would|could|should|can|may|might|must|"
                r"now|now,|available|here|today|tomorrow|only|also|just|still|"
                r"has|have|had|comes|came|features|includes|runs|drives|looks|sounds|seems|gets|"
                r"doesn|isn|wasn|weren|won|didn|couldn|shouldn|wouldn|"
                r"hasn|haven|hadn|shan|mustn|mightn|ain|don).*$",
                        "", car_desc, flags=re.I,
                    ).strip()
            if not visit_time:
                t_m = re.search(
                    r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday))?\b",
                    c, re.I,
                )
                if t_m:
                    visit_time = t_m.group(0).strip()
            if car_desc and visit_time:
                break

    # If we couldn't identify a specific car, this is a general visit - leave
    # the email ask alone.
    _car_lower = car_desc.lower()
    if not car_desc or _car_lower in {"general visit", "general", "a vehicle", "visit"}:
        return False

    # Augment a bare clock time with the day from history before it lands in
    # both the chat reply AND the pending row. Without this, the dealer/customer
    # alert SMS read just "3 PM" — the day injector only patches g.captured_reply,
    # not stored visit_time fields.
    if visit_time:
        visit_time = _augment_bare_time_with_day(visit_time, from_number, to_number)

    # Build the deterministic STEP 1.5 message.
    new_reply = (
        f"Got it - {visit_time} for the {car_desc}. Any other questions "
        f"about it, are you interested in financing, or do you have a "
        f"trade-in you'd like us to take a look at?"
    ) if visit_time else (
        f"Got it - for the {car_desc}. Any other questions about it, are you "
        f"interested in financing, or do you have a trade-in you'd like us to "
        f"take a look at?"
    )

    # Make sure a pending appointment exists so the next-turn pending block
    # picks up the customer's answer (especially trade-in details).
    if visit_time and not pending:
        if not visit_time_iso:
            _, visit_time_iso = parse_visit_time_from_text(visit_time)
        try:
            set_pending(from_number, to_number, dealer_phone or "",
                        visit_time, visit_time_iso or "", car_desc)
        except Exception as e:
            app.logger.warning("set_pending during step-1.5 inject failed: %s", e)

    # Replace the last assistant message in the DB so history reflects the
    # rewritten reply instead of the LLM's email ask.
    try:
        conn = _db()
        with conn:
            row = conn.execute(
                "SELECT id FROM messages WHERE customer_phone=? AND twilio_number=? "
                "AND role='assistant' ORDER BY id DESC LIMIT 1",
                (from_number, to_number),
            ).fetchone()
            if row:
                conn.execute("UPDATE messages SET content=? WHERE id=?",
                             (new_reply, row["id"]))
        conn.close()
    except Exception as e:
        app.logger.warning("step 1.5 reply rewrite (DB) failed: %s", e)

    g.captured_reply = new_reply
    app.logger.info("Injected STEP 1.5 reply (overrode LLM email ask) for %s", from_number)
    return True


def _silent_reply() -> str:
    """Intentional no-reply: empty TwiML for SMS, silent flag for widget."""
    try:
        g.captured_reply = ""
        g.captured_primer = None
        g.captured_silent = True
    except RuntimeError:
        pass
    return str(MessagingResponse())


# Terse acknowledgments after a confirmed appointment - the customer is just
# being polite, no reply needed. Matched only when the message is JUST the ack.
_TERSE_ACK_RE = re.compile(
    r"^\s*(?:"
    r"ok(?:ay)?|kk?|k+|"
    r"thanks?|thank\s+you|ty|tysm|thx|"
    r"sounds?\s+(?:good|great|fine|nice)|"
    r"perfect|great|cool|nice|awesome|sweet|"
    r"got\s+it|gotcha|understood|noted|alright|all\s+right|"
    r"see\s+(?:you|ya|u)(?:\s+(?:then|tomorrow|tmr|tmrw|soon|there))?"
    r")[\s!.,?👍🙏👌✅]*$",
    re.I,
)


@app.route("/sms", methods=["POST"])
def sms_webhook():
    body        = (request.form.get("Body") or "").strip()
    from_number = normalize_phone(request.form.get("From") or "")
    to_number   = normalize_phone(request.form.get("To")   or "")
    if not from_number or not to_number:
        twiml = MessagingResponse()
        twiml.message("Sorry - missing phone routing info.")
        return str(twiml)

    # Per-phone SMS abuse cap (resets on bot restart). Count keyed on the real
    # customer phone, not the widget-bridged pseudo-phone, so abuse isn't
    # bypassed by switching channels.
    with _sms_abuse_lock:
        _sms_abuse_counts[(from_number, to_number)] = (
            _sms_abuse_counts.get((from_number, to_number), 0) + 1
        )
        count = _sms_abuse_counts[(from_number, to_number)]
    if count > SMS_ABUSE_LIMIT + 1:
        # Cap already enforced once; stay silent.
        return str(MessagingResponse())
    if count == SMS_ABUSE_LIMIT + 1:
        # Look up the dealer phone for this twilio number so the notice tells
        # the customer who to call. read_dealers() is cached, so this is cheap.
        dealer_phone = ""
        try:
            dealer_row = select_dealer_for_twilio_number(read_dealers(), to_number)
            dealer_phone = normalize_phone(get_row_field(dealer_row, DEALER_NOTIFY_PHONE_ALIASES))
        except Exception as e:
            app.logger.warning("SMS abuse notice: dealer lookup failed: %s", e)
        notice = (
            f"You have reached the message limit for this number. "
            f"Please call the dealer directly at {dealer_phone} if you have any more questions."
            if dealer_phone else SMS_ABUSE_NOTICE
        )
        twiml = MessagingResponse()
        twiml.message(notice)
        return str(twiml)

    # Mark this request as inbound SMS so notify_customer_appointment can
    # skip its duplicate text (the bot's reply goes back to the customer's
    # phone automatically via TwiML).
    g.is_sms_request = True
    # Bridge: if this real phone previously used the widget for this dealer,
    # route the SMS into that widget session so reschedules/cancels can find
    # the appointment (which was saved under the +web<sessionid> pseudo-phone).
    widget_session = find_widget_session_for_real_phone(from_number, to_number)
    if widget_session:
        app.logger.info("SMS from %s bridged to widget session %s", from_number, widget_session)
        from_number = widget_session
    return _process_message(from_number, to_number, body)


_PHONE_RE = re.compile(
    r"(?:\+?1[\s\-.]?)?\(?(\d{3})\)?[\s\-.]?(\d{3})[\s\-.]?(\d{4})"
)
_NAME_INTRO_RE = re.compile(
    r"(?:my\s+name\s+is|name'?s|i\s*am|i'?m|im|this\s+is|it'?s|its)\s+"
    r"([A-Za-z][A-Za-z'\-]+)(?:\s+([A-Za-z][A-Za-z'\-]+))?",
    re.I,
)


def _extract_phone_us(text: str) -> str:
    """Return +1XXXXXXXXXX if a 10-digit US phone is present, else ''."""
    m = _PHONE_RE.search(text or "")
    if not m:
        return ""
    digits = m.group(1) + m.group(2) + m.group(3)
    return "+1" + digits if len(digits) == 10 else ""


_NAME_FILLER_WORDS = {
    # conjunctions / prepositions / articles
    "and", "but", "plus", "or", "also", "with", "the", "a", "an",
    "from", "to", "of", "for", "by", "in", "on", "at", "as",
    # demonstratives / contractions / common sentence-starters
    "this", "that", "these", "those", "thats", "this's",
    "its", "it's", "im", "i'm", "ive", "i've",
    # possessives / pronouns
    "my", "his", "her", "our", "their", "your", "i", "you", "he",
    "she", "it", "we", "they", "me", "him", "us", "them",
    # forms of "to be" / common verbs that should never start a name
    "is", "are", "was", "were", "be", "been", "being", "am",
    "have", "has", "had", "do", "does", "did", "doing",
    "can", "could", "will", "would", "should", "shall", "may",
    "might", "must", "want", "need", "looking", "interested",
    "see", "show", "tell", "give", "find", "get", "got",
    "buy", "sell", "schedule", "book", "test", "drive",
    # question words
    "what", "who", "whom", "when", "where", "why", "how", "which",
    "whats", "hows", "whens", "wheres",
    # form labels / generic chrome
    "number", "phone", "name", "names", "email", "here", "there",
    "first", "last", "mr", "mrs", "ms", "dr",
    # car-domain words that often appear in non-name questions
    "car", "cars", "truck", "trucks", "suv", "suvs", "sedan",
    "vehicle", "vehicles", "inventory", "available", "price",
    "between", "under", "over",
}


def _looks_like_real_name(word: str) -> bool:
    return bool(word) and word.lower() not in _NAME_FILLER_WORDS and is_valid_name(word)


def _extract_name_parts(text: str) -> Tuple[str, str]:
    """Return (first, last) extracted from text, or ('', '') if no confident match.

    Two strategies:
      1. Intro-phrase match: 'my name is X Y', 'i'm X Y', 'im X', 'this is X Y'.
      2. Leading-words fallback: take the first 1-2 alpha words and treat them
         as a name if they pass _looks_like_real_name. This lets us extract
         names from messages like 'evan lee and my number is 5551234567'
         where filler words appear LATER in the sentence.
    """
    cleaned = _PHONE_RE.sub(" ", text or "").strip()
    intro = _NAME_INTRO_RE.search(cleaned)
    if intro:
        first = (intro.group(1) or "").strip()
        last  = (intro.group(2) or "").strip()
        if _looks_like_real_name(first):
            return first, (last if _looks_like_real_name(last) else "")
    words = re.findall(r"\b[A-Za-z][A-Za-z'\-]*\b", cleaned)
    if not words or not _looks_like_real_name(words[0]):
        return "", ""
    first = words[0]
    last  = words[1] if len(words) > 1 and _looks_like_real_name(words[1]) else ""
    return first, last


def _process_message(from_number: str, to_number: str, body: str):
    """Shared routing - both /sms and /chat funnel through here. Returns TwiML
    string (used by /sms). The /chat endpoint reads g.captured_reply instead."""
    app.logger.info("Inbound from %s: %r", from_number, body)
    new_customer = not has_primer_been_sent(from_number, to_number)
    clear_cold_followup(from_number, to_number)
    save_message(from_number, to_number, "user", body)

    customer_profile = get_customer_profile(from_number, to_number)
    customer_name = customer_profile["name"]

    # ── PRIORITY 0: Widget profile gate. Customer must provide first name
    # AND a real phone before the bot will answer anything. Last name is no
    # longer required at this gate - it's collected later (during STEP 2 of
    # the booking flow) since the in-chat profile form only takes first name
    # and phone. SMS users are exempt (their phone is the From number).
    is_widget = from_number.startswith("+web")
    def _profile_incomplete(p: Dict[str, str]) -> bool:
        return not (p.get("name") and p.get("real_phone"))
    # Sanitize previously-saved name fields: if they look like junk (e.g.
    # extracted from a question before the filler list was tightened), drop
    # them so the gate re-prompts and we can capture the real name.
    if is_widget:
        sanitize_kwargs: Dict[str, Any] = {}
        existing_name = (customer_profile.get("name") or "").strip()
        existing_last = (customer_profile.get("last_name") or "").strip()
        if existing_name and not _looks_like_real_name(existing_name):
            sanitize_kwargs["name"] = ""
        if existing_last and not _looks_like_real_name(existing_last):
            sanitize_kwargs["last_name"] = ""
        if sanitize_kwargs:
            save_customer_profile(from_number, to_number, **sanitize_kwargs)
            customer_profile = get_customer_profile(from_number, to_number)
            customer_name = customer_profile["name"]
            app.logger.info("Sanitized junk name fields for %s: %s", from_number, sanitize_kwargs)
    if is_widget and _profile_incomplete(customer_profile):
        save_kwargs: Dict[str, Any] = {}
        if not customer_profile.get("real_phone"):
            ph = _extract_phone_us(body)
            if ph:
                save_kwargs["real_phone"] = ph
        if not customer_profile.get("name") or not customer_profile.get("last_name"):
            first, last = _extract_name_parts(body)
            if first and not customer_profile.get("name"):
                save_kwargs["name"] = first
                if last and not customer_profile.get("last_name"):
                    save_kwargs["last_name"] = last
            elif first and customer_profile.get("name") and not customer_profile.get("last_name"):
                # First name already on file; customer is now providing the
                # last name as a single word ("lee") -> save it as last_name.
                save_kwargs["last_name"] = first
        if save_kwargs:
            save_customer_profile(from_number, to_number, **save_kwargs)
            customer_profile = get_customer_profile(from_number, to_number)
            customer_name = customer_profile["name"]

            # First-time phone capture for this widget session: log the
            # terms-acceptance + phone submission. The helper handles
            # once-per-phone via INSERT OR IGNORE, so calling it here is
            # safe even on later gate passes.
            if "real_phone" in save_kwargs:
                try:
                    _dealers_for_log = read_dealers()
                    _dealer_for_log  = select_dealer_for_twilio_number(_dealers_for_log, to_number)
                    _dealer_name_log = get_row_field(_dealer_for_log, DEALER_NAME_ALIASES) if _dealer_for_log else ""
                except Exception:
                    _dealer_name_log = ""
                log_terms_acceptance(
                    real_phone=customer_profile.get("real_phone", ""),
                    first_name=customer_profile.get("name", ""),
                    last_name=customer_profile.get("last_name", ""),
                    dealer_name=_dealer_name_log,
                    twilio_number=to_number,
                )

        if _profile_incomplete(customer_profile):
            missing = []
            if not customer_profile.get("name"):       missing.append("first name")
            if not customer_profile.get("real_phone"): missing.append("phone number")
            if len(missing) == 1:
                missing_str = missing[0]
            elif len(missing) == 2:
                missing_str = " and ".join(missing)
            else:
                missing_str = ", ".join(missing[:-1]) + ", and " + missing[-1]
            reply = (
                f"Before I can help, could I please get your {missing_str}? "
                "We use these to text you appointment confirmations and follow up "
                "if you have questions later."
            )
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number)

        # Deferred-question recovery. If the gate just unlocked (profile became
        # complete with this message) AND the customer's PRIOR message was a
        # topic question we'd normally route through a dedicated handler
        # (financing / trade-in / etc.), retroactively answer it now using the
        # right handler so they don't need to re-ask.
        recent_for_defer = get_recent_messages(from_number, to_number, limit=6)
        prior_user_msg = ""
        for m in reversed(recent_for_defer[:-1]):  # skip the just-saved current body
            if m.get("role") == "user":
                prior_user_msg = (m.get("content") or "").strip()
                break
        if prior_user_msg and _is_financing_question(prior_user_msg):
            try:
                _dealers_d = read_dealers()
                _dealer_row_d = select_dealer_for_twilio_number(_dealers_d, to_number)
                _dealer_phone_d = normalize_phone(get_row_field(_dealer_row_d, DEALER_NOTIFY_PHONE_ALIASES))
                _financing_d = get_row_field(_dealer_row_d, DEALER_FINANCING_ALIASES)
            except Exception:
                _dealer_row_d, _dealer_phone_d, _financing_d = {}, "", ""
            if _financing_d:
                history_d = get_recent_messages(from_number, to_number, limit=8)
                deferred_reply = ai_policy_reply(
                    prior_user_msg, "financing", _financing_d, _dealer_phone_d,
                    history_d, customer_name=customer_name,
                ) or f"Regarding financing: {_financing_d}."
                first_for_intro = (customer_profile.get("name") or "").strip()
                intro = f"Thanks, {first_for_intro}! " if first_for_intro else "Thanks! "
                full_reply = intro + deferred_reply
                save_message(from_number, to_number, "assistant", full_reply)
                return _reply_twiml(full_reply, from_number, to_number)

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
                customer_phone=resolve_outbound_customer_phone(from_number, to_number) or from_number, customer_name=customer_name,
                customer_last_name=customer_profile["last_name"],
                customer_email=customer_profile["email"],
                dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
            ))
            notify_customer_appointment(dealer_row, customer_phone=from_number,
                twilio_number=to_number, customer_name=customer_name,
                visit_time=visit_time, car_desc=car_desc, action="cancelled")
            reply = "Understood - we have removed that appointment. When would you prefer to reschedule your visit?"
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        if YES_RE.search(body):
            clear_pending_reconfirmation(from_number, to_number)
            mark_reconfirmed(appointment_id)
            notify_all_staff(dealer_row, to_number, _dealer_reconfirm_body(
                customer_phone=resolve_outbound_customer_phone(from_number, to_number) or from_number, customer_name=customer_name,
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
        email_just_captured = False
        if email_scan and not customer_profile["email"] and is_valid_email(email_scan.group(0)):
            save_customer_profile(from_number, to_number, email=email_scan.group(0))
            customer_profile = get_customer_profile(from_number, to_number)
            customer_name = customer_profile["name"]
            email_just_captured = True

        # Auto-book on email capture: when the customer provides their email
        # and that completes the profile (and any in-progress trade-in info is
        # also done), don't make them re-confirm with "yes/no" - just log the
        # appointment and send the confirmation. This is the user's preferred
        # flow: email → booked.
        if email_just_captured and not missing_profile_field(customer_profile):
            _trade_on_file_now = (customer_profile.get("trade_in_vehicle") or "").strip()
            _history_for_trade = get_recent_messages(from_number, to_number, limit=14)
            _trade_missing_now = _trade_in_missing_parts(_history_for_trade) if _trade_on_file_now else []
            if not _trade_missing_now:
                pending_notify_phone = normalize_phone(pending.get("dealer_notify_phone", "")) or dealer_phone
                visit_time = pending["visit_time"]
                visit_time_iso = pending.get("visit_time_iso", "")
                car_desc = pending["car_desc"]
                appt_id, is_reschedule = log_appointment(
                    from_number, to_number, pending_notify_phone,
                    visit_time, visit_time_iso, car_desc,
                )
                clear_pending(from_number, to_number)
                additional_info = extract_customer_insights(get_recent_messages(from_number, to_number, limit=20))
                _alert_phone = resolve_outbound_customer_phone(from_number, to_number) or from_number
                alert_body = (
                    _dealer_reschedule_body(customer_phone=_alert_phone, customer_name=customer_name,
                                            customer_last_name=customer_profile["last_name"],
                                            customer_email=customer_profile["email"],
                                            dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                            additional_info=additional_info)
                    if is_reschedule else
                    _dealer_alert_body(customer_phone=_alert_phone, customer_name=customer_name,
                                       customer_last_name=customer_profile["last_name"],
                                       customer_email=customer_profile["email"],
                                       dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                       additional_info=additional_info)
                )
                notify_all_staff(dealer_row, to_number, alert_body)
                notify_customer_appointment(dealer_row, customer_phone=from_number,
                    twilio_number=to_number, customer_name=customer_name,
                    visit_time=visit_time, car_desc=car_desc,
                    action=("rescheduled" if is_reschedule else "confirmed"))
                _is_general = car_desc.lower().strip() in {"", "general visit", "general", "visit", "a vehicle"}
                if _is_general:
                    reply = f"You're all set, {customer_name}! Your appointment is confirmed for {visit_time}. We look forward to seeing you!"
                else:
                    reply = f"You're all set, {customer_name}! Your appointment is confirmed for {visit_time} to view the {car_desc}. We look forward to seeing you!"
                save_message(from_number, to_number, "assistant", reply)
                return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        # Cancellation signal: only treat the message as a cancel if it's a SHORT
        # "no" reply (no other meaningful content). The previous broad NO_RE
        # match clobbered the pending appointment whenever the customer's
        # message happened to contain "no", "not", or "don't" as part of a
        # larger answer (e.g. "no questions, but I have a trade-in"). Now we
        # require a terse no-only message OR a strong disinterest phrase.
        body_compact = re.sub(r"[^\w\s]", "", body).strip().lower()
        is_short_no = bool(re.fullmatch(r"(no|nah|nope|cancel|never\s*mind|nevermind|forget\s*it)\b\s*\.?", body_compact))
        if is_short_no or DISINTEREST_RE.search(body):
            clear_pending(from_number, to_number)
            reply = "Of course - what time would work best for you?"
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        # Detect financing interest UP FRONT so it isn't swallowed by the
        # YES_RE/affirmation match below. Customers often phrase financing
        # answers as "ok great i'd also like to finance it" - YES_RE matches
        # "ok great" and the financing piece gets dropped.
        _financing_keywords_re = re.compile(
            r"\b(financ\w*|loan\w*|monthly\s+payment|down\s+payment|apr|interest\s+rate|"
            r"interested\s+in\s+financ\w*)\b",
            re.I,
        )
        _early_financing_mention = bool(_financing_keywords_re.search(body))

        # Customers often type "yes" alongside a question or correction
        # ("yes i have a question", "no i mean yes i have questions", "wait
        # actually"). YES_RE alone would treat the whole message as
        # confirmation and skip the question. Exempt the YES_RE branch when
        # the message also signals a question or a self-correction.
        _early_question_signal = bool(re.search(
            r"\b(i\s+(?:have|got|gotta\s+ask|wanted\s+to\s+ask|need\s+to\s+ask)\s+"
            r"(?:a\s+|some\s+|few\s+|another\s+|more\s+|other\s+)?(?:question|q)s?|"
            r"have\s+(?:a\s+|some\s+|few\s+|another\s+|more\s+|other\s+)?questions?|"
            r"got\s+(?:a\s+|some\s+|few\s+|another\s+|more\s+|other\s+)?questions?|"
            r"can\s+i\s+ask|"
            r"i\s+(?:meant|mean)|wait|hold\s+on|actually|one\s+more\s+thing|"
            r"quick\s+question|another\s+question)\b",
            body, re.I,
        ))

        # Bare "yes" right after STEP 1.5 (which asked about questions /
        # financing / trade-in) is ambiguous — could mean any of the three.
        # Disambiguate instead of silently advancing the booking.
        _last_asst_for_step15 = ""
        for _m in reversed(get_recent_messages(from_number, to_number, limit=4)):
            if _m.get("role") == "assistant":
                _last_asst_for_step15 = (_m.get("content") or "").lower()
                break
        _last_was_step15 = (
            "any other questions about it" in _last_asst_for_step15
            and "financing" in _last_asst_for_step15
            and ("trade-in" in _last_asst_for_step15 or "trade in" in _last_asst_for_step15)
        )
        _body_stripped_compact = re.sub(r"[^\w\s]", "", body).strip().lower()
        _is_bare_yes = bool(re.fullmatch(
            r"(yes|yep|yeah|yup|sure|definitely|absolutely|ok|okay)\s*",
            _body_stripped_compact,
        ))
        if _last_was_step15 and _is_bare_yes:
            reply = (
                "Sure — which one: more questions about the vehicle, "
                "interested in financing, or do you have a trade-in?"
            )
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        if YES_RE.search(body) and not _early_financing_mention and not _early_question_signal:
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
                _dealer_reschedule_body(customer_phone=resolve_outbound_customer_phone(from_number, to_number) or from_number, customer_name=customer_name,
                                        customer_last_name=customer_profile["last_name"],
                                        customer_email=customer_profile["email"],
                                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                        additional_info=additional_info)
                if is_reschedule else
                _dealer_alert_body(customer_phone=resolve_outbound_customer_phone(from_number, to_number) or from_number, customer_name=customer_name,
                                   customer_last_name=customer_profile["last_name"],
                                   customer_email=customer_profile["email"],
                                   dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                   additional_info=additional_info)
            )
            notify_all_staff(dealer_row, to_number, alert_body)
            notify_customer_appointment(dealer_row, customer_phone=from_number,
                twilio_number=to_number, customer_name=customer_name,
                visit_time=visit_time, car_desc=car_desc,
                action=("rescheduled" if is_reschedule else "confirmed"))

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
        # Treat a short message as a name reply ONLY if either (a) we extracted
        # a real name from it, or (b) the customer doesn't have a name on file
        # yet (so the bot would actually be asking). Without this check, any
        # short question like "can i pay cash?" or "any rust?" trips
        # is_likely_name and gets answered with a flat email-ask, ignoring
        # the actual question. The previous logic always entered the block on
        # shape alone and returned the email prompt regardless.
        treat_as_name_reply = False
        save_kwargs: Dict[str, Any] = {}
        if is_likely_name:
            tokens = [t for t in body.strip().split() if t]
            new_first = tokens[0].title() if tokens else ""
            new_last = tokens[1].title() if len(tokens) >= 2 else None
            # Use the stricter _looks_like_real_name (filler-word check) so
            # phrases like "its in immaculate condition" don't get saved as
            # name="Its" / last_name="Condition" during a pending-confirm flow.
            valid_first = _looks_like_real_name(new_first) if new_first else False
            valid_last  = _looks_like_real_name(new_last) if new_last else False
            # Never overwrite an already-populated profile field. The customer
            # provided their real name earlier (via the widget profile form or
            # an explicit intro); a casual message in the middle of a booking
            # confirmation must not clobber it.
            if valid_first and not (customer_profile.get("name") or "").strip():
                save_kwargs["name"] = new_first
            if valid_last and not (customer_profile.get("last_name") or "").strip():
                save_kwargs["last_name"] = new_last
            has_name_on_file = bool((customer_profile.get("name") or "").strip())
            treat_as_name_reply = bool(save_kwargs) or not has_name_on_file
            if save_kwargs:
                save_customer_profile(from_number, to_number, **save_kwargs)
                customer_profile = get_customer_profile(from_number, to_number)
                customer_name = customer_profile["name"]
        if treat_as_name_reply:

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
                _dealer_reschedule_body(customer_phone=resolve_outbound_customer_phone(from_number, to_number) or from_number, customer_name=customer_name,
                                        customer_last_name=customer_profile["last_name"],
                                        customer_email=customer_profile["email"],
                                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                        additional_info=additional_info)
                if is_reschedule else
                _dealer_alert_body(customer_phone=resolve_outbound_customer_phone(from_number, to_number) or from_number, customer_name=customer_name,
                                   customer_last_name=customer_profile["last_name"],
                                   customer_email=customer_profile["email"],
                                   dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
                                   additional_info=additional_info)
            )
            notify_all_staff(dealer_row, to_number, alert_body)
            notify_customer_appointment(dealer_row, customer_phone=from_number,
                twilio_number=to_number, customer_name=customer_name,
                visit_time=visit_time, car_desc=car_desc,
                action=("rescheduled" if is_reschedule else "confirmed"))
            reply = f"Perfect, {customer_name}! You're all set for {visit_time} to see the {car_desc}. We look forward to seeing you!"
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        # Vague trade-in mention? If the customer says they have a trade-in but
        # didn't include a year/make/model, ask for the vehicle BEFORE we move
        # on to ask for missing profile fields. The dealer needs to know what
        # they're appraising.
        _trade_keywords_re = re.compile(
            r"\b(trade|trading)[\s\-]?in\b|\bhave\s+a\s+trade\b|\btrade\s+something|\bgot\s+a\s+trade\b",
            re.I,
        )
        _vehicle_year_re = re.compile(r"\b(19|20)\d{2}\b")
        _vehicle_make_re = re.compile(
            r"\b(toyota|honda|ford|chevrolet|chevy|gmc|jeep|dodge|ram|chrysler|nissan|"
            r"hyundai|kia|mazda|subaru|volkswagen|vw|bmw|mercedes|mercedes-benz|audi|"
            r"lexus|infiniti|acura|cadillac|buick|lincoln|porsche|land[\s-]*rover|"
            r"range[\s-]*rover|tesla|volvo|mitsubishi|fiat|alfa|mini|maserati|jaguar|"
            r"bentley|rolls|smart|saab|pontiac|saturn|hummer|isuzu|suzuki|genesis|"
            r"polestar|rivian|lucid|aston[\s-]*martin|ferrari|lamborghini|mclaren)\b",
            re.I,
        )
        mentions_trade = bool(_trade_keywords_re.search(body))
        has_vehicle_specifier = bool(_vehicle_year_re.search(body)) or bool(_vehicle_make_re.search(body))
        already_has_trade_on_file = bool((customer_profile.get("trade_in_vehicle") or "").strip())

        # Customers ask questions about the BUY vehicle ("does the honda have 4wd")
        # right after the bot asks about trade-ins. The make match alone shouldn't
        # pull them into the trade flow. Detect question-shaped messages so we can
        # short-circuit the trade-in branch and let the LLM answer the question.
        _looks_like_buy_question = (
            ("?" in body)
            or bool(re.match(
                r"^\s*(does|do|is|are|can|will|what|how|why|when|where|which|who|"
                r"tell\s+me|got\s+a\s+question|i'?m\s+asking|im\s+asking)\b",
                body, re.I,
            ))
            # Also catch mid-sentence question constructs that come after a
            # softening preamble ("before i do that does the car...", "wait
            # what about the mileage"). The earlier start-of-body regex misses
            # these because the question word isn't at position 0.
            or bool(re.search(
                r"\b(does\s+(?:it|the\s+\w+|that|this)|"
                r"is\s+(?:it|the\s+\w+|that|this|there)|"
                r"are\s+(?:there|those|these|they)|"
                r"any\s+(?:issues?|problems?|known\s+issues?|recalls?|"
                r"accidents?|damage|fees?|costs?|extras?)|"
                r"what\s+about|how\s+(?:much|many|old|long|far))\b",
                body, re.I,
            ))
        ) and not mentions_trade
        # Also detect when the customer references the PENDING buy vehicle by
        # make/model - if the make matches what they're booking AND no different
        # year/model is named, the message is about that vehicle.
        _pending_car_desc_lower = (pending.get("car_desc", "") or "").lower()
        _stop_tokens = {"the", "and", "have", "has", "does", "this", "that",
                        "with", "for", "any", "still", "year", "make", "model"}
        _body_refs_buy_car = bool(_pending_car_desc_lower) and any(
            tok in _pending_car_desc_lower
            for tok in re.findall(r"\b[a-z]{3,}\b", body.lower())
            if tok not in _stop_tokens
        )
        # If body has a YEAR different from pending's year, the customer is
        # naming a different vehicle (e.g. a trade-in) — NOT the buy car.
        _pending_year_m = re.search(r"\b(19|20)\d{2}\b", _pending_car_desc_lower)
        _body_year_m    = re.search(r"\b(19|20)\d{2}\b", body)
        if _pending_year_m and _body_year_m and _pending_year_m.group(0) != _body_year_m.group(0):
            _body_refs_buy_car = False
        # If the bot's immediately prior message was asking for the TRADE vehicle
        # itself ("what vehicle would you like to trade in?"), the next answer
        # naming a year/make IS the trade-in answer — don't treat it as a buy
        # question even if the make overlaps with the pending car.
        _last_asst_lower = ""
        for _m in reversed(get_recent_messages(from_number, to_number, limit=4)):
            if _m.get("role") == "assistant":
                _last_asst_lower = (_m.get("content") or "").lower()
                break
        _bot_just_asked_for_trade_vehicle = (
            "what vehicle would you like to trade in" in _last_asst_lower
            or "(year, make, and model" in _last_asst_lower
        )
        _is_buy_side_message = (
            _looks_like_buy_question or (_body_refs_buy_car and not mentions_trade)
        ) and not _bot_just_asked_for_trade_vehicle

        # Recovery: if we previously saved a trade-in but the customer is clearly
        # asking about the buy vehicle now, that earlier save was a false positive
        # (e.g. "does the honda have 4wd" misread as a Honda trade). Wipe it so the
        # next turns route correctly.
        if already_has_trade_on_file and _is_buy_side_message:
            save_customer_profile(from_number, to_number, trade_in_vehicle="")
            customer_profile = get_customer_profile(from_number, to_number)
            already_has_trade_on_file = False

        if mentions_trade and not has_vehicle_specifier and not already_has_trade_on_file:
            reply = "Got it - what vehicle would you like to trade in? (year, make, and model if you have it)"
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        # Customer message includes trade-in details (year+make, even without the
        # word "trade" if it follows a bot question about trade-in) - extract and
        # save the trade-in vehicle so it shows up on the dealer alert. Skip when
        # the message is clearly a buy-side question so we don't misread it.
        if (has_vehicle_specifier or mentions_trade or already_has_trade_on_file) and not _is_buy_side_message:
            try:
                history = get_recent_messages(from_number, to_number, limit=14)
                candidate_trade_in = extract_trade_in_vehicle(history)
                # If the LLM extract missed the condition phrase (common when
                # customer phrases it conversationally), splice it in.
                if candidate_trade_in:
                    candidate_trade_in = _augment_trade_in_with_condition(candidate_trade_in, history)
                existing = (customer_profile.get("trade_in_vehicle") or "").strip()
                if candidate_trade_in and candidate_trade_in != existing:
                    save_customer_profile(from_number, to_number, trade_in_vehicle=candidate_trade_in)
                    customer_profile = get_customer_profile(from_number, to_number)
                    app.logger.info("Captured/updated trade-in vehicle for %s: %s",
                                    from_number, candidate_trade_in)
            except Exception as e:
                app.logger.warning("Trade-in extraction failed in pending block: %s", e)

        next_missing = missing_profile_field(customer_profile)

        # Detect if the customer just expressed financing interest. The pending
        # block intercepts the message before the LLM can craft a tailored
        # reply, so without this we'd fall through to a flat "Thanks!" without
        # acknowledging the financing question at all.
        _financing_keywords_re = re.compile(
            r"\b(financ\w*|loan\w*|monthly\s+payment|down\s+payment|apr|interest\s+rate|"
            r"interested\s+in\s+financ\w*)\b",
            re.I,
        )
        mentions_financing = bool(_financing_keywords_re.search(body))

        # If a trade-in is in play, prefer the existing
        # `deterministic_trade_in_followup` flow which collects the dealer's
        # appraisal-relevant details (mileage, title status, condition) before
        # we ask for the email. Only fall through to the email/confirm reply
        # once those details have been gathered.
        # Treat the conversation as trade-in context if EITHER the customer
        # explicitly said "trade", or a trade is on file from a prior turn,
        # OR the customer just gave a vehicle specifier (year and/or make)
        # right after the bot asked about trade-ins. This catches replies
        # like "i have a 2012 honda accord" that don't include the word
        # "trade" but are clearly trade-in answers.
        _bot_just_asked_trade_in = False
        for m in get_recent_messages(from_number, to_number, limit=4):
            if m.get("role") == "assistant":
                _c = (m.get("content") or "").lower()
                if "trade-in" in _c or "trade in" in _c:
                    _bot_just_asked_trade_in = True
                    break
        _trade_on_file = (customer_profile.get("trade_in_vehicle") or "").strip()
        _trade_active = (
            bool(_trade_on_file)
            or mentions_trade
            or (has_vehicle_specifier and _bot_just_asked_trade_in)
        ) and not _is_buy_side_message
        if _trade_active:
            history_for_trade = get_recent_messages(from_number, to_number, limit=14)
            trade_missing_parts = _trade_in_missing_parts(history_for_trade)
            # Count how many trade-in detail follow-ups we've already sent so
            # we vary the phrasing and stop after 3 rounds (so a customer with
            # an unusual answer doesn't get trapped).
            _ask_phrases = ("round out the ballpark", "got it - and what", "got it - one more", "and the ")
            followup_count = sum(
                1 for m in history_for_trade if m.get("role") == "assistant"
                and any(p in (m.get("content") or "").lower() for p in _ask_phrases)
            )
            if trade_missing_parts and followup_count < 3:
                if followup_count == 0:
                    reply = (
                        f"Thanks for sharing that. To round out the ballpark, could "
                        f"you also share the {_format_missing_list(trade_missing_parts)}?"
                    )
                elif followup_count == 1:
                    if len(trade_missing_parts) == 1:
                        reply = f"Got it - and what's the {trade_missing_parts[0]}?"
                    else:
                        reply = f"Got it - one more thing - the {_format_missing_list(trade_missing_parts)}?"
                else:
                    if len(trade_missing_parts) == 1:
                        reply = f"And the {trade_missing_parts[0]}?"
                    else:
                        reply = f"And the {_format_missing_list(trade_missing_parts)}?"
                save_message(from_number, to_number, "assistant", reply)
                return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)
            # All trade-in details captured - acknowledge and proceed to the
            # email request (or final confirmation if email is already on file).
            if next_missing:
                reply = (
                    f"Got it - we'll have someone take a look at the {_trade_on_file or 'trade-in'}. "
                    f"To lock in {pending['visit_time']} for the {pending['car_desc']}, "
                    f"could I get your {next_missing}?"
                )
            else:
                reply = (
                    f"Got it - we'll have someone take a look at the {_trade_on_file or 'trade-in'}. "
                    f"To confirm - shall I keep your appointment at {pending['visit_time']} "
                    f"for the {pending['car_desc']}? Reply Yes or No."
                )
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        # If the customer is asking an actual question (about the vehicle,
        # policies, etc.) and it doesn't match the trade-in/financing/yes/no
        # patterns we've already handled, fall through to the LLM so it can
        # answer properly. Without this, the pending block always intercepts
        # with the email-ask catch-all and ignores the customer's question.
        _looks_like_question = (
            "?" in body
            or bool(re.match(
                r"^\s*(what|how|is|does|can|do|are|will|won't|why|when|where|which|who|"
                r"any|tell\s+me|i\s+have\s+a\s+question|got\s+a\s+question)\b",
                body, re.I,
            ))
        )
        if (_looks_like_question or _is_buy_side_message or _early_question_signal) and not mentions_financing and not _trade_active:
            # Skip the catch-all and let _process_message continue to the LLM.
            # The pending appointment stays set so the LLM sees it via history
            # and the next-turn flow still works. Fires for buy-side questions
            # ("does the honda have 4wd"), question-signal phrases ("i have
            # questions", "wait actually"), and self-corrections — so they
            # don't get swallowed by the email-ask catch-all.
            _pending_skip_catchall = True
        else:
            _pending_skip_catchall = False

        # If the customer mentioned financing, acknowledge that we offer it and
        # the team will go over options at the visit, then ask for email. If
        # the dealer's financing policy text contains a URL, surface it as a
        # preapproval link the customer can start before the visit.
        if mentions_financing:
            _financing_policy = (get_row_field(dealer_row, DEALER_FINANCING_ALIASES) or "").strip()
            _preapproval_url_m = re.search(r"https?://\S+", _financing_policy) if _financing_policy else None
            _preapproval_url = _preapproval_url_m.group(0).rstrip(".,)") if _preapproval_url_m else ""
            if _preapproval_url:
                _financing_line = (
                    f"Got it - we offer financing and the team will go over options with you "
                    f"when you're here. If you'd like, you can get preapproved beforehand here: "
                    f"{_preapproval_url}"
                )
            else:
                _financing_line = (
                    f"Got it - we offer financing and the team will go over options with you "
                    f"when you're here."
                )
            if next_missing:
                reply = (
                    f"{_financing_line} To lock in {pending['visit_time']} for the "
                    f"{pending['car_desc']}, could I get your {next_missing}?"
                )
            else:
                reply = (
                    f"{_financing_line} To confirm - shall I keep your appointment at "
                    f"{pending['visit_time']} for the {pending['car_desc']}? Reply Yes or No."
                )
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)

        if not _pending_skip_catchall:
            if next_missing:
                reply = (f"Thanks! Could I also get your {next_missing} so I can lock in "
                         f"{pending['visit_time']} for the {pending['car_desc']}?")
            else:
                reply = (f"To confirm - shall I keep your appointment at {pending['visit_time']} "
                         f"for the {pending['car_desc']}? Reply Yes or No.")
            save_message(from_number, to_number, "assistant", reply)
            return _reply_twiml(reply, from_number, to_number, send_primer=new_customer)
        # else: fall through to LLM so a customer question gets a real answer

    # ── PRIORITY 2.5: Pending cancellation ───────────────────────────────
    pending_cancel = get_pending_cancellation(from_number, to_number)
    if pending_cancel:
        visit_time, car_desc = pending_cancel["visit_time"], pending_cancel["car_desc"]
        cancel_notify_phone = normalize_phone(pending_cancel.get("dealer_notify_phone", "")) or dealer_phone

        if YES_RE.search(body):
            cancel_appointment(from_number, to_number)
            clear_pending_cancellation(from_number, to_number)
            notify_all_staff(dealer_row, to_number, _dealer_cancellation_body(
                customer_phone=resolve_outbound_customer_phone(from_number, to_number) or from_number, customer_name=customer_name,
                customer_last_name=customer_profile["last_name"],
                customer_email=customer_profile["email"],
                dealership_line=to_number, visit_time=visit_time, car_desc=car_desc,
            ))
            notify_customer_appointment(dealer_row, customer_phone=from_number,
                twilio_number=to_number, customer_name=customer_name,
                visit_time=visit_time, car_desc=car_desc, action="cancelled")
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

    # ── PRIORITY 4.-1: Terse acknowledgment after a confirmed appointment.
    # The booking is locked and the confirmation already said "look forward
    # to seeing you" - "ok"/"thanks"/"sounds good" don't need a response.
    if confirmed_appt and _TERSE_ACK_RE.match(body):
        return _silent_reply()

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

    if _is_cash_payment_question(body):
        # Deterministic cash confirmation. LLM was making up payment policies
        # ("certified check or wire transfer") instead of just saying yes.
        # Tie it into the booking flow when there's a pending appointment.
        _pending_cash = get_pending(from_number, to_number)
        if _pending_cash:
            _missing_cash = missing_profile_field(customer_profile)
            if _missing_cash:
                reply_text = (f"Yes, we accept cash. To lock in {_pending_cash['visit_time']} "
                              f"for the {_pending_cash['car_desc']}, could I get your {_missing_cash}?")
            else:
                reply_text = (f"Yes, we accept cash. To confirm — shall I keep your appointment "
                              f"at {_pending_cash['visit_time']} for the {_pending_cash['car_desc']}? Reply Yes or No.")
        elif confirmed_appt:
            reply_text = (f"Yes, we accept cash. See you at {confirmed_appt['visit_time']} "
                          f"for the {confirmed_appt['car_desc']}.")
        else:
            reply_text = "Yes, we accept cash. Would you like to schedule a time to come in?"
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_financing_question(body):
        # If the customer's message ALSO contains a clear booking time (e.g.
        # "i can be there today at 5:30, financing with 600 credit score"),
        # let it fall through to the LLM/booking flow which can handle both
        # at once. The financing-only handler would otherwise reply passively
        # and ignore the appointment intent.
        _has_booking_time = bool(parse_visit_time_from_text(body)[0])
        if not _has_booking_time:
            financing = get_row_field(dealer_row, DEALER_FINANCING_ALIASES)
            if financing:
                history = get_recent_messages(from_number, to_number, limit=6)
                reply_text = ai_policy_reply(body, "financing", financing, dealer_phone, history, customer_name=customer_name) or f"Regarding financing: {financing}."
            else:
                reply_text = build_unknown_answer(dealer_phone)
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # Trade-in trigger: the customer's message mentions "trade" OR the bot's
    # last reply was clearly soliciting trade-in details (so the customer's
    # follow-up answer routes back into the trade-in handler instead of
    # falling through to the generic LLM, which loses the tightening rules).
    _last_bot_msg = next(
        (m.get("content", "") for m in reversed(get_recent_messages(from_number, to_number, limit=4))
         if m.get("role") == "assistant"),
        ""
    ).lower()
    # Specific phrases that mean the bot was clearly asking for trade-in
    # DETAILS (not just mentioning trade-in in passing during booking).
    _bot_just_asked_trade_in = bool(re.search(
        r"year,?\s*make,?\s*model.*(mileage|title|condition)|"
        r"round out the ballpark|"
        r"title status\s*\(clean/salvage/rebuilt\)|"
        r"mileage.*title.*condition|"
        r"share the mileage|"
        r"share the year",
        _last_bot_msg,
    ))
    # If the bot was actually asking for booking info (name/email/time), this
    # is the appointment flow - DON'T misroute the reply back to trade-in.
    _bot_asked_for_booking_info = bool(re.search(
        r"first name.*last name.*email|"
        r"could i (please )?get your (first|name|email)|"
        r"what time works|"
        r"what day this week works|"
        r"to (lock|finalize|confirm) (it |your |the )?(in|appointment)",
        _last_bot_msg,
    ))
    # Match noun forms ("trade-in", "trade in", "trade ins") AND verb forms
    # ("trade my X in", "trade in my X", "trading", "trading my car"). Without
    # the verb-form patterns, "I want to trade my vehicle in" misses entirely
    # and the bot skips collecting trade-in details.
    # Explicit trade-in mentions in the body always fire the trade-in handler,
    # even mid-booking. The booking-info gate only blocks the state-based
    # trigger (`_bot_just_asked_trade_in`) — without it, a customer saying
    # "i also have a trade in too" right after the bot asks for email falls
    # through to _is_dealer_info_question (which also matches the word
    # "trade-in") and dumps the flat policy text instead of asking what
    # vehicle they're trading in.
    _explicit_trade_word = bool(
        re.search(r"\btrade[- ]?ins?\b", body, re.I)
        or re.search(r"\btrade\s+(?:my|in|that|this|the|a|it)\b", body, re.I)
        or re.search(r"\btrading\b", body, re.I)
    )
    # Topic-pivot guard: if the bot just asked for trade-in details but the
    # customer's new message is a fresh question that contains NO trade-in
    # related content (no vehicle year, no make, no "trade" word), they're
    # changing topics. Don't loop the trade-in details ask at them — let the
    # message route to its actual handler (service question, financing,
    # etc.). Without this guard, "do you do mechanical work" gets misread as
    # a trade-in answer just because it landed after a trade-in prompt.
    _looks_like_new_question = (
        "?" in body
        or bool(re.match(
            r"^\s*(do|does|can|will|would|should|is|are|what|how|why|when|"
            r"where|which|who|tell\s+me|got\s+a\s+question)\b",
            body, re.I,
        ))
    )
    _body_has_trade_signal = (
        bool(re.search(r"\b(19|20)\d{2}\b", body))  # year
        or _explicit_trade_word
        or bool(re.search(
            r"\b(toyota|honda|ford|chevy|chevrolet|nissan|hyundai|kia|mazda|"
            r"subaru|bmw|mercedes|audi|vw|volkswagen|lexus|acura|infiniti|"
            r"cadillac|buick|gmc|jeep|chrysler|dodge|ram|lincoln|volvo|"
            r"porsche|tesla)\b",
            body, re.I,
        ))  # any common make
        or bool(re.search(r"\b\d{1,3}[,k]?\s*(?:k\s+)?miles?\b|\b\d{2,3}k\b", body, re.I))  # mileage
        or bool(re.search(r"\b(clean|salvage|rebuilt|branded|lien)\s+title\b", body, re.I))  # title status
    )
    _trade_in_state_pivot = (
        _bot_just_asked_trade_in
        and _looks_like_new_question
        and not _body_has_trade_signal
    )
    _trade_in_trigger = (
        _explicit_trade_word
        or (_bot_just_asked_trade_in
            and not _bot_asked_for_booking_info
            and not _trade_in_state_pivot)
    )
    if _trade_in_trigger:
        tradeins = get_row_field(dealer_row, DEALER_TRADEINS_ALIASES)
        history = get_recent_messages(from_number, to_number, limit=12)
        # Try to capture the trade-in vehicle if the customer has shared details.
        candidate_trade_in = extract_trade_in_vehicle(history + [{"role": "user", "content": body}])
        # Splice in condition info from history when the LLM extract drops it
        # (common when customer phrases condition conversationally).
        if candidate_trade_in:
            candidate_trade_in = _augment_trade_in_with_condition(
                candidate_trade_in, history + [{"role": "user", "content": body}]
            )
        has_trade_in_on_file = bool((customer_profile.get("trade_in_vehicle") or "").strip())

        if (tradeins and not has_trade_in_on_file and not candidate_trade_in
                and not _bot_just_asked_trade_in):
            # First trade-in inquiry with no details yet - answer deterministically
            # so menu option 3 and direct text both reliably collect car data.
            # The LLM-based path was sometimes giving a policy-only answer here.
            policy_clean = tradeins.rstrip(".") + "."
            reply_text = (
                f"{policy_clean} A firm offer requires an in-person inspection, but if you "
                f"share the year, make, model, mileage, title status (clean/salvage/rebuilt), "
                f"and overall condition, the dealer can have a ballpark in mind before you visit."
            )
        elif tradeins:
            # Bot already asked OR customer has shared partial info - use the
            # deterministic followup which scans for what's still missing and
            # pivots to scheduling once everything is collected. Treat a
            # PENDING appointment the same as a confirmed one for this purpose
            # so the trade-in followup ties to the visit the customer just
            # picked instead of asking them to pick another day.
            _appt_for_trade = confirmed_appt
            if not _appt_for_trade:
                _pending_for_trade = get_pending(from_number, to_number)
                if _pending_for_trade and _pending_for_trade.get("visit_time"):
                    _appt_for_trade = {
                        "visit_time": _pending_for_trade["visit_time"],
                        "car_desc": _pending_for_trade.get("car_desc", ""),
                    }
            reply_text = deterministic_trade_in_followup(candidate_trade_in, history, confirmed_appt=_appt_for_trade)
            if not reply_text:
                reply_text = ai_policy_reply(body, "trade-ins", tradeins, dealer_phone, history[-6:], customer_name=customer_name) or f"Regarding trade-ins: {tradeins}."
        else:
            reply_text = build_unknown_answer(dealer_phone)

        if candidate_trade_in and candidate_trade_in != (customer_profile.get("trade_in_vehicle") or ""):
            save_customer_profile(from_number, to_number, trade_in_vehicle=candidate_trade_in)
            app.logger.info("Recorded trade-in vehicle for %s: %s", from_number, candidate_trade_in)
            # If the customer already has a confirmed appointment, notify the
            # dealer that a trade-in was added to it so staff can prep an
            # appraisal in advance. Fires once per new trade-in summary.
            if confirmed_appt and confirmed_appt.get("visit_time"):
                _alert_outbound_phone = resolve_outbound_customer_phone(from_number, to_number) or from_number
                _customer_full_name = (
                    (customer_profile.get("name", "") + " " + customer_profile.get("last_name", "")).strip()
                    or "Customer"
                )
                _trade_alert = (
                    f"Trade-in update: {_customer_full_name} ({_alert_outbound_phone}) "
                    f"added a trade-in to their {confirmed_appt['visit_time']} appointment"
                    + (f" for the {confirmed_appt.get('car_desc')}" if confirmed_appt.get("car_desc") else "")
                    + f". Trade-in details: {candidate_trade_in}."
                )
                try:
                    notify_all_staff(dealer_row, to_number, _trade_alert)
                except Exception as e:
                    app.logger.warning("Trade-in update notify failed: %s", e)
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

    if _is_price_breakdown_question(body):
        # ADI is the only dealer with confirmed, accurate per-vehicle fee data
        # (doc fee + title/tag) wired through the scraper. Other dealers may
        # share ADI's twilio number locally for testing OR have placeholder
        # fees — either way, we don't want to surface inaccurate fee numbers
        # to a customer. For non-ADI dealers, skip the breakdown and let the
        # message fall through to the LLM for a simpler price answer.
        fees = get_dealer_fees(to_number) if _dealer_uses_inspection_clause(dealer_row=dealer_row) else {"doc_fee": 0.0, "title_tag_fee": 0.0}
        if fees["doc_fee"] > 0:
            history      = get_recent_messages(from_number, to_number, limit=14)
            exact_match  = _find_exact_year_make_match(body, inventory_rows)
            anchor_match = _extract_car_from_last_bot_message(history, inventory_rows)
            match = None
            if exact_match:
                match = exact_match
            elif _body_mentions_car(body, inventory_rows):
                history_text = " ".join((m.get("content") or "") for m in history[-6:])
                appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""
                search_ctx   = f"{history_text} {appt_car} {body}".strip()
                matches      = find_inventory_matches(inventory_rows, search_ctx, top_k=1, current_msg=body)
                if matches:
                    match = matches[0]
            elif anchor_match:
                # Last bot message was about ONE specific vehicle - safe to anchor on it.
                # _extract_car_from_last_bot_message returns None when the previous reply
                # was a listing, so we don't accidentally pick the first car of a list.
                match = anchor_match
            if match:
                breakdown = _format_price_breakdown(match, fees)
                if breakdown:
                    save_message(from_number, to_number, "assistant", breakdown)
                    return _reply_twiml(breakdown, from_number, to_number, send_primer=new_customer)
            # No vehicle in context (e.g. customer asked a generic fee/cost question
            # right after a listing). Give a dealer-level fee answer instead of
            # picking a random car from history.
            doc_str = _fmt_money(fees["doc_fee"])
            tt = fees["title_tag_fee"]
            tt_part = f" There's also a {_fmt_money(tt)} title and tag processing fee." if tt > 0 else ""
            reply_text = (
                f"On top of the Internet Price, every vehicle has a {doc_str} doc fee."
                f"{tt_part} Indiana sales tax also applies. "
                "Want me to break down the total for a specific vehicle?"
            )
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
        reply_text   = (ai_vehicle_detail_reply(body, inventory_row_details(match), dealer_phone, history, twilio_number=to_number, dealer_row=dealer_row) or _issue_response_for_match(match, to_number, dealer_row=dealer_row)) if match else build_unknown_answer(dealer_phone)
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
        reply_text = (ai_vehicle_detail_reply(body, inventory_row_details(match), dealer_phone, history, twilio_number=to_number, dealer_row=dealer_row) or _title_status_response_for_match(match)) if match else build_unknown_answer(dealer_phone)
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_carfax_question(body):
        history      = get_recent_messages(from_number, to_number, limit=14)
        history_text = " ".join((m.get("content") or "") for m in history[-6:])
        appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""
        exact_match  = _find_exact_year_make_match(body, inventory_rows)
        if exact_match:
            match = exact_match
        elif _body_mentions_car(body, inventory_rows):
            search_ctx = f"{history_text} {appt_car} {body}".strip()
            matches    = find_inventory_matches(inventory_rows, search_ctx, top_k=1, current_msg=body)
            match      = matches[0] if matches else _best_history_vehicle_match(inventory_rows, history_text)
        else:
            match = _extract_car_from_last_bot_message(history, inventory_rows) or _best_history_vehicle_match(inventory_rows, history_text)
        if match:
            carfax_url = str(match.get("CarfaxURL", "")).strip()
            title = _vehicle_title(match)
            # Once-per-conversation rule: if we've already sent THIS specific
            # CarFax URL in this conversation, don't spam the customer with
            # the same link again. Refer them back to the report instead.
            _carfax_already_sent = bool(carfax_url) and any(
                carfax_url in (m.get("content") or "")
                for m in history if m.get("role") == "assistant"
            )
            if carfax_url and _carfax_already_sent:
                reply_text = (
                    f"That's covered in the CARFAX report I sent earlier for "
                    f"the {title} — it has the accident history, prior owners, "
                    f"and service records. Want to set up a time to come see it?"
                )
            elif carfax_url:
                reply_text = (
                    f"Here's the CARFAX report for the {title} — it covers "
                    f"accident history, prior owners, and service records: {carfax_url}"
                )
            elif _dealer_uses_inspection_clause(dealer_row=dealer_row):
                reply_text = (
                    f"I don't have the CARFAX for the {title}, but every car on our "
                    f"lot is thoroughly inspected before being listed. Would you like "
                    f"me to send the VIN instead, or set up a time to come see it in person?"
                )
            else:
                reply_text = (
                    f"I don't have the CARFAX for the {title}. Would you like me to "
                    f"send the VIN so you can run it yourself, or set up a time to "
                    f"come see it in person?"
                )
        elif _dealer_uses_inspection_clause(dealer_row=dealer_row):
            reply_text = (
                "I don't have a CARFAX on file for that one, but every car on our "
                "lot is thoroughly inspected before being listed. Would you like to "
                "set up a time to come take a look?"
            )
        else:
            reply_text = (
                "I don't have a CARFAX on file for that one. Would you like to set "
                "up a time to come take a look in person?"
            )
        save_message(from_number, to_number, "assistant", reply_text)
        return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    if _is_vehicle_link_question(body):
        history      = get_recent_messages(from_number, to_number, limit=14)
        history_text = " ".join((m.get("content") or "") for m in history[-6:])
        appt_car     = confirmed_appt["car_desc"] if confirmed_appt else ""
        search_ctx   = f"{history_text} {appt_car} {body}".strip()
        exact_match  = _find_exact_year_make_match(body, inventory_rows)
        if exact_match:
            match = exact_match
        elif _body_mentions_car(body, inventory_rows):
            matches = find_inventory_matches(inventory_rows, search_ctx, top_k=1, current_msg=body)
            match   = matches[0] if matches else _best_history_vehicle_match(inventory_rows, history_text)
        else:
            match   = _extract_car_from_last_bot_message(history, inventory_rows) or _best_history_vehicle_match(inventory_rows, history_text)
        if match:
            url = str(match.get("DetailURL", "")).strip()
            if url:
                if _is_vehicle_photo_question(body):
                    reply_text = f"You can see photos of the {_vehicle_title(match)} on the listing: {url}"
                else:
                    reply_text = f"Here's the listing for the {_vehicle_title(match)}: {url}"
            else:
                reply_text = build_unknown_answer(dealer_phone)
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

    # Superlative queries ("cheapest truck", "newest SUV") look like vehicle-detail
    # questions because they mention a body type, but they're not asking about a
    # specific vehicle — they're asking the dealer to pick one. Skip this handler
    # so the message routes to the superlative listing at PRIORITY 4.62.
    _is_superlative_query = bool(_extract_superlative_query(body))
    if (_is_avail_q or _is_vehicle_detail_question(body)) and not _has_category_filter and not _is_superlative_query:
        history  = get_recent_messages(from_number, to_number, limit=14)
        appt_car = confirmed_appt["car_desc"] if confirmed_appt else ""

        if _body_mentions_car(body, inventory_rows):
            # Prefer the vehicle the bot most recently discussed when the
            # customer's reference is non-specific ("the camry", "this one")
            # and isn't asking for an alternative. Without this, two vehicles
            # of the same model in inventory cause find_inventory_matches to
            # pick the wrong unit even when the conversation has clearly
            # anchored to one (e.g. mid-booking on the 2023 Camry, customer
            # says "more info about the camry" -> bot answers about the 2017).
            last_mentioned = _extract_car_from_last_bot_message(history, inventory_rows)
            has_explicit_year = bool(re.search(r"\b(19|20)\d{2}\b", body))
            wants_alternative = bool(re.search(r"\b(other|another|different|else)\b", body.lower()))
            if last_mentioned and not has_explicit_year and not wants_alternative:
                matches = [last_mentioned]
            else:
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
                reply_text = ai_vehicle_detail_reply(body, inventory_row_details(match), dealer_phone, history, twilio_number=to_number, dealer_row=dealer_row) or inventory_row_details(match)

        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)
        # No vehicle identified or wrong make matched - fall through to AI

    # ── PRIORITY 4.615: List-completeness confirmation ──────────────────
    # The customer just got a list back and is now asking if that list is
    # complete (e.g. "is that all", "are those all the hybrids you have").
    # Without this, the next handler (make-listing, feature-listing) would
    # re-fire on keywords in the question and dump the whole list again.
    _is_completeness_q = bool(re.search(
        r"\b("
        r"is\s+that\s+(?:all|everything|it)|"
        r"are\s+(?:those|these|they)\s+(?:all|the\s+only)|"
        r"is\s+(?:it|that)\s+the\s+only|"
        r"that('?s|\s+is)\s+(?:all|everything|it)"
        r")\b",
        body, re.I,
    ))
    if _is_completeness_q:
        # Walk back through the last few assistant messages looking for a list
        # (2+ year+make pairs in a single message). A list shown 2 turns back
        # still counts — common after the customer asks "what's the other one"
        # which produces a single-vehicle reply, then asks "is that all" which
        # is still about the original list's completeness.
        _hist = get_recent_messages(from_number, to_number, limit=8)
        _recent_assistant_msgs = [m.get("content", "") for m in reversed(_hist) if m.get("role") == "assistant"][:4]
        _had_recent_list = any(
            len(re.findall(r"\b(19[5-9]\d|20[0-2]\d)\s+([A-Za-z][A-Za-z\-]+)", _msg)) >= 2
            for _msg in _recent_assistant_msgs
        )
        if _had_recent_list:
            reply_text = (
                "Yes, that's everything we have matching that right now. "
                "Want to widen the search, or would you like more details on one of those?"
            )
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.62: Superlative inventory queries ─────────────────────
    # "cheapest SUV", "newest truck", "lowest mileage Toyota", etc. Pick the
    # single best match from inventory by the appropriate sort field. Fires
    # before the body-type / make / price listings so "cheapest SUV" returns
    # ONE specific cheapest SUV, not the whole SUV list.
    _superlative = _extract_superlative_query(body)
    if _superlative:
        _sf_field, _sf_asc, _sf_label = _superlative
        reply_text = _format_superlative_listing(inventory_rows, body, _sf_field, _sf_asc, _sf_label)
        if reply_text:
            reply_text = _commercial_subtype_prefix(body) + reply_text
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.63: Vague budget intent ───────────────────────────────
    # "I'm on a budget", "anything affordable", "something cheap". No specific
    # dollar amount, so show the cheapest 5 (filtered by body type / make if
    # the customer mentioned one). Pricing queries with a specific number go
    # through PRIORITY 4.75 instead.
    if _is_budget_intent(body):
        _budget_history = get_recent_messages(from_number, to_number, limit=10)
        _budget_year_floor = _extract_relative_year_floor(body, _budget_history)
        reply_text = _format_budget_listing(inventory_rows, body, year_min=_budget_year_floor)
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

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
            reply_text = _commercial_subtype_prefix(body) + reply_text
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.7: Deterministic feature-filtered listing ────────────
    # ── PRIORITY 4.7a: New arrivals listing ─────────────────────────────
    # "What's new", "any new arrivals", "just got in" → list vehicles that
    # don't have a posted price yet (those are typically the freshly-arrived
    # units the dealer hasn't priced for the website). For dealers whose
    # entire inventory has prices, this just returns "no new arrivals."
    if _is_new_arrivals_question(body):
        reply_text = _format_new_arrivals_listing(inventory_rows)
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

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
        # Apply relative price ('a little more expensive than that') when the
        # body has both a body-type filter AND a relative qualifier. Anchors to
        # the most recent price in history.
        if _min_p is None and _max_p is None:
            _rel_min_f, _rel_max_f = _extract_relative_price_filter(
                body, get_recent_messages(from_number, to_number, limit=10)
            )
            if _rel_min_f is not None or _rel_max_f is not None:
                _min_p, _max_p = _rel_min_f, _rel_max_f
        reply_text = _format_feature_listing(
            inventory_rows,
            body_type=_body_f, fuel_type=_fuel_f, drivetrain=_drive_f,
            min_p=_min_p, max_p=_max_p, year=_year_f,
        )
        if reply_text:
            reply_text = _commercial_subtype_prefix(body) + reply_text
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.75: Deterministic price-filtered listing ──────────────
    # The LLM was dropping cars from filtered lists ("under 10k" -> 5 of 11)
    # and occasionally including over-budget rows. Filter and format in code
    # so the listing is provably complete and accurate.
    if _min_p is not None or _max_p is not None:
        reply_text = _format_price_listing(
            inventory_rows, _min_p, _max_p,
            cars_only=_wants_cars_only(body),
            exclude_makes=_extract_exclude_makes(body, inventory_rows),
        )
        if reply_text:
            save_message(from_number, to_number, "assistant", reply_text)
            return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 4.74: Relative price filter ─────────────────────────────
    # "anything cheaper than that", "something more expensive", "less
    # expensive than the Prius". Anchors to the most recently mentioned
    # price in history and routes through the price listing.
    _rel_min_p, _rel_max_p = _extract_relative_price_filter(
        body, get_recent_messages(from_number, to_number, limit=10)
    )
    if _rel_min_p is not None or _rel_max_p is not None:
        reply_text = _format_price_listing(
            inventory_rows, _rel_min_p, _rel_max_p,
            cars_only=_wants_cars_only(body),
            exclude_makes=_extract_exclude_makes(body, inventory_rows),
        )
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

    # ── PRIORITY 4.95: "do you have any X" with sub-brand/trim search ────
    # The LLM tends to miss sub-brand or trim words that live inside the
    # model string (AMG, TRD, SRT, M Sport, ZR1) and falsely answer "no".
    # Substring-search the full row text + description so these queries
    # never fall back to LLM hallucinations. Skips when the term is a known
    # year/make/model — those have their own handlers further down.
    _have_any_term = _is_have_any_question(body)
    if _have_any_term and not _body_mentions_car(body, inventory_rows):
        _stopwords = {"the", "a", "an", "of", "for", "with", "and", "or",
                      "but", "is", "are", "any", "some", "this", "that",
                      "in", "on", "at"}
        def _depluralize(t):
            return t[:-1] if len(t) >= 4 and t.endswith("s") and not t.endswith("ss") else t
        _q_tokens = [
            _depluralize(t)
            for t in re.split(r"[^a-z0-9]+", _have_any_term)
            if t and len(t) >= 2 and t not in _stopwords
        ]
        if _q_tokens:
            _q_matches = []
            for r in inventory_rows:
                hay = (_row_text_for_match(r) + " " + str(r.get("Description", "") or "")).lower()
                if all(re.search(rf"\b{re.escape(t)}s?\b", hay) for t in _q_tokens):
                    _q_matches.append(r)
            if _q_matches:
                lines = [f"Yes, here's what we have matching '{_have_any_term}':"]
                for r in _q_matches[:25]:
                    price = str(r.get("Price", "")).strip()
                    line = f"- {_vehicle_title(r)}"
                    if price:
                        line += f": ${price}"
                    lines.append(line)
                if len(_q_matches) > 25:
                    lines.append(f"...and {len(_q_matches) - 25} more.")
                lines.append("")
                lines.append("Would you like more details on any of these?")
                reply_text = "\n".join(lines)
                save_message(from_number, to_number, "assistant", reply_text)
                return _reply_twiml(reply_text, from_number, to_number, send_primer=new_customer)

    # ── PRIORITY 5: Full AI conversation ─────────────────────────────────
    history  = get_recent_messages(from_number, to_number, limit=14)

    # Pulled up early so pronoun resolution below can use it.
    last_assistant = next(
        (m.get("content", "") for m in reversed(history) if m.get("role") == "assistant"),
        "",
    )

    _is_list_q = bool(re.search(
        r"\b(list|show|what do you have|what cars|what vehicles|what.?s available|"
        r"what are your|under \$?[\d,]+|over \$?[\d,]+|less than|more than|"
        r"between \$?[\d,]|all your|everything (under|over|you have))\b",
        body, re.I
    ))

    # If the customer says "info about it / tell me more about that / what else
    # on this one" — pronoun referring to a vehicle the bot just discussed —
    # treat it as a vehicle info request even though the message itself names
    # no year/make/model. Without this, follow-ups fall through to the generic
    # "I don't have that information" fallback.
    _uses_pronoun_for_vehicle = bool(re.search(
        r"\b(it|that|this|the\s+(?:car|vehicle|truck|suv|sedan|one)|that\s+one|this\s+one)\b",
        body, re.I,
    ))
    _last_assistant_mentions_car = _body_mentions_car(last_assistant or "", inventory_rows)
    # If the bot's last reply was about ONE specific vehicle (not a listing),
    # the customer's info follow-up applies to that anchor even without pronouns
    # or explicit make/model. Catches "what other information do you have".
    _last_assistant_single_anchor = _extract_car_from_last_bot_message(history, inventory_rows) is not None

    _is_vehicle_info_q = (
        _is_general_info_question(body)
        or bool(re.search(
            r"\b(info|information|details|specs|describe|rundown|overview|"
            r"break down|learn more|what.?s the deal|tell me)\b",
            body, re.I,
        ))
    ) and (
        _body_mentions_car(body, inventory_rows)
        or (_uses_pronoun_for_vehicle and _last_assistant_mentions_car)
        or _last_assistant_single_anchor
    )

    # Detect the "no more questions" follow-up: the bot just asked "Do you have
    # any specific questions about it?" and the customer answered with a closure.
    _bot_just_asked_for_questions = "specific questions about it" in (last_assistant or "").lower()
    _is_no_more_questions = bool(re.search(
        r"^\s*(no|nope|nah|not really|i.?m good|im good|that.?s it|thats it|"
        r"all good|no more|nothing else|that.?s all|thats all|good for now|"
        r"no thanks|i.?m all set|im all set)\b",
        body.strip(), re.I,
    ))

    # If this is a vehicle-info request and we can identify a single anchor vehicle,
    # the code (not the LLM) will produce the essentials block (price/mileage/issues)
    # using the smart-skip logic. The LLM is given a tighter prompt to write ONLY a
    # short features blurb. This is reliable because the LLM was inconsistent about
    # following the conditional skip rules when asked to handle essentials itself.
    _info_anchor = None
    if _is_vehicle_info_q:
        # Resolution order matters: an EXPLICIT vehicle reference in the body
        # always wins over a sticky anchor from the prior reply. Customers say
        # "what about the nissan rogue" mid-conversation about a different car,
        # and we don't want the prior anchor (the Mini Cooper) to override that.
        _info_anchor = _find_exact_year_make_match(body, inventory_rows)
        if not _info_anchor and _body_mentions_car(body, inventory_rows):
            # When the body's car reference is non-specific ("the camry",
            # no year) and the customer isn't asking for a different unit,
            # prefer the last-mentioned anchor over a fuzzy inventory match.
            # Two units of the same model otherwise cause the fuzzy search
            # to pick whichever scores higher in isolation, ignoring the
            # active conversation context.
            _has_explicit_year = bool(re.search(r"\b(19|20)\d{2}\b", body))
            _wants_alternative = bool(re.search(r"\b(other|another|different|else)\b", body.lower()))
            if not _has_explicit_year and not _wants_alternative:
                _info_anchor = _extract_car_from_last_bot_message(history, inventory_rows)
            if not _info_anchor:
                _fuzzy = find_inventory_matches(inventory_rows, body, top_k=1, current_msg=body)
                if _fuzzy:
                    _info_anchor = _fuzzy[0]
        if not _info_anchor:
            # No explicit reference — fall back to the prior single-vehicle anchor.
            _info_anchor = _extract_car_from_last_bot_message(history, inventory_rows)

    prompt   = build_prompt(dealer_row, inventory_rows, history, body, dealer_phone, confirmed_appt, customer_profile)
    if _is_list_q:
        prompt += "\n\n=== LISTING REQUEST ===\nThe customer is asking for a list of vehicles. You MAY list multiple vehicles on separate lines. Include year, make, model, and price for each. List ALL matching vehicles, not just a few."
    if _is_vehicle_info_q and _info_anchor:
        prompt += (
            "\n\n=== VEHICLE FEATURES BLURB ONLY ===\n"
            f"The customer is asking about the {_vehicle_title(_info_anchor)}. "
            "Write ONLY 1-2 short sentences describing notable features (color, interior, engine, drivetrain, notable options). "
            "DO NOT mention price, mileage, known issues, or CARFAX — the system prepends those automatically and will duplicate any you write. "
            "DO NOT push to schedule a visit. DO NOT add greetings or closings. "
            "Start the FIRST sentence with 'It features...' or 'It comes with...' — do NOT repeat the year/make/model at the start, the system already named the vehicle. "
            "Output the features sentences ONLY, nothing else."
        )
    elif _is_vehicle_info_q:
        prompt += (
            "\n\n=== VEHICLE INFO REQUEST ===\n"
            "The customer is asking for information about a specific vehicle. Lead with price, mileage, and known-issues status, then add 1-2 sentences on key features. "
            "Do NOT push to schedule a visit. "
            "END the reply with exactly this sentence: \"Do you have any specific questions about it?\""
        )
    if _bot_just_asked_for_questions and _is_no_more_questions:
        prompt += "\n\n=== READY TO SCHEDULE ===\nThe customer just confirmed they have no more questions about the vehicle. Acknowledge briefly in one short sentence, then ask if they would like to schedule a time to come see it. Keep the whole reply to 1-2 sentences."

    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600 if _is_list_q else (450 if _is_vehicle_info_q else 300),
        )
        raw_reply = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.error("OpenAI call failed: %s", e)
        raw_reply = ""

    if not raw_reply:
        raw_reply = "Thank you for reaching out. How may I assist you with your vehicle search today?"

    reply_text, meta = extract_meta(raw_reply)

    # Strip any URLs the LLM made up. Vehicle/page links only come from the
    # deterministic link handler (_is_vehicle_link_question), which reads
    # the real detail_url from inventory. The LLM has no way to know real URLs.
    reply_text = _scrub_llm_urls(reply_text)
    # If the LLM somehow slipped a CarFax URL through (or one was injected
    # later via essentials/post-processing), dedupe vs. what we already sent.
    reply_text = _dedupe_carfax_in_reply(reply_text, history)

    # For vehicle-info requests with a clear anchor, prepend the deterministic
    # essentials block (the LLM was told to write features only) and append the
    # closing question. This avoids relying on the LLM to follow the "skip
    # already-stated essentials" rules — code does the skip logic instead.
    if _is_vehicle_info_q and _info_anchor:
        essentials = _format_vehicle_essentials(_info_anchor, last_assistant, dealer_row=dealer_row)
        features_blurb = reply_text.strip()
        closing = "Do you have any specific questions about it?"
        pieces = [p for p in [essentials, features_blurb] if p]
        # Don't double up the closing if the LLM already added it.
        if closing.lower().rstrip("?") not in features_blurb.lower():
            pieces.append(closing)
        reply_text = " ".join(pieces).strip()

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
        if meta.get("_extracted_phone") and not customer_profile.get("real_phone"):
            normalized_phone = normalize_phone(meta["_extracted_phone"])
            phone_digits = re.sub(r"\D", "", normalized_phone)
            if normalized_phone.startswith("+1") and len(phone_digits) == 11:
                # Defense against LLM-hallucinated phone numbers: the 10-digit
                # form MUST appear in something the customer actually typed
                # (current message or any prior user turn). Without this check
                # the LLM can invent a phone number that looks structurally
                # valid (10 digits, +1 prefix) and the system will save it as
                # the customer's real_phone — leading to wrong-number leads.
                local_digits = phone_digits[1:]  # strip leading "1"
                _user_typed_digits = re.sub(r"\D", "", body or "")
                for _m in get_recent_messages(from_number, to_number, limit=20):
                    if _m.get("role") == "user":
                        _user_typed_digits += " " + re.sub(r"\D", "", _m.get("content") or "")
                if local_digits and local_digits in _user_typed_digits:
                    save_kwargs["real_phone"] = normalized_phone
                else:
                    app.logger.warning(
                        "Rejected META_PHONE %s — digits not present in customer messages (likely LLM hallucination)",
                        normalized_phone,
                    )
        if save_kwargs:
            save_customer_profile(from_number, to_number, **save_kwargs)
            customer_profile = get_customer_profile(from_number, to_number)
            customer_name = customer_profile["name"]

    if meta and (meta.get("confirmed") or meta.get("need_confirmation")):
        visit_time     = str(meta.get("visit_time",     "")).strip()
        visit_time     = _augment_bare_time_with_day(visit_time, from_number, to_number)
        visit_time_iso = _validate_iso(str(meta.get("visit_time_iso", "")).strip())
        car_desc       = str(meta.get("car_desc",       "")).strip()

        if not visit_time_iso and visit_time:
            _, visit_time_iso = parse_visit_time_from_text(visit_time)

        if visit_time and not has_clock_time(visit_time):
            # AI tried to confirm with just a date (no clock time). Reject and re-ask.
            reply_text = "Of course - what specific time of day works best for your visit?"
            app.logger.info("Held auto-book: visit_time has no clock time (%r)", visit_time)
            visit_time = ""  # short-circuit the rest of this block

        # Hallucination guard: the LLM has been observed to fabricate a
        # visit_time (e.g. "today at 11 AM") when no time was ever discussed.
        # Before auto-booking, verify the time the LLM is claiming was either
        # (a) mentioned by the customer in this conversation, or (b) echoed by
        # the bot AFTER being established by a previous customer turn. We
        # check by pulling the clock-time component out of visit_time (e.g.
        # "11", "2:30") and confirming it appears in at least one CUSTOMER
        # message. If not, the LLM made it up — reject and ask for a time.
        if visit_time:
            _time_m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", visit_time, re.I)
            if _time_m:
                _hour = _time_m.group(1)
                _customer_text = " ".join(
                    (m.get("content") or "").lower()
                    for m in get_recent_messages(from_number, to_number, limit=30)
                    if m.get("role") == "user"
                )
                # Also include the current body since it's about to be saved
                _customer_text += " " + (body or "").lower()
                # Look for the hour digit anywhere in customer messages.
                # Word-boundary regex fails on "3pm" (no boundary between digit
                # and letter), so use a lookahead/lookbehind pair that only
                # excludes matches inside a longer number (e.g. hour=3 should
                # match "3pm" but NOT "23" or "30").
                _hour_in_customer = bool(re.search(
                    rf"(?<!\d){re.escape(_hour)}(?!\d)",
                    _customer_text,
                ))
                if not _hour_in_customer:
                    reply_text = "Of course - what time works best for you?"
                    app.logger.warning(
                        "Held auto-book: LLM fabricated visit_time=%r (hour %s never appeared in customer messages)",
                        visit_time, _hour,
                    )
                    visit_time = ""  # short-circuit auto-book

        if visit_time:
            missing = missing_profile_field(customer_profile)
            if missing:
                # AI tried to confirm but profile is incomplete. Hold as pending and override the reply.
                set_pending(from_number, to_number, dealer_phone, visit_time, visit_time_iso, car_desc or "a vehicle")
                # If the booking is for a specific vehicle (not a general visit) AND we still
                # need an email, fold STEP 1.5 (questions/financing/trade-in) into the same
                # message so the customer gets that one chance to flag interest before we
                # lock it in. For general visits or non-email missing fields, keep the
                # original concise prompt.
                _car_desc_lower = (car_desc or "").strip().lower()
                _is_specific_car = bool(_car_desc_lower) and _car_desc_lower not in {"general visit", "general", "visit", "a vehicle"}
                if missing == "email address" and _is_specific_car:
                    # Ask STEP 1.5 questions WITHOUT bundling the email ask. The
                    # LLM will pick up the email request on the next turn after
                    # the customer answers (and will follow up for trade-in
                    # vehicle details if needed). Bundling both into one message
                    # caused the LLM to treat any single answer as "all done"
                    # and skip collecting trade-in details.
                    reply_text = (
                        f"Got it - {visit_time} for the {car_desc}. Any other questions "
                        f"about it, are you interested in financing, or do you have a "
                        f"trade-in you'd like us to take a look at?"
                    )
                else:
                    reply_text = (f"I have your appointment for {visit_time} on hold. "
                                  f"Could I please get your {missing} so I can lock it in?")
                app.logger.info("Held auto-book for missing profile field: %s", missing)
            elif meta.get("confirmed"):
                # Auto-book immediately - no pending confirmation needed
                appt_id, is_reschedule = log_appointment(
                    from_number, to_number, dealer_phone, visit_time, visit_time_iso, car_desc or "a vehicle"
                )
                additional_info = extract_customer_insights(get_recent_messages(from_number, to_number, limit=20))
                _alert_customer_phone = resolve_outbound_customer_phone(from_number, to_number) or from_number
                alert_body = (
                    _dealer_reschedule_body(
                        customer_phone=_alert_customer_phone, customer_name=customer_name,
                        customer_last_name=customer_profile["last_name"],
                        customer_email=customer_profile["email"],
                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc or "a vehicle",
                        additional_info=additional_info,
                    )
                    if is_reschedule else
                    _dealer_alert_body(
                        customer_phone=_alert_customer_phone, customer_name=customer_name,
                        customer_last_name=customer_profile["last_name"],
                        customer_email=customer_profile["email"],
                        dealership_line=to_number, visit_time=visit_time, car_desc=car_desc or "a vehicle",
                        additional_info=additional_info,
                    )
                )
                notify_all_staff(dealer_row, to_number, alert_body)
                notify_customer_appointment(dealer_row, customer_phone=from_number,
                    twilio_number=to_number, customer_name=customer_name,
                    visit_time=visit_time, car_desc=car_desc,
                    action=("rescheduled" if is_reschedule else "confirmed"))
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
from flask import redirect, abort

# Legacy single-tenant env vars - kept as fallback so existing deployments
# keep working until the dealer fills in the new slug column in the sheet.
WIDGET_DEALER_TWILIO_NUM = os.getenv("WIDGET_DEALER_TWILIO_NUM", "")
WIDGET_DEALER_NAME       = os.getenv("WIDGET_DEALER_NAME", "Auto District Indy")
# If set, visiting "/" (no slug) redirects to /widget/<this slug> so old
# bookmarks / Render URLs without a slug still land somewhere sensible.
WIDGET_DEFAULT_SLUG      = os.getenv("WIDGET_DEFAULT_SLUG", "")

app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")


def _session_to_phone(session_id: str) -> str:
    """Map a browser session id to a stable phone-like identifier so the
    existing routing logic (which keys everything on customer_phone) works
    without modification. We keep up to 32 cleaned chars so two browser
    sessions whose ids share a long common prefix (e.g. produced by the
    same Math.random pattern) don't collide on the customer_phone key."""
    cleaned = re.sub(r"[^a-zA-Z0-9]", "", session_id)[:32]
    return f"+web{cleaned}"


def _resolve_widget_dealer(slug: str) -> Dict[str, str]:
    """Look up a dealer by slug and return their widget branding fields.
    Returns {} if no match - caller decides how to handle that (404 vs
    fallback to env-var defaults)."""
    try:
        dealers = read_dealers()
    except Exception as e:
        app.logger.error("widget dealer lookup: sheet read failed: %s", e)
        return {}
    dealer = select_dealer_for_slug(dealers, slug)
    if not dealer:
        return {}
    return get_widget_branding(dealer)


def _compute_widget_category_flags(twilio_number: str) -> Dict[str, bool]:
    """Return per-category presence flags for the sidebar so empty buckets
    can hide their button. One inventory fetch + one in-memory pass per
    page load. All flags default to False; the caller can use Jinja's
    falsy-undefined behavior on top of these."""
    flags = {
        "has_any_inventory": False,
        "has_suvs": False,
        "has_trucks": False,
        "has_sedans": False,
        "has_vans": False,
        "has_coupes": False,
        "has_4wd": False,
        "has_commercial": False,
        "has_motorcycles": False,
        "has_hybrids": False,
        "has_new_arrivals": False,
    }
    if not twilio_number:
        return flags
    try:
        inv_rows = get_inventory_for_twilio(twilio_number)
    except Exception as e:
        app.logger.warning("category flag inventory lookup failed for %s: %s",
                           twilio_number, e)
        return flags
    flags["has_any_inventory"] = bool(inv_rows)
    _CATEGORY_KEYS = (
        "has_suvs", "has_trucks", "has_sedans", "has_vans", "has_coupes",
        "has_4wd", "has_commercial", "has_motorcycles",
        "has_hybrids", "has_new_arrivals",
    )
    for r in inv_rows:
        if not flags["has_suvs"] and _row_matches_body_type(r, "suv"):
            flags["has_suvs"] = True
        if not flags["has_trucks"] and _row_matches_body_type(r, "truck"):
            flags["has_trucks"] = True
        if not flags["has_sedans"] and _row_matches_body_type(r, "sedan"):
            flags["has_sedans"] = True
        if not flags["has_vans"] and _row_matches_body_type(r, "van"):
            flags["has_vans"] = True
        if not flags["has_coupes"] and _row_matches_body_type(r, "coupe"):
            flags["has_coupes"] = True
        if not flags["has_4wd"] and (
            _row_matches_drivetrain(r, "4wd") or _row_matches_drivetrain(r, "awd")
        ):
            flags["has_4wd"] = True
        if not flags["has_commercial"] and _is_commercial_row(r):
            flags["has_commercial"] = True
        if not flags["has_motorcycles"] and _is_motorcycle(r):
            flags["has_motorcycles"] = True
        if not flags["has_hybrids"] and _row_matches_fuel_type(r, "hybrid"):
            flags["has_hybrids"] = True
        # "New arrivals" = unpriced rows (just-scraped, dealer hasn't priced yet)
        if not flags["has_new_arrivals"] and not str(r.get("Price", "")).strip().lstrip("0"):
            flags["has_new_arrivals"] = True
        if all(flags[k] for k in _CATEGORY_KEYS):
            break
    return flags


@app.route("/")
def widget_root():
    # If a default slug is configured, send users there. Otherwise fall back
    # to the legacy single-tenant rendering path using env-var name + Twilio
    # number, so existing setups don't break before the sheet is updated.
    if WIDGET_DEFAULT_SLUG:
        return redirect(f"/widget/{WIDGET_DEFAULT_SLUG}", code=302)
    if WIDGET_DEALER_TWILIO_NUM:
        flags = _compute_widget_category_flags(WIDGET_DEALER_TWILIO_NUM)
        return render_template(
            "index.html",
            dealer_name=WIDGET_DEALER_NAME,
            brand_color="#4a90e2",
            logo_url="",
            slug="",
            terms_url=PRIMER_TERMS_URL,
            privacy_url=PRIMER_PRIVACY_URL,
            **flags,
        )
    return (
        "<h1>No dealer specified</h1>"
        "<p>Visit <code>/widget/&lt;dealer-slug&gt;</code> to load a dealer's widget.</p>",
        404,
    )


@app.route("/widget/<slug>")
def widget_for_dealer(slug):
    branding = _resolve_widget_dealer(slug)
    if not branding:
        return (
            f"<h1>Dealer not found</h1>"
            f"<p>No dealer with slug <code>{slug}</code> in the sheet. "
            f"Check the slug column for that dealer.</p>",
            404,
        )
    # Per-category sidebar flags. Computed in one pass so empty buckets hide
    # their button (e.g. a dealer with zero SUVs sees no SUVs button).
    flags = _compute_widget_category_flags(branding["twilio_number"])
    return render_template(
        "index.html",
        dealer_name=branding["name"],
        brand_color=branding["brand_color"],
        logo_url=branding["logo_url"],
        slug=branding["slug"],
        terms_url=PRIMER_TERMS_URL,
        privacy_url=PRIMER_PRIVACY_URL,
        **flags,
    )


@app.route("/chat", methods=["POST"])
def chat_webhook():
    data = request.get_json(silent=True) or {}
    user_message = (data.get("message") or "").strip()
    session_id   = (data.get("session_id") or "").strip() or session.get("sid")
    slug         = (data.get("slug") or "").strip()
    # Self-healing real_phone: the widget echoes the customer's phone (from
    # localStorage) on every chat, so if the DB was wiped (e.g. DEV_CLEAR_DB)
    # we re-attach it without re-prompting the customer.
    cached_phone = (data.get("real_phone") or "").strip()

    # Resolve which dealer's Twilio number this chat targets. Slug from the
    # widget page wins; fall back to the legacy env var so old single-tenant
    # deployments keep working.
    to_number = ""
    if slug:
        branding = _resolve_widget_dealer(slug)
        if branding and branding["twilio_number"]:
            to_number = branding["twilio_number"]
    if not to_number:
        to_number = WIDGET_DEALER_TWILIO_NUM

    if not to_number:
        return jsonify({"error": "no dealer resolved (missing slug and no fallback configured)"}), 400

    if not session_id:
        session_id = _uuid.uuid4().hex
        session["sid"] = session_id

    if not user_message:
        return jsonify({"error": "empty message"}), 400

    from_number = _session_to_phone(session_id)

    # Self-heal real_phone if the JS sent it but it's not on file (e.g. DB
    # was wiped between visits but the widget kept the phone in localStorage).
    if cached_phone:
        normalized_cached = normalize_phone(cached_phone)
        if normalized_cached.startswith("+1") and len(re.sub(r"\D", "", normalized_cached)) == 11:
            existing = get_customer_profile(from_number, to_number).get("real_phone", "")
            if not existing:
                save_customer_profile(from_number, to_number, real_phone=normalized_cached)

    # Profile gate: before processing the customer's actual question, require
    # first name + 10-digit phone. We don't run the LLM or save the message;
    # the JS shows a profile form, the customer fills it in, then re-sends the
    # original message which now passes this gate.
    profile = get_customer_profile(from_number, to_number)
    needs_name  = not (profile.get("name") or "").strip()
    needs_phone = not (profile.get("real_phone") or "").strip()
    if needs_name or needs_phone:
        gate_reply = (
            "Sure, I can help with that. Before I do, could I get your first name "
            "and 10-digit phone number? We use these so we can text you "
            "appointment confirmations and follow up if you have any questions."
        )
        return jsonify({
            "reply": gate_reply,
            "needs_profile": True,
            "missing": {"name": needs_name, "phone": needs_phone},
            "pending_message": user_message,
            "session_id": session_id,
        })

    g.captured_reply  = None
    g.captured_primer = None
    g.captured_silent = False

    try:
        _process_message(from_number, to_number, user_message)
    except Exception as e:
        app.logger.error("chat _process_message failed: %s", e)
        return jsonify({"error": "processing error"}), 500

    # STEP 1.5 enforcement. The LLM doesn't reliably ask the
    # questions/financing/trade-in question before requesting the email - it
    # often skips straight to "could I get your email?". When that happens, we
    # rewrite the reply server-side so STEP 1.5 always fires before STEP 2.
    try:
        _maybe_inject_step_1_5(from_number, to_number, dealer_phone=normalize_phone(
            get_row_field(_resolve_widget_dealer(slug) or {}, DEALER_NOTIFY_PHONE_ALIASES) if slug else ""
        ))
    except Exception as e:
        app.logger.warning("step 1.5 injection check failed: %s", e)

    # Day-reference enforcement. LLMs strip the day when echoing a clock time
    # back ("Got it - 3 PM" instead of "Got it - Saturday at 3 PM"), which is
    # ambiguous for the customer. Rewrite to include the day from history.
    try:
        _maybe_inject_day_in_time(from_number, to_number)
    except Exception as e:
        app.logger.warning("day-in-time injection check failed: %s", e)

    # Customer-name correction. LLMs sometimes pull a word from the customer's
    # most recent message as a name ("can i pay cash" -> "Thanks, Can!") even
    # when the customer's real first name is in the prompt context. Rewrite
    # any greeter-name pair so it uses the profile's first name.
    try:
        _maybe_fix_customer_name_in_reply(from_number, to_number)
    except Exception as e:
        app.logger.warning("customer-name correction failed: %s", e)

    silent = bool(g.get("captured_silent"))
    if silent:
        reply = ""
    else:
        reply = g.get("captured_reply") or "Sorry, I had trouble processing that. Could you try again?"
    primer = g.get("captured_primer")

    return jsonify({
        "reply": reply,
        "primer": primer,
        "silent": silent,
        "session_id": session_id,
    })


@app.route("/widget/welcome", methods=["POST"])
def widget_welcome():
    """Generate the proactive welcome message shown right after a customer
    accepts terms and enters the chat. Replaces the SMS-style FYI primer for
    widget users by both introducing the bot's capabilities and asking for the
    customer's phone + name upfront. Saves the welcome to conversation history
    and marks the primer as sent so it won't be appended again later."""
    data = request.get_json(silent=True) or {}
    session_id = (data.get("session_id") or "").strip()
    slug       = (data.get("slug") or "").strip()
    if not session_id or not slug:
        return jsonify({"error": "missing session_id or slug"}), 400

    branding = _resolve_widget_dealer(slug)
    if not branding or not branding.get("twilio_number"):
        return jsonify({"error": "dealer not found"}), 404

    twilio_number = branding["twilio_number"]
    customer_key  = _session_to_phone(session_id)
    dealer_name   = branding.get("name") or "us"

    # If the welcome (or any primer) was already sent to this session, skip.
    if has_primer_been_sent(customer_key, twilio_number):
        return jsonify({"welcome": ""})

    welcome = (
        f"Hi, my name is Dave with {dealer_name}. I can help with inventory, "
        "vehicles, financing, and scheduling a visit — or type MENU for more options.\n\n"
        f"By communicating with our assistant, you agree to our [terms]({PRIMER_TERMS_URL}). "
        "If you receive and respond to SMS follow-ups from the dealership, msg frequency varies and msg & data rates may apply. "
        "Reply HELP for help, STOP to opt out anytime."
    )
    welcome_followup = "How can I help you?"

    save_message(customer_key, twilio_number, "assistant", welcome)
    save_message(customer_key, twilio_number, "assistant", welcome_followup)
    mark_primer_sent(customer_key, twilio_number)

    return jsonify({"welcome": welcome, "welcome_followup": welcome_followup})


@app.route("/widget/history", methods=["POST"])
def widget_history():
    """Return saved conversation history for a widget session so the chat
    re-hydrates after a page refresh instead of showing an empty window."""
    data = request.get_json(silent=True) or {}
    session_id = (data.get("session_id") or "").strip()
    slug       = (data.get("slug") or "").strip()
    if not session_id or not slug:
        return jsonify({"messages": []})

    branding = _resolve_widget_dealer(slug)
    if not branding or not branding.get("twilio_number"):
        return jsonify({"messages": []})

    twilio_number = branding["twilio_number"]
    customer_key  = _session_to_phone(session_id)
    msgs = get_recent_messages(customer_key, twilio_number, limit=MAX_MESSAGES_PER_CHAT)
    return jsonify({"messages": msgs})


@app.route("/widget/profile", methods=["POST"])
def widget_profile():
    """Save first name + 10-digit phone collected via the in-chat profile form
    that pops up after the customer's first message. Required before the LLM
    will respond to any question."""
    data = request.get_json(silent=True) or {}
    session_id = (data.get("session_id") or "").strip()
    slug       = (data.get("slug") or "").strip()
    name       = (data.get("name") or "").strip()
    phone_raw  = (data.get("phone") or "").strip()

    if not session_id or not slug:
        return jsonify({"error": "missing session_id or slug"}), 400
    if not name:
        return jsonify({"error": "missing name"}), 400

    digits = re.sub(r"\D", "", phone_raw)
    if len(digits) != 10:
        return jsonify({"error": "phone must be 10 digits"}), 400
    normalized_phone = "+1" + digits

    branding = _resolve_widget_dealer(slug)
    if not branding or not branding.get("twilio_number"):
        return jsonify({"error": "dealer not found"}), 404

    twilio_number = branding["twilio_number"]
    customer_key  = _session_to_phone(session_id)

    save_customer_profile(customer_key, twilio_number,
                          name=name, real_phone=normalized_phone)
    return jsonify({"ok": True})


@app.route("/widget/register-phone", methods=["POST"])
def widget_register_phone():
    """Save the customer's real phone number against their widget session.
    The widget collects this on open (required) so we can text appointment
    confirmations, reminders, and follow-ups to a real number instead of the
    +web<sessionid> pseudo-phone we use as a DB key."""
    data = request.get_json(silent=True) or {}
    session_id = (data.get("session_id") or "").strip()
    slug       = (data.get("slug") or "").strip()
    raw_phone  = (data.get("phone") or "").strip()
    if not session_id or not slug or not raw_phone:
        return jsonify({"error": "missing session_id, slug, or phone"}), 400

    normalized = normalize_phone(raw_phone)
    digits = re.sub(r"\D", "", normalized)
    # Require a 10-digit US number (E.164 with leading +1).
    if not (normalized.startswith("+1") and len(digits) == 11):
        return jsonify({"error": "invalid phone number"}), 400

    branding = _resolve_widget_dealer(slug)
    if not branding or not branding.get("twilio_number"):
        return jsonify({"error": "dealer not found"}), 404

    customer_key = _session_to_phone(session_id)  # +web<sessionid>
    save_customer_profile(customer_key, branding["twilio_number"],
                          real_phone=normalized)
    return jsonify({"ok": True, "phone": normalized})


@app.route("/widget/clear-session", methods=["POST"])
def widget_clear_session():
    """Wipe all per-customer state tied to a widget session (conversation
    history, profile, pending bookings, cold follow-up scheduling, primer
    flag). Triggered by the "Clear Chat" button so a customer who resets
    isn't still chased by lead follow-ups based on the old conversation.

    terms_acceptance_log is intentionally NOT cleared — it's a legal record
    of the customer's consent and stays put regardless of chat resets."""
    data = request.get_json(silent=True) or {}
    session_id = (data.get("session_id") or "").strip()
    slug       = (data.get("slug") or "").strip()
    if not session_id or not slug:
        return jsonify({"error": "missing session_id or slug"}), 400

    branding = _resolve_widget_dealer(slug)
    if not branding or not branding.get("twilio_number"):
        return jsonify({"error": "dealer not found"}), 404

    customer_key  = _session_to_phone(session_id)
    twilio_number = branding["twilio_number"]

    # Look up the customer's real phone BEFORE deleting anything so we can
    # find every sibling session this human ever had with this dealer (each
    # browser tab / Clear Chat / device creates a fresh +web<sessionid>).
    # Wipe must span ALL of them — otherwise the cold-followup scheduler
    # would later pick up a sibling session that still has messages and
    # text the customer about a conversation they think they deleted.
    real_phone_for_reset = (get_customer_profile(customer_key, twilio_number)
                            .get("real_phone", "") or "").strip()
    # If the current session's row hasn't recorded a real_phone yet (fresh
    # session after a refresh), accept the cached real_phone the JS sends
    # from localStorage so we can still find sibling sessions to wipe.
    if not real_phone_for_reset:
        cached_phone = (data.get("real_phone") or "").strip()
        if cached_phone:
            normalized_cached = normalize_phone(cached_phone)
            if normalized_cached.startswith("+1") and len(re.sub(r"\D", "", normalized_cached)) == 11:
                real_phone_for_reset = normalized_cached

    # Collect every customer_phone we need to wipe: the current session
    # plus all siblings tied to the same real phone.
    sibling_keys: set = {customer_key}
    if real_phone_for_reset:
        try:
            conn = _db()
            sibling_rows = conn.execute(
                "SELECT customer_phone FROM customer_names "
                "WHERE real_phone=? AND twilio_number=?",
                (real_phone_for_reset, twilio_number),
            ).fetchall()
            conn.close()
            for r in sibling_rows:
                sibling_keys.add(r["customer_phone"])
        except Exception as e:
            app.logger.warning("widget_clear_session sibling lookup failed: %s", e)

    tables = (
        "messages",
        "customer_names",
        "appointments",
        "pending_appointments",
        "pending_reconfirmations",
        "pending_cancellations",
        "cold_followups",
        "primer_sent",
    )
    try:
        conn = _db()
        with conn:
            for ck in sibling_keys:
                for t in tables:
                    conn.execute(
                        f"DELETE FROM {t} WHERE customer_phone=? AND twilio_number=?",
                        (ck, twilio_number),
                    )
        conn.close()
    except Exception as e:
        app.logger.warning("widget_clear_session DB delete failed: %s", e)
        return jsonify({"error": "clear failed"}), 500

    app.logger.info(
        "Widget session cleared for %s on %s (wiped %d session(s))",
        customer_key, twilio_number, len(sibling_keys),
    )
    return jsonify({"ok": True})


@app.route("/health")
def health():
    return jsonify({"ok": True, "default_dealer": WIDGET_DEALER_NAME})


# Embed bubble loader served at top-level path so dealers paste a clean URL.
# Re-exposes static/embed.js without the /static/ prefix.
@app.route("/embed.js")
def embed_js():
    from flask import send_from_directory, make_response
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    resp = make_response(send_from_directory(static_dir, "embed.js"))
    resp.headers["Content-Type"] = "application/javascript; charset=utf-8"
    # Browsers can cache for an hour; bump the version in the snippet if you
    # ever need to bust the cache mid-day.
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


# Lightweight branding endpoint the embed loader hits to color the bubble.
# Returned with CORS wide-open so embed.js can fetch it from any dealer's
# website (cross-origin call from their domain to ours).
@app.route("/widget-config")
def widget_config():
    slug = (request.args.get("dealer") or "").strip()
    if not slug:
        resp = jsonify({"error": "missing dealer slug"})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp, 400
    branding = _resolve_widget_dealer(slug)
    if not branding:
        resp = jsonify({"error": "dealer not found"})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp, 404
    resp = jsonify({
        "dealer_name": branding["name"],
        "brand_color": branding["brand_color"],
        "logo_url":    branding["logo_url"],
        "slug":        branding["slug"],
    })
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


# Simple admin index that lists every dealer's copy-paste embed snippet.
# Password-gated by the ADMIN_PASSWORD env var (default "5643"). Visit:
#     https://<your-render-url>/admin
# and enter the password. Stays unlocked for the browser session.
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "5643")


def _admin_authed() -> bool:
    return bool(session.get("admin_ok"))


@app.route("/admin", methods=["GET", "POST"])
def admin_index():
    # Login form submission
    if request.method == "POST":
        if (request.form.get("password") or "").strip() == ADMIN_PASSWORD:
            session["admin_ok"] = True
        else:
            return _admin_login_page(error="Wrong password.")
        # Fall through to render the dashboard

    if not _admin_authed():
        return _admin_login_page()

    # Build dealer rows from the sheet
    try:
        dealers = read_dealers()
    except Exception as e:
        return f"<h1>Admin</h1><p>Sheet read failed: {e}</p>", 500

    # Prefer PUBLIC_BASE_URL env var (set in Render to e.g.
    # https://dealer-chat-widget.onrender.com) so the snippets always show the
    # production URL even when /admin is accessed locally. Falls back to the
    # current request's host if the env var isn't set.
    base_url = (os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
                or request.url_root.rstrip("/"))
    rows_html = []
    for d in dealers:
        name        = get_row_field(d, DEALER_NAME_ALIASES) or "(unnamed dealer)"
        twilio_num  = normalize_phone(get_row_field(d, TWILIO_NUMBER_ALIASES))
        explicit    = _normalize_slug(get_row_field(d, SLUG_ALIASES))
        slug        = explicit or _normalize_slug(name)
        if not slug:
            continue  # nothing to embed for unnamed/unkeyed dealers
        snippet = (
            f'<script src="{base_url}/embed.js?dealer={slug}"></script>'
        )
        widget_url = f"{base_url}/widget/{slug}"
        slug_label = "" if explicit else " <em>(derived from name)</em>"
        rows_html.append(f"""
        <article class="dealer">
          <h2>{name}</h2>
          <p class="meta">slug: <code>{slug}</code>{slug_label} &middot; twilio: <code>{twilio_num or '(none)'}</code></p>
          <p><strong>Widget preview:</strong> <a href="{widget_url}" target="_blank">{widget_url}</a></p>
          <p><strong>Embed snippet</strong> (paste this on the dealer's site, before <code>&lt;/body&gt;</code>):</p>
          <div class="snippet-row">
            <textarea readonly rows="2" onclick="this.select()">{snippet}</textarea>
            <button type="button" onclick="navigator.clipboard.writeText(this.previousElementSibling.value).then(()=>{{this.textContent='Copied!';setTimeout(()=>this.textContent='Copy',1500);}})">Copy</button>
          </div>
        </article>
        """)

    if not rows_html:
        rows_html.append("<p>No dealers found in the sheet yet.</p>")

    body = "\n".join(rows_html)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Admin · Embed codes</title>
<style>
  body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; max-width: 880px; margin: 24px auto; padding: 0 16px; color: #222; }}
  h1 {{ margin-bottom: 4px; }}
  .topbar {{ display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #e5e7eb; padding-bottom:12px; margin-bottom:20px; }}
  .topbar a {{ color:#6b7280; text-decoration:none; font-size:14px; }}
  .dealer {{ border:1px solid #e5e7eb; border-radius:10px; padding:16px 18px; margin-bottom:16px; background:#fafafa; }}
  .dealer h2 {{ margin: 0 0 4px 0; font-size: 18px; }}
  .meta {{ color:#666; font-size:13px; margin:0 0 10px 0; }}
  code {{ background:#eef0f3; padding:1px 5px; border-radius:4px; font-size:13px; }}
  textarea {{ flex:1; font-family: ui-monospace, Menlo, Consolas, monospace; font-size:12.5px; padding:10px; border:1px solid #ccc; border-radius:6px; resize:vertical; min-height:48px; }}
  .snippet-row {{ display:flex; gap:8px; align-items:flex-start; }}
  .snippet-row button {{ background:#1f2937; color:#fff; border:none; padding:9px 14px; border-radius:6px; cursor:pointer; font-size:13px; }}
  .snippet-row button:hover {{ background:#111827; }}
  a {{ color:#1f2937; }}
</style></head>
<body>
  <div class="topbar">
    <h1>Dealer embed codes</h1>
    <a href="/admin/logout">Log out</a>
  </div>
  <p style="color:#555">Each dealer's row below has a copy-paste snippet. Hand it to the dealer (or their web person) — they paste it once on their site and the chat bubble appears on every page.</p>
  {body}
</body></html>"""


def _admin_login_page(error: str = "") -> str:
    err_html = f'<p style="color:#b91c1c;font-size:14px;">{error}</p>' if error else ""
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Admin login</title>
<style>
  body {{ font-family: -apple-system,BlinkMacSystemFont,sans-serif; display:flex; align-items:center; justify-content:center; min-height:100vh; margin:0; background:#f3f4f6; }}
  form {{ background:#fff; padding:28px 32px; border-radius:10px; box-shadow:0 4px 20px rgba(0,0,0,0.08); width:300px; }}
  h1 {{ font-size:20px; margin:0 0 16px 0; }}
  input[type=password] {{ width:100%; padding:10px 12px; border:1px solid #d1d5db; border-radius:6px; font-size:15px; box-sizing:border-box; margin-bottom:12px; }}
  button {{ width:100%; padding:10px; background:#1f2937; color:#fff; border:none; border-radius:6px; font-size:15px; cursor:pointer; }}
  button:hover {{ background:#111827; }}
</style></head>
<body>
  <form method="POST" action="/admin">
    <h1>Admin login</h1>
    {err_html}
    <input type="password" name="password" placeholder="Password" autofocus required>
    <button type="submit">Enter</button>
  </form>
</body></html>"""


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_ok", None)
    return redirect("/admin", code=302)


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
            "SELECT year, make, model, mileage, price FROM inventory WHERE twilio_number=? LIMIT 10",
            (WIDGET_DEALER_TWILIO_NUM,),
        ).fetchall()
        with_mileage = conn.execute(
            "SELECT COUNT(*) FROM inventory WHERE twilio_number=? AND mileage <> '' AND mileage IS NOT NULL",
            (WIDGET_DEALER_TWILIO_NUM,),
        ).fetchone()[0]
        conn.close()
        return jsonify({
            "widget_dealer_twilio_num": WIDGET_DEALER_TWILIO_NUM,
            "rows_for_widget_dealer": widget_count,
            "rows_with_mileage": with_mileage,
            "all_dealer_groups": [{"twilio_number": tn, "count": n} for tn, n in all_groups],
            "sample_rows_for_widget_dealer": [
                {"year": y, "make": mk, "model": md, "mileage": mi, "price": pr}
                for y, mk, md, mi, pr in sample
            ],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =========================
# AI PHONE CALL HANDLING
# Voice version of the chat/SMS flow. Same dealer routing, same dealer profile,
# same inventory awareness, but TwiML <Gather> instead of text. The LLM emits
# [TAKE_MESSAGE] / [TRANSFER] / [HANGUP] tokens to control call flow; on
# handoff we LLM-summarize the call and SMS+email the staff so they pick up
# (or call back) with full context.
# =========================
VOICE_MAX_CALL_TURNS    = 16
VOICE_WRAPUP_MESSAGE    = ("Thanks for the time — I'll have someone from our sales team follow up. "
                           "If you need immediate help, please call back during business hours.")
VOICE_PUBLIC_BASE_URL   = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")


def _voice_action_url(path: str) -> str:
    """Absolute URL for TwiML action attrs. PUBLIC_BASE_URL when set
    (production), request host as fallback (local ngrok)."""
    if VOICE_PUBLIC_BASE_URL:
        return f"{VOICE_PUBLIC_BASE_URL}{path}"
    try:
        return f"{request.url_root.rstrip('/')}{path}"
    except Exception:
        return path


def _voice_session_record(call_sid: str, twilio_number: str, customer_phone: str) -> None:
    conn = _db()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO voice_sessions "
            "(call_sid, twilio_number, customer_phone, turns, created_at) "
            "VALUES (?,?,?,?,?)",
            (call_sid, twilio_number, customer_phone, 0, _utc_now_iso()),
        )
    conn.close()


def _voice_session_bump(call_sid: str) -> int:
    conn = _db()
    with conn:
        conn.execute("UPDATE voice_sessions SET turns=turns+1 WHERE call_sid=?", (call_sid,))
    row = conn.execute("SELECT turns FROM voice_sessions WHERE call_sid=?", (call_sid,)).fetchone()
    conn.close()
    return row["turns"] if row else 0


_VOICE_WRAPUP_RE = re.compile(
    r"\b("
    r"thank(s| you)?|thanks a lot|thanks so much|"
    r"ok(ay)?|alright|all right|sounds good|sounds great|"
    r"good bye|goodbye|bye|talk to you later|see ya|"
    r"that('?s| is)? (all|it|everything|fine)|"
    r"i'?m (good|all set|done)|appreciate it|perfect|got it"
    r")\b",
    re.I,
)


def _looks_like_voice_wrapup(speech: str) -> bool:
    return bool(_VOICE_WRAPUP_RE.search(speech or ""))


def _build_voice_gather(say_text: str, action_path: str) -> VoiceResponse:
    """Speak say_text, then listen. If the caller stays silent through the
    Gather's timeout, instead of hanging up we re-prompt and Gather again —
    the second Gather still POSTs to the same handle URL with empty
    SpeechResult, which the handler re-prompts on. Only after a long
    repeated silence does Twilio's outer fall-through (final Say + Hangup)
    fire, and only as a last resort."""
    vr = VoiceResponse()
    if say_text:
        vr.say(say_text, voice="Polly.Joanna-Neural")
    gather = Gather(
        input="speech",
        action=_voice_action_url(action_path),
        method="POST",
        speech_timeout="auto",
        timeout=8,
        language="en-US",
    )
    vr.append(gather)
    # First fall-through: gentle nudge, then re-listen. This keeps the call
    # alive when the caller is thinking or in a noisy environment.
    vr.say("Sorry, I didn't catch that. Take your time — what can I help with?",
           voice="Polly.Joanna-Neural")
    retry = Gather(
        input="speech",
        action=_voice_action_url(action_path),
        method="POST",
        speech_timeout="auto",
        timeout=10,
        language="en-US",
    )
    vr.append(retry)
    # Last resort after two timeouts: wrap up politely.
    vr.say("It seems I can't hear you. I'll let you go — please call back when you're ready.",
           voice="Polly.Joanna-Neural")
    vr.hangup()
    return vr


_VOICE_RULES_APPEND = (
    "\n\n=== VOICE-CALL OVERRIDES (replace the SMS/chat rules above when they conflict) ===\n"
    "This conversation is happening over a phone call. The customer can't see anything you write.\n"
    "- Reply in 1-2 short, spoken sentences. Sound like a real receptionist, not a phone tree.\n"
    "- No markdown, no bullet points, no URLs read aloud (say 'on our website' instead).\n"
    "- When you say prices, vehicles, or stock numbers, say each digit individually if accuracy matters (e.g. 'stock D-zero-zero-one'), not 'd one').\n"
    "\n"
    "DO NOT ASK FOR THE CALLER'S NAME OR PHONE NUMBER UNLESS THEY ARE TRYING TO BOOK A TEST DRIVE OR DEALERSHIP VISIT. Most calls are people shopping — answer their questions about inventory, financing, hours, trade-ins, etc. WITHOUT collecting personal info. They already called you, so you don't need their number to follow up. Only collect name + phone when:\n"
    "  • They say they want to come in / book a test drive / schedule a visit\n"
    "  • They explicitly ask you to have someone call them back\n"
    "  • They ask for a personalized financing or trade-in quote that needs a salesperson to follow up\n"
    "\n"
    "ACT LIKE A REAL SALES RECEPTIONIST. Don't deflect — try to handle the call:\n"
    "- Inventory questions: pull from the TOP MATCHING VEHICLE DETAILS in the prompt above. If the customer's interest matches a specific vehicle, describe it in 1-2 sentences and offer details (price, miles, features). Only ask if they want to schedule a visit after they show interest.\n"
    "- Financing, trade-ins, hours, location, policies: answer directly from the dealership profile.\n"
    "- Pricing on specific cars: quote the listed price. If they ask about out-the-door / financed payment, say 'a salesperson can run real numbers for you' and ask if they want a callback.\n"
    "\n"
    "IF YOU CAN'T ANSWER A QUESTION, DON'T END THE CALL. Say 'That's a great question for a salesperson — would you like me to have one call you back, or is there anything else I can help with right now?' Keep the conversation going. Never silently emit [HANGUP] just because you don't know an answer.\n"
    "\n"
    "READ BACK AND CONFIRM KEY DETAILS only when collecting them (name, phone, vehicle they want to test drive, appointment time). Speech-to-text mangles digits:\n"
    "  Caller: 'My number is 317-999-7907.'\n"
    "  You: 'Got it — 3-1-7, 9-9-9, 7-9-0-7. Did I get that right?'\n"
    "Before you hand off a booking, do one final summary readback ('Just to confirm — Evan Lee, 3-1-7-9-9-9-7-9-0-7, the 2022 BMW X7, Saturday at 2 PM. Sound right?') and only after they confirm, emit the handoff token.\n"
    "\n"
    "HOW TO END THE CALL — MUST emit one of these tokens at the end of your reply once the call should wrap up. Token on its own line at the very end. Without one the call keeps looping.\n"
    "\n"
    "  [TAKE_MESSAGE] — use this for handoffs where the team needs to follow up:\n"
    "    • You collected booking details for a test drive / visit (name + phone + vehicle/time)\n"
    "    • Caller asked for a callback or a question only a salesperson can answer\n"
    "    • Caller said goodbye AFTER you had any kind of substantive lead-style conversation\n"
    "    Your spoken line must reassure: 'Got it, Evan — I've sent your details to our sales team and someone will call you back at 3-1-7-9-9-9-7-9-0-7 shortly.' DO NOT say 'call us' — they're on the phone with us already.\n"
    "\n"
    "  [TRANSFER] — ONLY when the caller explicitly demands a live person, is clearly upset, or asks for a manager by name.\n"
    "\n"
    "  [HANGUP] — only when the caller said goodbye after a casual info-only conversation (no booking, no callback request) AND there's nothing for the team to follow up on. NEVER emit [HANGUP] mid-call because you couldn't answer — keep talking instead.\n"
    "\n"
    "DEFAULT: don't go more than 5-6 turns for a routine inquiry. If they want to book, do the summary readback, get a yes, then emit [TAKE_MESSAGE]. If they're just window-shopping and say goodbye, emit [HANGUP].\n"
)


def build_dealer_voice_prompt(dealer, inventory_rows, history, customer_msg,
                              dealer_phone, customer_name="") -> List[Dict[str, str]]:
    """Reuse the existing chat prompt builder (so we keep all the inventory
    matching, dealer policies, and scheduling smarts) and append the voice
    overrides on top. build_prompt returns the system prompt as a STRING, not
    a message list — we wrap it into chat-completion messages ourselves."""
    system_prompt = build_prompt(
        dealer,
        inventory_rows,
        history,
        customer_msg,
        dealer_phone,
        None,           # confirmed_appt
        customer_name,
    )
    if not isinstance(system_prompt, str):
        # Defensive: if build_prompt ever changes shape, coerce.
        system_prompt = str(system_prompt or "")
    system_prompt = system_prompt + _VOICE_RULES_APPEND

    msgs: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    for m in (history or [])[-MAX_MESSAGES_PER_CHAT:]:
        role = m.get("role") if isinstance(m, dict) else None
        content = m.get("content") if isinstance(m, dict) else None
        if role and content:
            msgs.append({"role": role, "content": content})
    msgs.append({"role": "user", "content": customer_msg})
    return msgs


def _summarize_voice_call_for_dealer(dealer, history, customer_name, caller_phone) -> str:
    """Condense the call into 5-7 lines the sales staff can read at a glance.
    Includes caller phone so they can call back without hunting."""
    dealer_name = get_row_field(dealer, DEALER_NAME_ALIASES) or "the dealership"
    convo = "\n".join(f"{m['role']}: {m['content']}" for m in history[-16:])
    info_lines = []
    if caller_phone:
        info_lines.append(f"caller phone: {caller_phone}")
    if isinstance(customer_name, dict):
        for k in ("name", "last_name", "email", "real_phone"):
            v = customer_name.get(k, "")
            if v:
                info_lines.append(f"{k}: {v}")
    elif customer_name:
        info_lines.append(f"name: {customer_name}")
    info_block = "\n".join(info_lines) or "(none yet)"
    sys = (
        f"You're summarizing a phone call for {dealer_name}'s sales staff. The AI receptionist "
        "is handing off — either taking a message or transferring. Output 5-7 short lines that "
        "let the staff pick up cold: caller name, phone number, vehicle(s) they're interested "
        "in (with stock number if known), what they want (test drive, financing, trade-in info, "
        "etc.), urgency, anything already promised or quoted. No greetings, no preamble — just "
        "the facts, one per line."
    )
    user = f"CALL TRANSCRIPT:\n{convo}\n\nKNOWN CALLER INFO:\n{info_block}"
    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": sys},
                      {"role": "user",   "content": user}],
            temperature=0.2,
            max_tokens=250,
        )
        out = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.warning("voice call summary failed: %s", e)
        out = ""
    return out or "New call (see voice history)."


@app.route("/voice", methods=["POST"])
def voice_webhook():
    """Inbound call entrypoint. Twilio POSTs From / To / CallSid here when
    a customer calls the dealership's Twilio number."""
    call_sid    = request.form.get("CallSid") or ""
    from_number = normalize_phone(request.form.get("From") or "")
    to_number   = normalize_phone(request.form.get("To") or "")
    if not call_sid or not to_number:
        vr = VoiceResponse()
        vr.say("Sorry, this number isn't configured. Goodbye.", voice="Polly.Joanna-Neural")
        vr.hangup()
        return str(vr)

    try:
        dealers = read_dealers()
    except Exception as e:
        app.logger.error("voice: read_dealers failed: %s", e)
        dealers = []
    dealer_row = select_dealer_for_twilio_number(dealers, to_number)
    if not dealer_row:
        vr = VoiceResponse()
        vr.say("Sorry, this number isn't set up yet. Goodbye.", voice="Polly.Joanna-Neural")
        vr.hangup()
        return str(vr)

    dealer_name = get_row_field(dealer_row, DEALER_NAME_ALIASES) or "the dealership"
    greeting = f"Thanks for calling {dealer_name}. How can I help you today?"

    _voice_session_record(call_sid, to_number, from_number)
    save_message(from_number, to_number, "assistant", greeting)
    app.logger.info("voice/webhook call=%s from=%s to=%s dealer=%s",
                    call_sid, from_number, to_number, dealer_name)
    return str(_build_voice_gather(greeting, f"/voice/handle?call_sid={call_sid}"))


@app.route("/voice/handle", methods=["POST"])
def voice_handle():
    call_sid    = (request.args.get("call_sid") or request.form.get("CallSid") or "").strip()
    speech      = (request.form.get("SpeechResult") or "").strip()
    confidence  = request.form.get("Confidence") or "0"
    from_number = normalize_phone(request.form.get("From") or "")
    to_number   = normalize_phone(request.form.get("To") or "")
    app.logger.info("voice/handle call=%s speech=%r conf=%s", call_sid, speech, confidence)

    if not call_sid or not to_number:
        vr = VoiceResponse()
        vr.say("Sorry, I lost the connection. Goodbye.", voice="Polly.Joanna-Neural")
        vr.hangup()
        return str(vr)

    try:
        dealers = read_dealers()
    except Exception:
        dealers = []
    dealer_row = select_dealer_for_twilio_number(dealers, to_number)
    dealer_phone = normalize_phone(get_row_field(dealer_row, DEALER_NOTIFY_PHONE_ALIASES))
    try:
        inventory_rows = get_inventory_for_twilio(to_number)
    except Exception as e:
        app.logger.warning("voice: inventory fetch failed: %s", e)
        inventory_rows = []

    turns = _voice_session_bump(call_sid)
    if turns >= VOICE_MAX_CALL_TURNS:
        vr = VoiceResponse()
        vr.say(VOICE_WRAPUP_MESSAGE, voice="Polly.Joanna-Neural")
        vr.hangup()
        return str(vr)

    if not speech:
        return str(_build_voice_gather("Sorry, I missed that. Could you say it again?",
                                       f"/voice/handle?call_sid={call_sid}"))

    save_message(from_number, to_number, "user", speech)
    history = get_recent_messages(from_number, to_number, limit=MAX_MESSAGES_PER_CHAT)
    customer_profile = get_customer_profile(from_number, to_number) or {}

    msgs = build_dealer_voice_prompt(
        dealer=dealer_row,
        inventory_rows=inventory_rows,
        history=history[:-1],
        customer_msg=speech,
        dealer_phone=dealer_phone,
        customer_name=customer_profile,
    )
    # Use a faster model for voice — Twilio's webhook timeout is 15s, and
    # gpt-4o sometimes runs right up to that on cold-cache turns. gpt-4o-mini
    # is sub-second for these short voice replies and quality is fine for
    # 1-2 sentence answers.
    voice_model = os.getenv("OPENAI_VOICE_MODEL", "gpt-4o-mini")
    try:
        resp = openai_client.chat.completions.create(
            model=voice_model,
            messages=msgs,
            temperature=0.4,
            max_tokens=180,
            timeout=10,  # hard ceiling so Twilio doesn't time out before we reply
        )
        raw_reply = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        app.logger.error("voice LLM failed: %s", e)
        raw_reply = ""
    if not raw_reply:
        raw_reply = "Sorry, I had trouble with that. Let me try again — what were you asking about?"
    app.logger.info("voice/handle LLM raw=%r", raw_reply)

    # Safety net: LLM sometimes forgets the token even after the caller clearly
    # wraps up. If they say bye/thanks 3+ turns in and no token was emitted,
    # force [TAKE_MESSAGE] so the lead doesn't evaporate.
    if turns >= 3 and not any(tok in raw_reply for tok in ("[TAKE_MESSAGE]", "[TRANSFER]", "[HANGUP]")):
        if _looks_like_voice_wrapup(speech):
            app.logger.info("voice/handle: wrap-up cue detected, forcing [TAKE_MESSAGE]")
            raw_reply = raw_reply.rstrip() + "\n[TAKE_MESSAGE]"

    transfer     = "[TRANSFER]" in raw_reply
    take_message = "[TAKE_MESSAGE]" in raw_reply
    hangup       = "[HANGUP]" in raw_reply
    say_text = (raw_reply
                .replace("[TRANSFER]", "")
                .replace("[TAKE_MESSAGE]", "")
                .replace("[HANGUP]", "")
                .strip())
    save_message(from_number, to_number, "assistant", say_text)

    if take_message or transfer:
        try:
            full_history = get_recent_messages(from_number, to_number, limit=MAX_MESSAGES_PER_CHAT)
            summary = _summarize_voice_call_for_dealer(
                dealer_row, full_history, customer_profile, from_number,
            )
            tag = "Incoming call - transferring NOW" if transfer else "Call summary - please follow up"
            body = f"[{get_row_field(dealer_row, DEALER_NAME_ALIASES) or 'Dealership'} AI · {tag}]\n\n{summary}"
            notify_all_staff(dealer_row, to_number, body)
        except Exception as e:
            app.logger.warning("voice handoff notify failed: %s", e)

    if transfer:
        transfer_num = dealer_phone or normalize_phone(get_row_field(dealer_row, DEALER_NOTIFY_PHONE_ALIASES))
        if transfer_num:
            vr = VoiceResponse()
            vr.say(say_text or "Let me connect you now.", voice="Polly.Joanna-Neural")
            vr.dial(transfer_num)
            return str(vr)
        vr = VoiceResponse()
        vr.say(say_text or "I've sent your details to our team and they'll call you back shortly. Goodbye.",
               voice="Polly.Joanna-Neural")
        vr.hangup()
        return str(vr)

    if take_message:
        vr = VoiceResponse()
        vr.say(say_text or "Got it — I've sent your details to our team and someone will reach out to you shortly. Thanks for calling.",
               voice="Polly.Joanna-Neural")
        vr.hangup()
        return str(vr)

    if hangup:
        vr = VoiceResponse()
        vr.say(say_text or "Thanks for calling. Goodbye.", voice="Polly.Joanna-Neural")
        vr.hangup()
        return str(vr)

    return str(_build_voice_gather(say_text, f"/voice/handle?call_sid={call_sid}"))


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

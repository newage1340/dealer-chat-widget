"""
Convert the legacy demo_profiles/*.json (Google-Sheet-row shaped) into data/tenants/*.json.

Usage: python scripts/import_demo_profiles.py [--overwrite]
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "demo_profiles"
DEST = ROOT / "data" / "tenants"


def _row(row: dict, *needles: str) -> str:
    """
    First row value whose header contains every needle (headers in the sheet are long questions).
    """
    for key, value in row.items():
        k = key.lower()
        if all(n in k for n in needles):
            return str(value or "").strip()
    return ""


def _split(value: str) -> list[str]:
    return [v.strip() for v in re.split(r"[,;\n]+", value or "") if v.strip()]


def _money(amount: float) -> str:
    return f"${amount:,.0f}" if amount == int(amount) else f"${amount:,.2f}"


def convert(slug: str, profile: dict) -> dict:
    row = profile["row"]
    doc_fee = float(profile.get("doc_fee") or 0)
    tt_fee = float(profile.get("title_tag_fee") or 0)
    fee_phrase = ""
    if doc_fee > 0:
        fee_phrase = f"a {_money(doc_fee)} doc fee" + (f" and {_money(tt_fee)} for title and tag" if tt_fee > 0 else "")

    links = []
    if (url := _row(row, "pre-approval")):
        links.append({"label": "pre-approval / credit application", "url": url, "trigger": r"pre-?approv|credit app|application|apply"})
    if (url := _row(row, "loan calculator")):
        links.append({"label": "payment calculator", "url": url, "trigger": r"calculator|monthly|weekly|payment"})

    return {
        "id":        slug,
        "slug":      slug,
        "name":      _row(row, "dealership name"),
        "domain":    "automotive",
        "timezone":  "America/Indiana/Indianapolis",
        "phone":     _row(row, "dealer phone"),
        "address":   _row(row, "dealer address"),
        "website":   _row(row, "website"),
        "hours":     _row(row, "dealer hours"),
        "endpoints": {"sms": [], "voice": []},
        "staff":     {"phones": _split(_row(row, "salesman phone")), "emails": _split(_row(row, "dealer email")) + _split(_row(row, "salesman emails"))},
        "branding":  {"color": _row(row, "brand color") or "#4a90e2", "logo_url": _row(row, "logo url")},
        "profile": {
            "financing": _row(row, "financing"),
            "trade_ins": _row(row, "trade-ins"),
            "policies":  _row(row, "policies"),
        },
        "settings": {
            "fee_phrase":            fee_phrase,
            "inspection_clause":     False,
            "cold_followup_minutes": int(_row(row, "cold followup") or 30),
        },
        "links":   links,
        "_source": profile.get("source", ""),
        "catalog": profile.get("inventory", []),
    }


def main() -> None:
    overwrite = "--overwrite" in sys.argv
    DEST.mkdir(parents=True, exist_ok=True)
    for path in sorted(SRC.glob("*.json")):
        out = DEST / path.name
        if out.exists() and not overwrite:
            print(f"skip {out} (exists; pass --overwrite)")
            continue
        tenant = convert(path.stem, json.loads(path.read_text(encoding="utf-8")))
        out.write_text(json.dumps(tenant, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"wrote {out} ({len(tenant['catalog'])} catalog items)")


if __name__ == "__main__":
    main()

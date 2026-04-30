# scraper.py — inventory scraper for twilio-bot2
import re
import json
import time
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v)).strip()


def _parse_price(raw: str) -> str:
    return re.sub(r"[^\d]", "", raw) or ""


def _parse_mileage(raw: str) -> str:
    return re.sub(r"[^\d]", "", raw) or ""


_MAKE_CAPS = {
    "bmw": "BMW", "gmc": "GMC", "ram": "RAM", "vw": "VW",
    "kia": "Kia", "jeep": "Jeep",
}

def _fix_make(make: str) -> str:
    return _MAKE_CAPS.get(make.lower(), make.title())


def _parse_vehicle_title(title: str) -> Dict[str, str]:
    parts = title.strip().split()
    year  = parts[0] if parts and re.fullmatch(r"(19|20)\d{2}", parts[0]) else ""
    rest  = parts[1:] if year else parts
    make  = _fix_make(rest[0]) if rest else ""
    model = " ".join(rest[1:3]).title() if len(rest) > 1 else ""
    trim  = " ".join(rest[3:]).title() if len(rest) > 3 else ""
    return {"year": year, "make": make, "model": model, "trim": trim}


def _deduplicate(vehicles: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for v in vehicles:
        vin   = v["VIN"].strip()
        stock = v["Stock"].strip()
        # Prefer VIN as unique key, fall back to stock#, then year+make+model
        if vin:
            key = ("vin", vin)
        elif stock:
            key = ("stock", stock)
        else:
            key = ("ymm", v["Year"], v["Make"].lower(), v["Model"].lower())
        if key not in seen:
            seen.add(key)
            out.append(v)
    return out


def _normalize_url(url: str) -> str:
    """Strip query params and fragments so the same page isn't visited twice."""
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}"


def _extract_spec(pattern: str, text: str) -> str:
    m = re.search(pattern, text, re.I)
    return _clean(m.group(1)) if m else ""


# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

def _detect_platform(html: str, url: str) -> str:
    """Return a platform identifier string based on page content/URL."""
    if re.search(r"dealercarsearch\.com|imagescdn\.dealercarsearch", html, re.I):
        return "dealercarsearch"
    # Default: original DealerSocket-style platform
    return "dealersocket"


# ---------------------------------------------------------------------------
# Playwright page loader
# ---------------------------------------------------------------------------

def _load_page_playwright(browser, url: str, attempts: int = 3) -> str:
    from playwright.sync_api import TimeoutError as PWTimeout
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        page = browser.new_page()
        try:
            page.set_extra_http_headers({"User-Agent": UA})
            # Bump the goto timeout on retries — most failures are slow first-byte.
            goto_timeout_ms = 30000 if attempt == 1 else 60000
            page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except PWTimeout:
                pass
            return page.content()
        except Exception as e:
            last_err = e
            if attempt < attempts:
                logger.info("Playwright attempt %d/%d failed for %s: %s — retrying",
                            attempt, attempts, url, e)
                time.sleep(2)
        finally:
            page.close()
    logger.warning("Playwright failed for %s after %d attempts: %s", url, attempts, last_err)
    return ""


# ===========================================================================
# PLATFORM: DealerSocket (original / default)
# ===========================================================================

def _ds_collect_detail_links(html: str, base_url: str) -> List[str]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("mailto:") or href.startswith("javascript"):
            continue
        full = urljoin(base_url, href)
        if urlparse(full).netloc != urlparse(base_url).netloc:
            continue
        if not re.search(r"/vehicle.detail", full, re.I):
            continue
        if "mailto" in full or "subject=" in full:
            continue
        norm = _normalize_url(full)
        if norm not in seen:
            seen.add(norm)
            links.append(norm)

    return links


def _ds_scrape_detail_page(html: str, detail_url: str = "") -> Optional[Dict[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None

    soup = BeautifulSoup(html, "html.parser")

    specs_el   = soup.find(class_=re.compile(r"element-type-vehiclespecifics", re.I))
    specs_text = _clean(specs_el.get_text(" ")) if specs_el else ""

    title_el   = soup.find(class_=re.compile(r"vehicle-label|element-type-inventorylisttitle", re.I))
    title_text = _clean(title_el.get_text()) if title_el else ""
    if not title_text and specs_text:
        title_text = specs_text.split("Exterior Color")[0].split("Interior Color")[0].strip()

    parsed = _parse_vehicle_title(title_text) if title_text else {}

    price_el   = soup.find(class_=re.compile(r"element-type-price", re.I))
    price_text = _clean(price_el.get_text(" ")) if price_el else ""
    price_m    = re.search(r"Internet\s*Price[:\s]*\$?([\d,]+)", price_text, re.I)
    if not price_m:
        price_m = re.search(r"\$\s*([\d,]+)", price_text)
    price = _parse_price(price_m.group(1)) if price_m else ""

    ext_color    = _extract_spec(r"Exterior\s*Color[:\s]+([^:]+?)(?=Interior|Stock|Mileage|Engine|Fuel|Trans|Title|VIN|$)", specs_text)
    int_color    = _extract_spec(r"Interior\s*Color[:\s]+([^:]+?)(?=Exterior|Stock|Mileage|Engine|Fuel|Trans|Title|VIN|$)", specs_text)
    stock        = _extract_spec(r"Stock\s*(?:Number|#|No)[:\s]+([A-Z0-9\-]+)", specs_text)
    mileage      = _parse_mileage(_extract_spec(r"Mileage[:\s]+([\d,]+)", specs_text))
    engine       = _extract_spec(r"Engine[:\s]+([^:]+?)(?=Fuel|Trans|Title|VIN|Stock|Mileage|$)", specs_text)
    fuel         = _extract_spec(r"Fuel[:\s]+([^:]+?)(?=Engine|Trans|Title|VIN|Stock|Mileage|$)", specs_text)
    transmission = _extract_spec(r"Transmission[:\s]+([^:]+?)(?=Engine|Fuel|Title|VIN|Stock|Mileage|$)", specs_text)
    title_status = _extract_spec(r"Title[:\s]+([^:]+?)(?=VIN|Stock|Mileage|Engine|Fuel|Trans|AutoCheck|$)", specs_text)
    vin          = _extract_spec(r"VIN[:\s]+([A-HJ-NPR-Z0-9]{17})", specs_text)
    if not vin:
        vin_m = re.search(r"_Vin\s*=\s*['\"]([A-HJ-NPR-Z0-9]{17})['\"]", html)
        vin = vin_m.group(1) if vin_m else ""

    if not stock:
        stock_m = re.search(r"Stock\s*#[:\s]*([A-Z0-9\-]+)", html, re.I)
        stock = stock_m.group(1) if stock_m else ""

    desc_el     = soup.find(class_=re.compile(r"vehicle-description|element-type-description", re.I))
    description = _clean(desc_el.get_text(" "))[:800] if desc_el else ""

    feature_summaries = []
    for fc in soup.find_all(class_="feature-container"):
        # Get text with newline separators so each list item / spec sits on
        # its own line, then re-join with " ;; " — a sentinel the formatter
        # splits on to render one item per line.
        raw = fc.get_text("\n")
        items = []
        for ln in raw.split("\n"):
            ln = re.sub(r"[ \t]+", " ", ln).strip()
            if ln:
                items.append(ln)
        if not items:
            continue
        text = items[0] + ((" ;; " + " ;; ".join(items[1:])) if len(items) > 1 else "")
        if len(text) < 20 or len(text) > 5000:
            continue
        feature_summaries.append(text)
    features_text = " | ".join(feature_summaries)

    spec_parts = []
    if engine:        spec_parts.append(f"Engine: {engine}")
    if transmission:  spec_parts.append(f"Transmission: {transmission}")
    if fuel:          spec_parts.append(f"Fuel: {fuel}")
    if int_color:     spec_parts.append(f"Interior: {int_color}")
    if title_status:  spec_parts.append(f"Title: {title_status}")
    spec_summary = " | ".join(spec_parts)

    full_description = " || ".join(filter(None, [description, spec_summary, features_text]))[:8000]

    if not parsed.get("year") and not parsed.get("make"):
        return None

    return {
        "Year":        parsed.get("year", ""),
        "Make":        parsed.get("make", ""),
        "Model":       parsed.get("model", ""),
        "Trim":        parsed.get("trim", ""),
        "Color":       ext_color or "",
        "Price":       price,
        "Mileage":     mileage,
        "VIN":         vin.upper(),
        "Stock":       stock,
        "Description": full_description,
        "DetailURL":   detail_url,
    }


def _ds_list_page_parse(html: str) -> List[Dict[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []

    for label in soup.find_all(True, class_=re.compile(r"vehicle-label", re.I)):
        title_text = _clean(label.get_text())
        if not re.match(r"(19|20)\d{2}\s+\w", title_text):
            continue

        container = label
        for _ in range(5):
            p = container.parent
            if not p:
                break
            ct = _clean(p.get_text())
            if re.search(r"\$[\d,]{3}", ct) or re.search(r"\d{4,}\s*mi", ct, re.I):
                container = p
                break
            container = p

        detail_text = _clean(container.get_text(" "))
        price_m  = re.search(r"\$\s*([\d,]+)", detail_text)
        mile_m   = re.search(r"([\d,]+)\s*(?:mi(?:les?)?)\b", detail_text, re.I)
        vin_m    = re.search(r"\bVIN[:\s#]*([A-HJ-NPR-Z0-9]{17})\b", detail_text, re.I)
        color_m  = re.search(r"\b(black|white|silver|gray|grey|red|blue|green|brown|gold|orange|yellow|purple|beige|tan|maroon|navy|pearl|charcoal|champagne|burgundy|bronze|copper)\b", detail_text, re.I)
        stock_m  = re.search(r"\bstock[:\s#]*([A-Z0-9\-]+)\b", detail_text, re.I)

        parsed = _parse_vehicle_title(title_text)
        if parsed.get("year") and parsed.get("make"):
            results.append({
                "Year":    parsed["year"],
                "Make":    parsed["make"],
                "Model":   parsed["model"],
                "Trim":    parsed["trim"],
                "Color":   color_m.group(1).title() if color_m else "",
                "Price":   _parse_price(price_m.group(1)) if price_m else "",
                "Mileage": _parse_mileage(mile_m.group(1)) if mile_m else "",
                "VIN":     vin_m.group(1).upper() if vin_m else "",
                "Stock":   stock_m.group(1) if stock_m else "",
                "Description": "",
            })

    return results


# ===========================================================================
# PLATFORM: DealerCarSearch (e.g. govautosales.net)
# Detail URLs: /vdp/{id}/BuyHerePayHere-{year}-{make}-{model}-...
# Data format: plain "Label: Value" text in the page, no special CSS classes
# No pagination — all vehicles on one page
# ===========================================================================

def _dcs_collect_detail_links(html: str, base_url: str) -> List[str]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(html, "html.parser")
    seen_ids = set()
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("mailto:") or href.startswith("javascript"):
            continue
        full = urljoin(base_url, href)
        if urlparse(full).netloc != urlparse(base_url).netloc:
            continue
        # DealerCarSearch detail pages: /vdp/{numeric_id}/...
        id_m = re.search(r"/vdp/(\d+)", full, re.I)
        if not id_m:
            continue
        vehicle_id = id_m.group(1)
        if vehicle_id not in seen_ids:
            seen_ids.add(vehicle_id)
            links.append(_normalize_url(full))

    return links


def _dcs_scrape_detail_page(html: str, detail_url: str = "") -> Optional[Dict[str, str]]:
    """
    DealerCarSearch detail pages (govautosales.net style).
    Specs are in <p class="i08r_opt*"> tags inside .i08r_mainInfoWrap.
    Options are in #collapseOptions > ul > li.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None

    soup = BeautifulSoup(html, "html.parser")

    def _field(cls: str) -> str:
        """Extract text after the <label> inside a i08r_opt* element."""
        el = soup.find(class_=cls)
        if not el:
            return ""
        label = el.find("label")
        if label:
            label.decompose()
        return _clean(el.get_text())

    # Title from h1.i08r_vehicleTitle
    title_el   = soup.find(class_="i08r_vehicleTitle")
    title_text = _clean(title_el.get_text()) if title_el else ""
    # fallback: first h1/h2 with a year
    if not title_text:
        for tag in soup.find_all(["h1", "h2"]):
            t = _clean(tag.get_text())
            if re.match(r"(19|20)\d{2}\s+\w", t):
                title_text = t
                break

    parsed = _parse_vehicle_title(title_text) if title_text else {}

    # Price
    price_el = soup.find(class_=re.compile(r"i08r_.*price|retail.?price", re.I))
    price_text = _clean(price_el.get_text()) if price_el else ""
    if not price_text:
        price_m = re.search(r"Retail\s*Price\s*\$?([\d,]+)", html, re.I)
        price_text = price_m.group(1) if price_m else ""
    price = _parse_price(price_text)

    # Specs — pulled directly from CSS classes, no regex on full page text
    stock        = _field("i08r_optStock")
    engine       = _field("i08r_optEngine")
    transmission = _field("i08r_optTrans")
    drive        = _field("i08r_optDrive")
    mileage      = _parse_mileage(_field("i08r_optMPG"))
    color        = _field("i08r_optColor")
    interior     = _field("i08r_optInteriorColor") or _field("i08r_optInterior")
    vin          = _field("i08r_optVin").upper()

    # VIN fallback from raw HTML
    if not vin or len(vin) != 17:
        vin_m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", html)
        vin = vin_m.group(1).upper() if vin_m else ""

    # Stock fallback
    if not stock:
        stock_m = re.search(r"Stock\s*#?\s*:?\s*([A-Z0-9\-]+)", html, re.I)
        stock = stock_m.group(1) if stock_m else ""

    # Vehicle options from #collapseOptions > ul > li
    options_el = soup.find(id="collapseOptions")
    options = []
    if options_el:
        options = [_clean(li.get_text()) for li in options_el.find_all("li") if _clean(li.get_text())]

    # CarFax link — find any anchor pointing to carfax.com
    carfax_url = ""
    for a in soup.find_all("a", href=True):
        if "carfax.com" in a["href"]:
            carfax_url = a["href"].strip()
            break

    spec_parts = []
    if engine:        spec_parts.append(f"Engine: {engine}")
    if transmission:  spec_parts.append(f"Transmission: {transmission}")
    if drive:         spec_parts.append(f"Drive: {drive}")
    if interior:      spec_parts.append(f"Interior: {interior}")

    parts = []
    if spec_parts: parts.append(" | ".join(spec_parts))
    if options:    parts.append("Options: " + ", ".join(options))
    full_description = " || ".join(parts)[:1500]

    if not parsed.get("year") and not parsed.get("make"):
        return None

    return {
        "Year":        parsed.get("year", ""),
        "Make":        parsed.get("make", ""),
        "Model":       parsed.get("model", ""),
        "Trim":        parsed.get("trim", ""),
        "Color":       color,
        "Price":       price,
        "Mileage":     mileage,
        "VIN":         vin,
        "Stock":       stock,
        "Description": full_description,
        "CarfaxURL":   carfax_url,
        "DetailURL":   detail_url,
    }


# ===========================================================================
# Public entry point
# ===========================================================================

def scrape_dealer_inventory(url: str, max_pages: int = 10, max_vehicles: int = 0) -> List[Dict[str, str]]:
    """
    Scrape full vehicle inventory from a dealer website.
    Detects the platform and routes to the correct scraping logic.
    """
    if not url:
        return []
    if not url.startswith("http"):
        url = "https://" + url

    logger.info("Scraping inventory from: %s", url)

    try:
        from playwright.sync_api import sync_playwright
        playwright_available = True
    except ImportError:
        playwright_available = False
        logger.warning("Playwright not available — using requests fallback")

    if playwright_available:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                # Load first page and detect platform
                first_html = _load_page_playwright(browser, url)
                if not first_html:
                    return []

                platform = _detect_platform(first_html, url)
                logger.info("Platform detected: %s", platform)

                # Route to platform-specific collectors
                if platform == "dealercarsearch":
                    collect_fn = _dcs_collect_detail_links
                    detail_fn  = _dcs_scrape_detail_page
                    paginate   = False  # single page, no pagination
                else:
                    collect_fn = _ds_collect_detail_links
                    detail_fn  = _ds_scrape_detail_page
                    paginate   = True

                # ── Phase 1: collect detail links ───────────────────────
                all_detail_links: List[str] = []

                pages = range(1, max_pages + 1) if paginate else range(1, 2)
                for page_num in pages:
                    page_url = url if page_num == 1 else f"{url}?page={page_num}"
                    html = first_html if page_num == 1 else _load_page_playwright(browser, page_url)
                    if not html:
                        break
                    links = collect_fn(html, url)
                    if not links:
                        logger.info("No detail links on page %d — stopping.", page_num)
                        break
                    before = len(all_detail_links)
                    for lnk in links:
                        if lnk not in all_detail_links:
                            all_detail_links.append(lnk)
                    added = len(all_detail_links) - before
                    logger.info("List page %d: %d links (+%d new). Total: %d",
                                page_num, len(links), added, len(all_detail_links))
                    if added == 0:
                        break

                if not all_detail_links:
                    logger.warning("No detail links found — falling back to list page parse")
                    result = _ds_list_page_parse(first_html)
                    return _deduplicate(result)

                # ── Phase 2: scrape each detail page ────────────────────
                if max_vehicles > 0:
                    all_detail_links = all_detail_links[:max_vehicles]
                    logger.info("DEV: limiting to first %d detail pages", max_vehicles)

                vehicles: List[Dict[str, str]] = []
                for i, detail_url in enumerate(all_detail_links):
                    html = _load_page_playwright(browser, detail_url)
                    if not html:
                        continue
                    vehicle = detail_fn(html, detail_url)
                    if vehicle:
                        vehicles.append(vehicle)
                        logger.info("Scraped %d/%d: %s %s %s",
                                    i + 1, len(all_detail_links),
                                    vehicle["Year"], vehicle["Make"], vehicle["Model"])
                    else:
                        logger.warning("Could not parse detail page: %s", detail_url)

            finally:
                browser.close()

    else:
        import requests
        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": UA})
            html = resp.text
        except Exception as e:
            logger.error("requests fetch failed: %s", e)
            return []
        platform = _detect_platform(html, url)
        if platform == "dealercarsearch":
            return _deduplicate(_dcs_collect_detail_links(html, url) and [] or _ds_list_page_parse(html))
        return _deduplicate(_ds_list_page_parse(html))

    result = _deduplicate(vehicles)
    logger.info("Total vehicles scraped: %d", len(result))
    return result

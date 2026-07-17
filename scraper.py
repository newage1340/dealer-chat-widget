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


def _parse_fee(raw: str) -> str:
    """Parse a fee string like '$239' or '$37.50' to a normalized 'NNN.NN' or 'NNN'."""
    if not raw:
        return ""
    m = re.search(r"(\d{1,5}(?:\.\d{1,2})?)", raw.replace(",", ""))
    return m.group(1) if m else ""


def _extract_doc_fee(price_text: str, html: str) -> str:
    """Find the dealer's doc fee in the price block first, then anywhere on the page."""
    for src in (price_text or "", html or ""):
        m = re.search(r"doc(?:ument(?:ary)?)?\s*fee[^$\d]{0,40}\$?\s*([\d,]+(?:\.\d{1,2})?)", src, re.I)
        if m:
            return _parse_fee(m.group(1))
    return ""


def _extract_title_tag_fee(html: str) -> str:
    """Find a title-and-tag processing fee mentioned anywhere on the page (usually fine print)."""
    if not html:
        return ""
    patterns = [
        r"title\s*(?:and|&|/|,)?\s*tag(?:\s*processing)?\s*(?:fee)?[^$\d]{0,40}\$?\s*([\d,]+(?:\.\d{1,2})?)",
        r"\$?\s*([\d,]+(?:\.\d{1,2})?)[^.\n]{0,40}title\s*(?:and|&|/|,)?\s*tag",
    ]
    for pat in patterns:
        m = re.search(pat, html, re.I)
        if m:
            return _parse_fee(m.group(1))
    return ""


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


def _build_page_url(base_url: str, page_num: int) -> str:
    """Append ?page=N or &page=N to a URL, preserving any existing query
    string. e.g. 'https://x.com/inv?clearall=1' + page 2 →
    'https://x.com/inv?clearall=1&page=2'."""
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
    parts = urlparse(base_url)
    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    # Replace any existing page=... or strip and re-add
    query_pairs = [(k, v) for (k, v) in query_pairs if k.lower() != "page"]
    query_pairs.append(("page", str(page_num)))
    new_query = urlencode(query_pairs)
    return urlunparse(parts._replace(query=new_query))


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
    # DealerCenter (used by e.g. unitedautomotive501.com). Identified by the
    # dws-* class prefix that appears on every DealerCenter-built dealer site.
    if re.search(r"dealercenter|imagescf\.dealercenter|dws-vehicle-detail|dws-forms-", html, re.I):
        return "dealercenter"
    # WooCommerce-based custom inventory (e.g. autogalaxyservice.com).
    # Identified by WooCommerce product class markers plus an /inventory/ URL.
    if re.search(r"woocommerce|product-type-simple|dhvc-woocommerce", html, re.I) and "/inventory/" in url:
        return "woocommerce_inv"
    # Default: original DealerSocket-style platform
    return "dealersocket"


# ---------------------------------------------------------------------------
# Playwright page loader
# ---------------------------------------------------------------------------

def _load_page_playwright(browser, url: str, attempts: int = 3,
                          wait_selector: str = "") -> str:
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
            # Best-effort wait for a specific late-rendering element (e.g. the
            # third-party CarFax badge, which loads several seconds after
            # networkidle fires — networkidle can trip during an earlier lull,
            # so without this the anchor is missed and carfax_url stays empty).
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=5000)
                except Exception:
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


def _load_page_playwright_with_scroll(browser, url: str, max_scrolls: int = 20,
                                       attempts: int = 3) -> str:
    """Load a page and scroll to the bottom repeatedly so lazy-loaded content
    (e.g. inventory cards loaded on scroll) gets rendered before we read the
    HTML. Returns final HTML after content stops growing or max_scrolls hits."""
    from playwright.sync_api import TimeoutError as PWTimeout
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        page = browser.new_page()
        try:
            page.set_extra_http_headers({"User-Agent": UA})
            goto_timeout_ms = 30000 if attempt == 1 else 60000
            page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except PWTimeout:
                pass
            # Scroll until page height stops growing (no more lazy content) or
            # we hit the safety cap.
            prev_height = 0
            stable_passes = 0
            for i in range(max_scrolls):
                cur_height = page.evaluate("document.body.scrollHeight")
                if cur_height == prev_height:
                    stable_passes += 1
                    if stable_passes >= 2:
                        break
                else:
                    stable_passes = 0
                prev_height = cur_height
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                try:
                    page.wait_for_load_state("networkidle", timeout=4000)
                except PWTimeout:
                    pass
                page.wait_for_timeout(500)  # small grace period for late renders
            return page.content()
        except Exception as e:
            last_err = e
            if attempt < attempts:
                logger.info("Playwright scroll attempt %d/%d failed for %s: %s — retrying",
                            attempt, attempts, url, e)
                time.sleep(2)
        finally:
            page.close()
    logger.warning("Playwright (scroll) failed for %s after %d attempts: %s", url, attempts, last_err)
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

    doc_fee = _extract_doc_fee(price_text, html)
    title_tag_fee = _extract_title_tag_fee(html)

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

    # CarFax link — the DealerSocket detail page exposes a per-VIN CarFax link
    # (e.g. carfax.com/cfm/check_order.cfm?partner=...&VIN=...) as a plain <a> in
    # the SERVER-rendered HTML. Catch: the site's JavaScript swaps that anchor for
    # a CarFax widget in the live DOM, so the headless browser's page.content()
    # loses the plain carfax.com link entirely — which is why cloud scrapes came
    # back 0/90 while a plain fetch finds it on every car. So: try the browser
    # HTML first, and if it's missing, RE-FETCH the page with a plain GET (proven
    # to contain the anchor) and pull it from there.
    def _scan_carfax(text: str) -> str:
        cands = [c.replace("&amp;", "&").strip()
                 for c in re.findall(r"https?://[^\s\"'<>\\]*carfax\.com[^\s\"'<>\\]*", text or "", re.I)]
        if not cands:
            return ""
        return next((c for c in cands if any(k in c.lower()
                     for k in ("check_order", "vehiclehistory", "vin="))), cands[0])

    def _build_from_vin(text: str) -> str:
        if not vin:
            return ""
        _pm = re.search(r"partner=([A-Za-z0-9_]+)", text or "")
        return (f"https://www.carfax.com/cfm/check_order.cfm?partner={_pm.group(1)}&VIN={vin.upper()}"
                if _pm else "")

    carfax_url = ""
    for a in soup.find_all("a", href=True):
        if "carfax.com" in a["href"].lower():
            carfax_url = a["href"].strip()
            break
    if not carfax_url:
        carfax_url = _scan_carfax(html)          # scan the browser HTML as-is
    if not carfax_url and detail_url:
        # Re-fetch the raw server HTML — this is the reliable source.
        try:
            import requests
            _raw = requests.get(detail_url, timeout=20, headers={"User-Agent": UA}).text
            carfax_url = _scan_carfax(_raw) or _build_from_vin(_raw)
        except Exception as _e:
            logger.warning("carfax raw refetch failed for %s: %s", detail_url, _e)
    if not carfax_url:
        carfax_url = _build_from_vin(html)       # last resort from browser HTML

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
        "CarfaxURL":   carfax_url,
        "DetailURL":   detail_url,
        "DocFee":      doc_fee,
        "TitleTagFee": title_tag_fee,
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
# PLATFORM: DealerCenter (e.g. unitedautomotive501.com)
# Detail URLs: /inventory/<make>/<model>/<stock>/
# Most fields live in an embedded JSON blob with "vin", "year", "make",
# "model", "stockNumber", "transmission", "fuelType", "exteriorColor", "price".
# Mileage is in a separate "mileage":NNNNN JSON snippet. CarFax URL is an
# anchor pointing to carfax.com/vehiclehistory/.
# ===========================================================================

def _dc_collect_detail_links(html: str, base_url: str) -> List[str]:
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
        # DealerCenter detail pages: /inventory/<make>/<model>/<stock>/
        if not re.search(r"/inventory/[a-z0-9\-]+/[a-z0-9\-]+/[a-z0-9\-]+/?$", full, re.I):
            continue
        norm = _normalize_url(full)
        if norm not in seen:
            seen.add(norm)
            links.append(norm)

    return links


def _dc_scrape_detail_page(html: str, detail_url: str = "") -> Optional[Dict[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Primary data source: the "vehicleDetails" JSON object that DealerCenter
    # embeds in a tracking <script>. It contains ONLY the current vehicle —
    # crucial because the page also embeds JSON for related/recently-viewed
    # vehicles. Without anchoring to vehicleDetails, plain `"vin":` regex
    # matches the wrong car's VIN. Falls back to whole-page scan when this
    # block isn't found (defensive — other DealerCenter sites may differ).
    primary_json = html
    vd_match = re.search(r'"vehicleDetails"\s*:\s*\{([^{}]{0,2500})\}', html)
    if vd_match:
        primary_json = vd_match.group(1)

    def _json_field(key: str) -> str:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]*)"', primary_json)
        if not m:
            return ""
        # JSON-encoded strings may contain escape sequences like "\/" (literal
        # forward slash) and "\"". Decode the common ones so they don't bleed
        # into customer-facing text.
        return m.group(1).replace(r'\/', '/').replace(r'\"', '"').strip()

    def _json_int_field(key: str) -> str:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*(\d+)', primary_json)
        return m.group(1) if m else ""

    year         = _json_int_field("year")
    make         = (_json_field("make") or "").title()
    if make:
        make = _MAKE_CAPS.get(make.lower(), make)
    model        = (_json_field("model") or "").title()
    vin          = _json_field("vin").upper()
    stock        = _json_field("stockNumber")
    transmission = _json_field("transmission")
    fuel         = _json_field("fuelType").title()
    body_style   = _json_field("bodyStyle").title()
    ext_color    = _json_field("exteriorColor").title()
    int_color    = _json_field("interiorColor").title()
    price        = _json_int_field("price")
    mileage      = _json_int_field("mileage") or _json_int_field("odometer")

    # Title fallback from <h1>
    if not (year and make and model):
        title_el = soup.find("h1")
        if title_el:
            parsed = _parse_vehicle_title(_clean(title_el.get_text()))
            year  = year  or parsed.get("year", "")
            make  = make  or parsed.get("make", "")
            model = model or parsed.get("model", "")

    # DOM-side spec extraction — DealerCenter renders each per-vehicle spec
    # in a <div class="vehicle-fields-item ...">. Two layouts exist across
    # DealerCenter sites:
    #   (A) AutoGalaxy-style: explicit <span class="vehicle-label">Trim</span>
    #       <span class="vehicle-value">2.0XT PREMIUM</span> siblings.
    #   (B) UnitedAuto-style: an icon span identifies the spec kind via its
    #       class (dws-icons-feature-trim / -engine / -drivetrain etc.) and
    #       the entire item text reads as "Label Value" together
    #       (e.g. "Trim SCAT PACK WIDEBODY SEDAN 4D").
    # Per-vehicle context — these blocks are scoped to THIS vehicle, not
    # related-vehicle widgets, so they're authoritative when present.
    dom_specs: Dict[str, str] = {}
    _icon_to_label = {
        "trim":           "trim",
        "engine":          "engine",
        "drivetrain":      "drivetrain",
        "transmission":    "transmission",
        "vin":             "vin",
        "stock-number":    "stock number",
        "mileage":         "mileage",
        "door":            "doors",
        "exterior-color":  "exterior color",
        "interior-color":  "interior color",
        "mpg":             "mpg",
    }
    for fields_item in soup.find_all(class_=re.compile(r"vehicle-fields-item", re.I)):
        # Layout A: explicit label/value spans
        label_el = fields_item.find(class_=re.compile(r"\bvehicle-label\b", re.I))
        value_el = fields_item.find(class_=re.compile(r"\bvehicle-value\b", re.I))
        if label_el and value_el:
            label = _clean(label_el.get_text()).lower()
            value = _clean(value_el.get_text())
            if label and value:
                dom_specs[label] = value
            continue
        # Layout B: identify spec kind from the icon class, then strip the
        # leading label word from the item's full text.
        icon_el = fields_item.find(class_=re.compile(r"dws-icons-feature-", re.I))
        if not icon_el:
            continue
        icon_kind = ""
        for cls in icon_el.get("class", []):
            if cls.startswith("dws-icons-feature-"):
                icon_kind = cls[len("dws-icons-feature-"):]
                break
        label = _icon_to_label.get(icon_kind)
        if not label:
            continue
        full_text = _clean(fields_item.get_text(" "))
        # The text starts with the human label (e.g. "Trim ", "Stock No. ",
        # "Engine ", "Mileage ", "Doors "). Strip whichever prefix is present.
        value = re.sub(
            r"^\s*(VIN|Mileage|Engine|Drivetrain|Stock\s*No\.?|Stock\s*Number|Transmission|Trim|Doors|Exterior\s*Color|Interior\s*Color|MPG)\s*[:\-]?\s*",
            "",
            full_text,
            flags=re.I,
        ).strip()
        if value:
            dom_specs[label] = value

    # Fill in / override JSON fields with DOM values where the DOM has them.
    trim         = dom_specs.get("trim", "")
    engine       = dom_specs.get("engine", "")
    drive        = dom_specs.get("drivetrain", "") or dom_specs.get("drive train", "")
    doors        = dom_specs.get("doors", "")
    int_color    = int_color or dom_specs.get("interior color", "")
    ext_color    = ext_color or dom_specs.get("exterior color", "")
    if not transmission:
        transmission = dom_specs.get("transmission", "")
    if not mileage:
        mileage = _parse_mileage(dom_specs.get("mileage", ""))
    if not stock:
        stock = dom_specs.get("stock number", "")

    # VIN fallback: scan raw HTML for any 17-char VIN
    if not vin or len(vin) != 17:
        vin_m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", html)
        vin = vin_m.group(1).upper() if vin_m else ""

    # Stock fallback: dws-forms-stock-number class
    if not stock:
        sn_el = soup.find(class_=re.compile(r"dws-forms-stock-number", re.I))
        if sn_el:
            stock = re.sub(r"[^A-Za-z0-9\-]", "", _clean(sn_el.get_text()))

    # Mileage fallback: dws-forms-mileage class
    if not mileage:
        mi_el = soup.find(class_=re.compile(r"dws-forms-mileage", re.I))
        if mi_el:
            mileage = _parse_mileage(_clean(mi_el.get_text()))

    # CarFax URL — DealerCenter exposes a real vehiclehistory URL when CarFax
    # data is available on the vehicle. We grab the first non-badge link.
    carfax_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "carfax.com/vehiclehistory" in href.lower():
            carfax_url = href
            break

    # Equipment list — DealerCenter packs the full feature list in
    # <div class="dws-vehicle-equipments"> (plural — note the trailing 's').
    # This holds every feature on the car: safety, infotainment, comfort, etc.
    # The bot uses this to answer specific feature questions ("does it have
    # bluetooth", "is there a backup camera", etc.).
    feature_text = ""
    eq_el = soup.find(class_=re.compile(r"dws-vehicle-equipments\b", re.I))
    if eq_el:
        raw = _clean(eq_el.get_text(" "))
        if len(raw) > 30:
            feature_text = raw[:3000]

    spec_parts = []
    if trim:         spec_parts.append(f"Trim: {trim}")
    if engine:       spec_parts.append(f"Engine: {engine}")
    if transmission: spec_parts.append(f"Transmission: {transmission}")
    if drive:        spec_parts.append(f"Drive: {drive}")
    if fuel:         spec_parts.append(f"Fuel: {fuel}")
    if doors:        spec_parts.append(f"Doors: {doors}")
    if int_color:    spec_parts.append(f"Interior: {int_color}")
    if body_style:   spec_parts.append(f"Body: {body_style}")

    parts = []
    if spec_parts:   parts.append(" | ".join(spec_parts))
    if feature_text: parts.append("Features: " + feature_text)
    full_description = " || ".join(parts)[:5000]

    if not (year and make):
        return None

    return {
        "Year":        year,
        "Make":        _fix_make(make),
        "Model":       model,
        "Trim":        trim,
        "Color":       ext_color,
        "Price":       price,
        "Mileage":     mileage,
        "VIN":         vin,
        "Stock":       stock,
        "Description": full_description,
        "CarfaxURL":   carfax_url,
        "DetailURL":   detail_url,
    }


# ===========================================================================
# PLATFORM: WooCommerce-based custom inventory (e.g. autogalaxyservice.com)
# Detail URLs: /inventory/<year>-<make>-<model>-<trim>/
# Pagination: /inventory/page/N/
# Specs live in a single attributes table (one row per Drive Train / Engine /
# VIN / Mileage / Transmission / Cylinders / Fuel Economy). Title in <h1> or
# h2.entry-title. Price in <p class="price">. Features in <ul> under a
# "Vehicle Options" heading. No CarFax exposure.
# ===========================================================================

def _wc_collect_detail_links(html: str, base_url: str) -> List[str]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    links = []

    # Detail URL pattern: /inventory/<year-make-model-...>/  where the slug
    # starts with a 4-digit year. Excludes /inventory/page/N/ pagination links.
    detail_pat = re.compile(r"/inventory/((?:19|20)\d{2}-[a-z0-9\-]+)/?$", re.I)
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("mailto:") or href.startswith("javascript"):
            continue
        full = urljoin(base_url, href)
        if urlparse(full).netloc != urlparse(base_url).netloc:
            continue
        if not detail_pat.search(full):
            continue
        norm = _normalize_url(full)
        if norm not in seen:
            seen.add(norm)
            links.append(norm)

    return links


def _wc_scrape_detail_page(html: str, detail_url: str = "") -> Optional[Dict[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title from h1 (preferred) or the entry-title h2 fallback.
    title_el = soup.find("h1")
    if not title_el or not _clean(title_el.get_text()):
        title_el = soup.find(class_=re.compile(r"entry-title", re.I))
    title_text = _clean(title_el.get_text()) if title_el else ""
    parsed = _parse_vehicle_title(title_text) if title_text else {}

    # Price from <p class="price"> (or first .price element). Strip HTML
    # entities like &#36; (which is "$").
    price = ""
    price_el = soup.find(class_=re.compile(r"\bprice\b", re.I))
    if price_el:
        price_text = price_el.get_text(" ").strip()
        price_m = re.search(r"([\d,]+)", price_text)
        if price_m:
            price = _parse_price(price_m.group(1))

    # Stock from <span class="sku"> — comes formatted like "#0002".
    stock = ""
    sku_el = soup.find(class_=re.compile(r"\bsku\b", re.I))
    if sku_el:
        stock = re.sub(r"[^A-Za-z0-9\-]", "", _clean(sku_el.get_text()))

    # Spec table — one row per spec, <th>label</th> <td>value</td>.
    specs = {}
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = _clean(th.get_text()).lower()
            value = _clean(td.get_text())
            if label and value:
                specs[label] = value

    vin          = (specs.get("vin", "") or "").upper()
    mileage      = _parse_mileage(specs.get("mileage", ""))
    engine       = specs.get("engine", "")
    transmission = specs.get("transmission", "")
    drive        = specs.get("drive train", "") or specs.get("drivetrain", "")
    cylinders    = specs.get("cylinders", "")
    fuel_econ    = specs.get("fuel economy", "")

    # VIN fallback: scan raw HTML for any 17-char VIN.
    if not vin or len(vin) != 17:
        vin_m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", html)
        vin = vin_m.group(1).upper() if vin_m else ""

    # Vehicle Options — <ul> after a "Vehicle Options" heading.
    options = []
    options_marker = soup.find(string=re.compile(r"Vehicle\s+Options", re.I))
    if options_marker:
        # Walk to the next <ul> sibling
        parent = options_marker.find_parent()
        ul = None
        if parent:
            ul = parent.find_next("ul")
        if ul:
            options = [_clean(li.get_text()) for li in ul.find_all("li") if _clean(li.get_text())]

    spec_parts = []
    if engine:       spec_parts.append(f"Engine: {engine}")
    if transmission: spec_parts.append(f"Transmission: {transmission}")
    if drive:        spec_parts.append(f"Drive: {drive}")
    if cylinders:    spec_parts.append(f"Cylinders: {cylinders}")
    if fuel_econ:    spec_parts.append(f"Fuel Economy: {fuel_econ}")

    parts = []
    if spec_parts: parts.append(" | ".join(spec_parts))
    if options:    parts.append("Options: " + ", ".join(options))
    full_description = " || ".join(parts)[:2000]

    if not parsed.get("year") and not parsed.get("make"):
        return None

    return {
        "Year":        parsed.get("year", ""),
        "Make":        parsed.get("make", ""),
        "Model":       parsed.get("model", ""),
        "Trim":        parsed.get("trim", ""),
        "Color":       "",
        "Price":       price,
        "Mileage":     mileage,
        "VIN":         vin,
        "Stock":       stock,
        "Description": full_description,
        "CarfaxURL":   "",  # Platform doesn't expose CarFax — omit cleanly.
        "DetailURL":   detail_url,
    }


# ===========================================================================
# Public entry point
# ===========================================================================

def scrape_dealer_inventory(url: str, max_pages: int = 10, max_vehicles: int = 0,
                            on_vehicle_scraped=None, should_skip=None) -> List[Dict[str, str]]:
    """
    Scrape full vehicle inventory from a dealer website.
    Detects the platform and routes to the correct scraping logic.

    If on_vehicle_scraped is provided, it will be called with each vehicle dict
    immediately after that vehicle finishes scraping. Lets the caller persist
    rows incrementally so progress isn't lost on a crash mid-scrape.

    If should_skip(detail_url) is provided and returns True, that detail page
    will be skipped (not scraped). Lets the caller resume an interrupted
    scrape by skipping vehicles already saved in a recent session.
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
                # Load first page and detect platform. Quick load just to
                # detect — if it's dealercarsearch we re-load with scrolling
                # so lazy-loaded inventory cards get picked up.
                first_html = _load_page_playwright(browser, url)
                if not first_html:
                    return []

                platform = _detect_platform(first_html, url)
                logger.info("Platform detected: %s", platform)

                # Route to platform-specific collectors
                if platform == "dealercarsearch":
                    collect_fn = _dcs_collect_detail_links
                    detail_fn  = _dcs_scrape_detail_page
                    paginate   = True
                    # Re-load with scrolling first so any lazy-loaded cards on
                    # page 1 also get picked up.
                    scrolled = _load_page_playwright_with_scroll(browser, url)
                    if scrolled:
                        first_html = scrolled
                elif platform == "dealercenter":
                    collect_fn = _dc_collect_detail_links
                    detail_fn  = _dc_scrape_detail_page
                    paginate   = True
                    # DealerCenter listing pages also benefit from a scroll
                    # to surface the full grid before we read the HTML.
                    scrolled = _load_page_playwright_with_scroll(browser, url)
                    if scrolled:
                        first_html = scrolled
                elif platform == "woocommerce_inv":
                    collect_fn = _wc_collect_detail_links
                    detail_fn  = _wc_scrape_detail_page
                    paginate   = True
                else:
                    collect_fn = _ds_collect_detail_links
                    detail_fn  = _ds_scrape_detail_page
                    paginate   = True

                # ── Phase 1: collect detail links ───────────────────────
                all_detail_links: List[str] = []

                pages = range(1, max_pages + 1) if paginate else range(1, 2)
                for page_num in pages:
                    if page_num == 1:
                        page_url = url
                    elif platform == "woocommerce_inv":
                        # WooCommerce-style path pagination: /inventory/page/N/
                        # Insert /page/N/ before any trailing slash on the base URL.
                        base = url.rstrip("/")
                        page_url = f"{base}/page/{page_num}/"
                    elif platform == "dealercenter":
                        # DealerCenter inventory pagination uses ?page_no=N
                        # (NOT the generic ?page=N). Confirmed by inspecting
                        # autogalaxysales.com / unitedautomotive501.com which
                        # both use this query param. Preserve any existing
                        # query string on the inventory URL.
                        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
                        parts = urlparse(url)
                        qs = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "page_no"]
                        qs.append(("page_no", str(page_num)))
                        page_url = urlunparse(parts._replace(query=urlencode(qs)))
                    else:
                        # DealerSocket, DealerCarSearch use ?page=N query-string pagination.
                        page_url = _build_page_url(url, page_num)
                    if page_num == 1:
                        html = first_html
                    elif platform in ("dealercarsearch", "dealercenter"):
                        html = _load_page_playwright_with_scroll(browser, page_url)
                    else:
                        html = _load_page_playwright(browser, page_url)
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
                    if should_skip and should_skip(detail_url):
                        logger.info("Skipping %d/%d (recently scraped): %s",
                                    i + 1, len(all_detail_links), detail_url)
                        continue
                    html = _load_page_playwright(browser, detail_url,
                                                 wait_selector="a[href*='carfax']")
                    if not html:
                        continue
                    vehicle = detail_fn(html, detail_url)
                    if vehicle:
                        vehicles.append(vehicle)
                        logger.info("Scraped %d/%d: %s %s %s",
                                    i + 1, len(all_detail_links),
                                    vehicle["Year"], vehicle["Make"], vehicle["Model"])
                        # Incremental save callback - lets caller persist rows
                        # one at a time so progress survives mid-scrape crashes.
                        if on_vehicle_scraped:
                            try:
                                on_vehicle_scraped(vehicle)
                            except Exception as _e:
                                logger.warning("on_vehicle_scraped callback failed: %s", _e)
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

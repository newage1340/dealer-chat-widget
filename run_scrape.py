"""Standalone inventory scrape — runs in its OWN process, spawned by the web
app's scheduler every 30 minutes.

Why this exists: the scrape drives Chromium + a lot of HTML parsing. Running it
*inside* the single gunicorn web worker starved that worker long enough for
gunicorn's request timeout to KILL and restart it — which dropped any live call
into silence with nothing in the app log, then "recovered on its own" when the
worker rebooted. A separate process has its own GIL and memory, so the web
worker stays responsive during a scrape. It writes to the same SQLite DB the web
app reads (WAL mode handles the concurrent access).

The three env vars below are set BEFORE importing app so the import runs
init_db() ONLY — no web server, no scheduler, no cache warmer, no startup scrape,
and (belt-and-suspenders) never a DEV_CLEAR_DB wipe. Then we run one full
inventory refresh and exit.
"""
import os

os.environ["DISABLE_SCHEDULER"] = "1"    # don't start a second scheduler
os.environ["SKIP_STARTUP_SCRAPE"] = "1"  # don't kick the module-level scrape
os.environ["DEV_CLEAR_DB"] = "0"         # never wipe the DB from the scraper

import app  # noqa: E402  (env must be set first) — import runs init_db() only

if __name__ == "__main__":
    app.app.logger.info("run_scrape: starting standalone inventory refresh")
    app.refresh_all_inventory(max_vehicles=0)
    app.app.logger.info("run_scrape: inventory refresh complete")

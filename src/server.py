"""
Application factory: builds providers, storage, the pipeline and the Flask app.

Run with:  gunicorn --chdir src "server:create_app()"     (production)
       or: python src/main.py                              (development)
"""

import json
import logging
from pathlib import Path

from flask import Flask

from channels import admin, sms, vapi, voice, web
from channels.context import ChannelContext
from config import ROOT, Config, LLMConfig
from core import UsageUnit
from domains import DomainRegistry
from pipeline import ActionExecutor, ConversationHandler, OptOutHandler, Services, TurnRunner, WebProfileGate
from prompts import PromptComposer
from providers import LLMProvider, MessagingProvider, ModelTier, PriceTable, UsageSink
from providers.openai_compat import OpenAICompatibleLLM
from providers.router import LLMRouter, Route
from providers.twilio_sms import LoggingMessaging, TwilioMessaging
from storage import SqliteStore
from tenants import TenantDirectory

log = logging.getLogger(__name__)


def load_prices(path: str | Path) -> PriceTable:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("no price table loaded from %s: %s", path, e)
        return PriceTable()
    prices = {}
    for provider, models in raw.items():
        if provider.startswith("_"):
            continue
        for model, units in models.items():
            prices[(provider, model)] = {UsageUnit(u): float(v) for u, v in units.items()}
    return PriceTable(prices)


def build_llm(cfg: LLMConfig, sink: UsageSink, prices: PriceTable) -> LLMRouter:
    """
    Routes per tier, in priority order. "local" is any OpenAI-compatible server (e.g. Ollama on the GPU box);
    its concurrency cap makes overflow and failures spill to OpenAI.
    """
    openai_route = local_route = None
    if cfg.openai_api_key:
        openai_route = Route(OpenAICompatibleLLM(
            name="openai", api_key=cfg.openai_api_key,
            models={ModelTier.FAST: cfg.openai_model_fast, ModelTier.CAPABLE: cfg.openai_model_capable},
        ))
    if cfg.local_base_url and cfg.local_model_fast:
        local_route = Route(OpenAICompatibleLLM(
            name="local", base_url=cfg.local_base_url, api_key=cfg.local_api_key, timeout_s=60,
            models={ModelTier.FAST: cfg.local_model_fast, ModelTier.CAPABLE: cfg.local_model_capable or cfg.local_model_fast},
        ), max_concurrency=cfg.local_max_concurrency)

    order = [local_route, openai_route] if cfg.primary == "local" else [openai_route, local_route]
    routes = [r for r in order if r is not None]
    if not routes:
        log.warning("No LLM provider configured (set OPENAI_API_KEY and/or LOCAL_LLM_BASE_URL); replies will be fallbacks.")
    else:
        log.info("LLM routes: %s", " -> ".join(r.provider.name for r in routes))
    return LLMRouter({ModelTier.FAST: routes, ModelTier.CAPABLE: routes}, sink=sink, prices=prices)


def build_messaging(cfg: Config) -> MessagingProvider:
    if cfg.twilio_account_sid and cfg.twilio_auth_token:
        return TwilioMessaging(account_sid=cfg.twilio_account_sid, auth_token=cfg.twilio_auth_token,
                               messaging_service_sid=cfg.twilio_messaging_sid)
    log.warning("Twilio credentials not set; outbound SMS will be logged, not sent.")
    return LoggingMessaging()


def create_app(
    config:          Config | None = None,
    *,
    llm:             LLMProvider | None = None,
    messaging:       MessagingProvider | None = None,
    start_scheduler: bool | None = None,
) -> Flask:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = config or Config.from_env()

    registry = DomainRegistry.from_directory()
    store = SqliteStore(cfg.db_path)
    prices = load_prices(cfg.llm.prices_file or cfg.data_dir / "prices.json")
    services = Services(
        registry       = registry,
        directory      = TenantDirectory.from_directory(cfg.tenants_dir, registry),
        store          = store,
        composer       = PromptComposer(),
        llm            = llm or build_llm(cfg.llm, store, prices),
        messaging      = messaging or build_messaging(cfg),
        usage          = store,
        default_sender = cfg.twilio_default_from,
    )
    executor = ActionExecutor(services)
    runner = TurnRunner(services, [OptOutHandler(), WebProfileGate(), ConversationHandler(services)], executor)
    context = ChannelContext(config=cfg, services=services, runner=runner, executor=executor)

    app = Flask(__name__, static_folder=str(ROOT / "static"), static_url_path="/static")
    app.secret_key = cfg.secret_key
    app.extensions["front_desk"] = context
    for module in (web, sms, voice, vapi, admin):
        app.register_blueprint(module.create_blueprint(context))

    if cfg.scheduler_enabled if start_scheduler is None else start_scheduler:
        import scheduler
        app.extensions["scheduler"] = scheduler.start(services, executor, reminder_lead_minutes=cfg.reminder_lead_minutes)
    return app

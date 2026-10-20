"""
Runtime configuration, read from environment variables.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _flag(name: str, default: bool = False) -> bool:
    return _env(name, "1" if default else "0").lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True, slots=True, kw_only=True)
class LLMConfig:
    """
    `primary` picks which route is tried first: "openai" or "local".
    The local route is any OpenAI-compatible server (Ollama, llama.cpp, vLLM) at `local_base_url`;
    `local_max_concurrency` caps in-flight requests so overflow spills to OpenAI.
    """

    primary:               str = "openai"
    openai_api_key:        str = ""
    openai_model_fast:     str = "gpt-4o-mini"
    openai_model_capable:  str = "gpt-4o-mini"
    local_base_url:        str = ""
    local_api_key:         str = ""
    local_model_fast:      str = ""
    local_model_capable:   str = ""
    local_max_concurrency: int = 2
    prices_file:           str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    data_dir:              Path = ROOT / "data"
    tenants_dir:           Path = ROOT / "data" / "tenants"
    db_path:               Path = ROOT / "data" / "frontdesk.db"
    secret_key:            str = "dev-secret-change-me"
    admin_token:           str = ""
    upload_token:          str = ""
    public_base_url:       str = ""
    default_slug:          str = ""
    terms_url:             str = "https://inventiq.net/terms.html"
    privacy_url:           str = "https://inventiq.net/privacy.html"
    twilio_account_sid:    str = ""
    twilio_auth_token:     str = ""
    twilio_messaging_sid:  str = ""
    twilio_default_from:   str = ""
    twilio_voice:          str = "Polly.Joanna-Neural"
    scheduler_enabled:     bool = True
    reminder_lead_minutes: int = 60
    llm:                   LLMConfig = field(default_factory=LLMConfig)

    @classmethod
    def from_env(cls) -> "Config":
        data_dir = Path(_env("DATA_DIR", str(ROOT / "data")))
        return cls(
            data_dir              = data_dir,
            tenants_dir           = Path(_env("TENANTS_DIR", str(data_dir / "tenants"))),
            db_path               = Path(_env("DB_PATH", str(data_dir / "frontdesk.db"))),
            secret_key            = _env("FLASK_SECRET", "dev-secret-change-me"),
            admin_token           = _env("ADMIN_TOKEN"),
            upload_token          = _env("INVENTORY_UPLOAD_TOKEN"),
            public_base_url       = _env("PUBLIC_BASE_URL").rstrip("/"),
            default_slug          = _env("WIDGET_DEFAULT_SLUG"),
            terms_url             = _env("PRIMER_TERMS_URL", "https://inventiq.net/terms.html"),
            privacy_url           = _env("PRIMER_PRIVACY_URL", "https://inventiq.net/privacy.html"),
            twilio_account_sid    = _env("TWILIO_ACCOUNT_SID"),
            twilio_auth_token     = _env("TWILIO_AUTH_TOKEN"),
            twilio_messaging_sid  = _env("TWILIO_MESSAGING_SERVICE_SID"),
            twilio_default_from   = _env("TWILIO_DEFAULT_FROM"),
            twilio_voice          = _env("TWILIO_VOICE", "Polly.Joanna-Neural"),
            scheduler_enabled     = not _flag("DISABLE_SCHEDULER"),
            reminder_lead_minutes = _int("REMINDER_LEAD_MINUTES", 60),
            llm = LLMConfig(
                primary               = _env("LLM_PRIMARY", "openai").lower(),
                openai_api_key        = _env("OPENAI_API_KEY"),
                openai_model_fast     = _env("OPENAI_MODEL_FAST", _env("OPENAI_VOICE_MODEL", "gpt-4o-mini")),
                openai_model_capable  = _env("OPENAI_MODEL_CAPABLE", _env("OPENAI_MODEL", "gpt-4o-mini")),
                local_base_url        = _env("LOCAL_LLM_BASE_URL"),
                local_api_key         = _env("LOCAL_LLM_API_KEY"),
                local_model_fast      = _env("LOCAL_LLM_MODEL_FAST", _env("LOCAL_LLM_MODEL")),
                local_model_capable   = _env("LOCAL_LLM_MODEL_CAPABLE", _env("LOCAL_LLM_MODEL")),
                local_max_concurrency = _int("LOCAL_LLM_MAX_CONCURRENCY", 2),
                prices_file           = _env("PRICES_FILE"),
            ),
        )

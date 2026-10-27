"""
LLM adapter for any server that speaks the OpenAI chat-completions API.

That covers OpenAI itself and the common self-hosted runtimes
(Ollama, llama.cpp server, vLLM, LM Studio), so cloud vs. local is just a base URL.
"""

import time
from collections.abc import Mapping
from datetime import datetime, timezone

from core import ServiceKind, UsageRecord, UsageUnit

from .errors import ProviderError, ProviderTimeout, ProviderUnavailable
from .llm import LLMRequest, LLMResponse, ModelTier


class OpenAICompatibleLLM:
    """
    `models` maps each tier to the model name this server should use for it.
    A tier missing from the map falls back to the FAST model.
    """

    def __init__(
        self,
        *,
        name:        str,
        models:      Mapping[ModelTier, str],
        api_key:     str = "",
        base_url:    str | None = None,
        timeout_s:   float = 30.0,
    ):
        from openai import OpenAI

        self.name = name
        self._models = dict(models)
        self._timeout_s = timeout_s
        # Local servers usually ignore the key but the SDK requires a non-empty one.
        self._client = OpenAI(api_key=api_key or "not-needed", base_url=base_url, max_retries=0)

    def model_for(self, tier: ModelTier) -> str:
        return self._models.get(tier) or self._models[ModelTier.FAST]

    def complete(self, request: LLMRequest) -> LLMResponse:
        import openai

        model = self.model_for(request.tier)
        kwargs: dict = {
            "model":      model,
            "messages":   [{"role": str(m.role), "content": m.content} for m in request.messages],
            "max_tokens": request.max_tokens,
            "timeout":    request.timeout_s or self._timeout_s,
        }
        if request.temperature is not None:
            kwargs["temperature"] = request.temperature

        started = time.monotonic()
        try:
            resp = self._client.chat.completions.create(**kwargs)
        except openai.APITimeoutError as e:
            raise ProviderTimeout(self.name, str(e)) from e
        except (openai.RateLimitError, openai.APIConnectionError) as e:
            raise ProviderUnavailable(self.name, str(e)) from e
        except openai.APIStatusError as e:
            raise ProviderError(self.name, str(e), retryable=e.status_code >= 500) from e

        choice = resp.choices[0]
        usage = getattr(resp, "usage", None)
        units: dict[UsageUnit, float] = {}
        if usage is not None:
            cached = getattr(getattr(usage, "prompt_tokens_details", None), "cached_tokens", 0) or 0
            units[UsageUnit.INPUT_TOKENS] = float((usage.prompt_tokens or 0) - cached)
            units[UsageUnit.CACHED_INPUT_TOKENS] = float(cached)
            units[UsageUnit.OUTPUT_TOKENS] = float(usage.completion_tokens or 0)

        record = UsageRecord(
            provider    = self.name,
            service     = ServiceKind.LLM,
            operation   = model,
            occurred_at = datetime.now(timezone.utc),
            units       = units,
            purpose     = request.purpose,
            latency_ms  = (time.monotonic() - started) * 1000,
            tenant_id   = request.tenant_id,
            channel     = request.channel,
        )
        return LLMResponse(
            text          = (choice.message.content or "").strip(),
            provider      = self.name,
            model         = model,
            usage         = record,
            finish_reason = choice.finish_reason or "",
        )

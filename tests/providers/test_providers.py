import pytest

from core import Address, AddressKind, Role, UsageUnit
from providers import (
    ChatMessage,
    LLMProvider,
    LLMRequest,
    MessagingProvider,
    OutboundMessage,
    PriceTable,
    ProviderError,
    ProviderTimeout,
)
from providers.fakes import FakeLLM, FakeMessaging


def request(text="hi", **kwargs):
    return LLMRequest(messages=[ChatMessage(Role.USER, text)], **kwargs)


def test_fakes_satisfy_protocols():
    llm: LLMProvider = FakeLLM(["x"])
    messaging: MessagingProvider = FakeMessaging()
    assert llm.name and messaging.name


def test_fake_llm_scripted_replies_in_order_and_records_requests():
    llm = FakeLLM(["first", "second"])

    assert llm.complete(request("a")).text == "first"
    assert llm.complete(request("b", purpose="keep_warm")).text == "second"
    assert [r.messages[0].content for r in llm.requests] == ["a", "b"]
    assert llm.requests[1].purpose == "keep_warm"


def test_fake_llm_reply_function_and_usage_tagging():
    llm = FakeLLM(lambda req: req.messages[-1].content.upper())

    response = llm.complete(request("echo", purpose="followup"))

    assert response.text == "ECHO"
    assert response.usage.purpose == "followup"
    assert response.usage.units[UsageUnit.OUTPUT_TOKENS] > 0


def test_fake_messaging_records_sent():
    messaging = FakeMessaging()
    phone = Address(AddressKind.PHONE, "+13175550100")

    receipt = messaging.send(OutboundMessage(to=phone, sender=phone, text="hello"))

    assert messaging.sent[0].text == "hello"
    assert receipt.usage.units[UsageUnit.SMS_SEGMENTS] == 1


def test_price_table_estimates_with_model_fallback():
    table = PriceTable({
        ("openai", "small"): {UsageUnit.INPUT_TOKENS: 1e-6, UsageUnit.OUTPUT_TOKENS: 4e-6},
        ("sms-co", ""): {UsageUnit.SMS_SEGMENTS: 0.01},
    })

    assert table.estimate("openai", "small", {UsageUnit.INPUT_TOKENS: 1000, UsageUnit.OUTPUT_TOKENS: 100}) == pytest.approx(0.0014)
    assert table.estimate("sms-co", "any", {UsageUnit.SMS_SEGMENTS: 2}) == pytest.approx(0.02)
    assert table.estimate("openai", "small", {UsageUnit.CALL_SECONDS: 5}) is None
    assert table.estimate("unknown", "", {UsageUnit.REQUESTS: 1}) is None


def test_error_retryability():
    assert ProviderTimeout("x", "slow").retryable
    assert not ProviderError("x", "bad request").retryable
    assert ProviderError("x", "502", retryable=True).retryable

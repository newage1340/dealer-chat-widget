"""
Smoke tests through the real Flask app with a scripted LLM and a recording messenger.
"""

import json
import shutil
from pathlib import Path

import pytest

from config import Config
from providers.fakes import FakeLLM
from providers.twilio_sms import LoggingMessaging
from server import create_app

ROOT = Path(__file__).resolve().parent.parent
DEALER = "+13175550100"
PLUMBER = "+13175550200"
CUSTOMER = "+13175550111"

BOOKING = ('You\'re all set, Alex! Your visit is confirmed for Saturday at 2 PM for the 2017 Buick Encore Preferred.\n'
           'META_JSON: {"confirmed": true, "visit_time": "Saturday at 2 PM", "visit_time_iso": "2026-10-24T14:00:00", '
           '"subject": "2017 Buick Encore Preferred", "customer_name": "Alex", "customer_email": "alex@example.com", '
           '"details": {"trade_in": "", "financing": "cash"}}')


def scripted(request):
    if request.purpose == "call_summary":
        return "RECAP\n  Caller wants a plumber for a leaking water heater."
    last = request.messages[-1].content.lower()
    if "book it" in last:
        return BOOKING
    if "transfer me" in last:
        return "Okay, connecting you now.\n[TRANSFER]"
    if "that's all" in last:
        return "Alright, take it easy!\n[TAKE_MESSAGE]"
    if "link" in last:
        return "Sure, I'll send over the listing for the 2017 Buick Encore Preferred."
    return "We've got a few - want me to go over them?"


@pytest.fixture
def env(tmp_path):
    tenants = tmp_path / "tenants"
    tenants.mkdir()
    gov = json.loads((ROOT / "data/tenants/hoosiers.json").read_text())
    gov |= {"endpoints": {"sms": [DEALER], "voice": [DEALER]}, "staff": {"phones": ["+13175550999"], "emails": []}}
    (tenants / "hoosiers.json").write_text(json.dumps(gov))
    plumber = json.loads((ROOT / "data/tenants/demo-plumber.json").read_text())
    plumber |= {"endpoints": {"voice": [PLUMBER]}, "staff": {"phones": ["+13175550888"]}, "transfer_to": "+13175550777"}
    (tenants / "demo-plumber.json").write_text(json.dumps(plumber))
    shutil.copy(ROOT / "data/prices.json", tmp_path / "prices.json")

    messaging = LoggingMessaging()
    llm = FakeLLM(scripted)
    app = create_app(Config(data_dir=tmp_path, tenants_dir=tenants, db_path=tmp_path / "t.db", upload_token="secret"),
                     llm=llm, messaging=messaging, start_scheduler=False)
    return app.test_client(), llm, messaging


def test_web_widget_flow_with_profile_gate_link_and_booking(env):
    client, llm, messaging = env
    page = client.get("/widget/hoosiers").get_data(as_text=True)
    assert "SUVs" in page and "Hybrids" not in page  # topics filtered by the tenant's catalog

    assert client.post("/widget/welcome", json={"session_id": "s1", "slug": "hoosiers"}).json["welcome"].startswith("Hi, my name is Dave")
    gated = client.post("/chat", json={"session_id": "s1", "slug": "hoosiers", "message": "any SUVs?"}).json
    assert gated["needs_profile"] and not llm.requests

    assert client.post("/widget/profile", json={"session_id": "s1", "slug": "hoosiers", "name": "Alex", "phone": "317-555-0111"}).json["ok"]
    assert "want me to go over" in client.post("/chat", json={"session_id": "s1", "slug": "hoosiers", "message": "any SUVs?"}).json["reply"]

    linked = client.post("/chat", json={"session_id": "s1", "slug": "hoosiers", "message": "can you send me the link for the Encore Preferred?"}).json
    assert "hoosiersautosale.com" in linked["reply"]

    booked = client.post("/chat", json={"session_id": "s1", "slug": "hoosiers", "message": "book it"}).json
    assert "META_JSON" not in booked["reply"]
    sent_to = [m.to.value for m in messaging.sent]
    assert CUSTOMER in sent_to and "+13175550999" in sent_to  # customer confirmation + staff alert
    history = client.post("/widget/history", json={"session_id": "s1", "slug": "hoosiers"}).json["messages"]
    assert len(history) == 8

    assert client.post("/widget/clear-session", json={"session_id": "s1", "slug": "hoosiers"}).json["ok"]
    assert client.post("/widget/history", json={"session_id": "s1", "slug": "hoosiers"}).json["messages"] == []


def test_sms_primer_and_opt_out(env):
    client, _, _ = env
    first = client.post("/sms", data={"From": CUSTOMER, "To": DEALER, "Body": "any trucks?"}).get_data(as_text=True)
    assert "want me to go over" in first and "Reply STOP" in first
    second = client.post("/sms", data={"From": CUSTOMER, "To": DEALER, "Body": "ok"}).get_data(as_text=True)
    assert "Reply STOP to opt out." not in second
    assert "unsubscribed" in client.post("/sms", data={"From": CUSTOMER, "To": DEALER, "Body": "STOP"}).get_data(as_text=True)
    assert "<Message>" not in client.post("/sms", data={"From": CUSTOMER, "To": DEALER, "Body": "hello?"}).get_data(as_text=True)


def test_twilio_voice_greeting_turn_and_handoff(env):
    client, _, messaging = env
    greeting = client.post("/voice", data={"CallSid": "CA1", "From": CUSTOMER, "To": PLUMBER}).get_data(as_text=True)
    assert "Circle City Plumbing" in greeting and "<Gather" in greeting
    turn = client.post("/voice/handle?call_sid=CA1", data={"From": CUSTOMER, "To": PLUMBER, "SpeechResult": "my water heater is leaking", "Confidence": "0.9"}).get_data(as_text=True)
    assert "<Gather" in turn
    done = client.post("/voice/handle?call_sid=CA1", data={"From": CUSTOMER, "To": PLUMBER, "SpeechResult": "that's all thanks", "Confidence": "0.9"}).get_data(as_text=True)
    assert "<Hangup" in done
    staff = [m for m in messaging.sent if m.to.value == "+13175550888"]
    assert staff and "Call back:" in staff[0].text


def test_vapi_greeting_and_transfer(env):
    client, _, _ = env
    call = {"id": "v1", "customer": {"number": CUSTOMER}, "phoneNumber": {"number": PLUMBER}}
    greet = client.post("/vapi/chat/completions", json={"stream": False, "call": call, "messages": []}).json
    assert "Circle City Plumbing" in greet["choices"][0]["message"]["content"]
    xfer = client.post("/vapi/chat/completions", json={
        "stream": False, "call": call, "messages": [{"role": "user", "content": "transfer me please"}],
        "tools": [{"type": "function", "function": {"name": "transferCall"}}],
    }).json
    tool = xfer["choices"][0]["message"]["tool_calls"][0]["function"]
    assert tool["name"] == "transferCall" and json.loads(tool["arguments"])["destination"] == "+13175550777"


def test_catalog_upload_replaces_and_persists(env, tmp_path):
    client, _, _ = env
    rows = [{"Year": "2019", "Make": "Honda", "Model": "Civic", "Trim": "EX", "Price": "15900", "Stock": "A1"}]
    assert client.post("/admin/catalog-upload", json={"slug": "hoosiers", "items": rows}).status_code == 401
    assert client.post("/admin/inventory-upload", json={"twilio_number": DEALER, "vehicles": []},
                       headers={"X-Upload-Token": "secret"}).status_code == 400
    ok = client.post("/admin/inventory-upload", json={"twilio_number": DEALER, "vehicles": rows},
                     headers={"X-Upload-Token": "secret"}).json
    assert ok == {"ok": True, "tenant": "hoosiers", "items": 1}
    saved = json.loads((tmp_path / "tenants" / "hoosiers.json").read_text())
    assert saved["catalog"] == rows

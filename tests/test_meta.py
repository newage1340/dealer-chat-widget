from pipeline.meta import parse_reply


def test_booking_profile_and_tokens_are_extracted_and_hidden():
    raw = ('Sweet, you\'re on the books for 9 tomorrow! See https://example.com/x\n'
           'META_JSON: {"confirmed": true, "visit_time": "9 AM tomorrow", "visit_time_iso": "2026-10-21T09:00:00", '
           '"subject": "2018 Ford Transit 250", "customer_name": "Evan Lee", "customer_email": "", "details": {"trade_in": "2006 Mustang"}}\n'
           'META_PHONE: (317) 555-0100\n[TAKE_MESSAGE]')
    parsed = parse_reply(raw)
    assert parsed.text == "Sweet, you're on the books for 9 tomorrow! See"
    assert parsed.booking["details"] == {"trade_in": "2006 Mustang"}
    assert parsed.profile == {"first_name": "Evan", "phone": "+13175550100"}
    assert parsed.tokens == {"TAKE_MESSAGE"}


def test_plain_reply_passes_through():
    parsed = parse_reply("We've got two trucks under 20k.")
    assert parsed.text == "We've got two trucks under 20k." and parsed.booking is None and not parsed.tokens

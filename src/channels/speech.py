"""
Turning reply text into something a text-to-speech engine reads naturally.

Domain packs supply pronunciations (e.g. "F-250" -> "F two-fifty") and whether years are spoken
("2015" -> "twenty fifteen"); engine-specific fixes live here.
"""

import re

from domains import DomainPack

_ONES = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
         "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"]
_TENS = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]
_YEAR_RE = re.compile(r"(?<![\d$,.])\b(19[5-9]\d|20[0-3]\d)\b(?!\d)")
_SSML_TAG_RE = re.compile(r"<(break|emphasis|prosody|say-as|sub|p|s)\b", re.I)
_BIG_NUMBER_RE = re.compile(r"\$\s?\d{1,3}(?:,\d{3})+(?:\.\d+)?|\$\s?\d+(?:\.\d+)?|\b\d{1,3}(?:,\d{3})+\b")


def int_to_words(n: int) -> str:
    if n < 20:
        return _ONES[n]
    if n < 100:
        return _TENS[n // 10] + (" " + _ONES[n % 10] if n % 10 else "")
    if n < 1000:
        return _ONES[n // 100] + " hundred" + (" " + int_to_words(n % 100) if n % 100 else "")
    if n < 1_000_000:
        return int_to_words(n // 1000) + " thousand" + (" " + int_to_words(n % 1000) if n % 1000 else "")
    return str(n)


def spell_year(year: int) -> str:
    if 2000 <= year <= 2009:
        return "two thousand" + (" " + _ONES[year % 10] if year % 10 else "")
    hi, lo = divmod(year, 100)
    tail = "hundred" if lo == 0 else (f"oh {_ONES[lo]}" if lo < 10 else int_to_words(lo))
    return f"{int_to_words(hi)} {tail}"


def apply_domain(text: str, pack: DomainPack) -> str:
    for pattern, say in pack.voice.pronunciations:
        text = re.sub(pattern, say, text, flags=re.I)
    if pack.voice.speak_years:
        text = _YEAR_RE.sub(lambda m: spell_year(int(m.group(1))), text)
    return text


def speak_big_numbers(text: str) -> str:
    """
    "$7,500" -> "seven thousand five hundred dollars" (ElevenLabs misreads grouped digits).
    """
    def repl(m: re.Match) -> str:
        token = m.group(0)
        digits = re.sub(r"[^\d]", "", token.split(".")[0])
        if not digits:
            return token
        return int_to_words(int(digits)) + (" dollars" if token.lstrip().startswith("$") else "")
    return _BIG_NUMBER_RE.sub(repl, text)


def for_polly(text: str, pack: DomainPack) -> str:
    """
    Text for Twilio <Say> with Amazon Polly: SSML is kept and wrapped in <speak> when present.
    """
    text = apply_domain((text or "").strip(), pack)
    text = text.replace("&", " and ")
    if _SSML_TAG_RE.search(text) and not text.lower().startswith("<speak>"):
        return f"<speak>{text}</speak>"
    return text


def for_elevenlabs(text: str, pack: DomainPack) -> str:
    """
    Text for Vapi/ElevenLabs: SSML stripped, prices and grouped numbers spelled out.
    """
    text = apply_domain((text or "").strip(), pack)
    text = re.sub(r"<[^>]+>", "", text)
    text = speak_big_numbers(text)
    return re.sub(r"\s+", " ", text).strip()

"""
Business-hours parsing and the facts derived from it (open now, next open day, 7-day schedule).

Worked out in code and handed to the model as facts:
models are unreliable at date arithmetic and will happily offer "tomorrow" when tomorrow is closed.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
_DAY_ALIASES = {
    "mon": "Monday", "monday": "Monday",
    "tue": "Tuesday", "tues": "Tuesday", "tuesday": "Tuesday",
    "wed": "Wednesday", "weds": "Wednesday", "wednesday": "Wednesday",
    "thu": "Thursday", "thur": "Thursday", "thurs": "Thursday", "thursday": "Thursday",
    "fri": "Friday", "friday": "Friday",
    "sat": "Saturday", "saturday": "Saturday",
    "sun": "Sunday", "sunday": "Sunday",
}
_DAY = r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tues|tue|weds|wed|thurs|thur|thu|fri|sat|sun)"
_TIME = r"(\d{1,2}(?::\d{2})?\s*(?:am|pm|a\.m\.|p\.m\.|a|p)?)"
_SEGMENT_SPLIT = re.compile(r"[,;|/\n]+|(?<=[)\sap]m)\s+(?=" + _DAY + r"\b)", re.I)
_RANGE_RE = re.compile(r"\b" + _DAY + r"\s*(?:-|–|to|thru|through)\s*" + _DAY + r"\b", re.I)
_TIME_RANGE_RE = re.compile(_TIME + r"\s*(?:-|–|to)\s*" + _TIME, re.I)


@dataclass(frozen=True, slots=True)
class DayHours:
    """
    `opens`/`closes` are minutes after midnight; both None means closed.
    `by_appointment` marks days that are open only for booked visits.
    """

    text:           str
    opens:          int | None = None
    closes:         int | None = None
    by_appointment: bool = False

    @property
    def closed(self) -> bool:
        return self.opens is None and not self.by_appointment


def _minutes(token: str, *, end: bool, start: int | None = None) -> int | None:
    m = re.match(r"(\d{1,2})(?::(\d{2}))?\s*([ap])?", token.strip().lower().replace(".", ""))
    if not m:
        return None
    hour, minute, meridiem = int(m.group(1)), int(m.group(2) or 0), m.group(3)
    if meridiem == "p" and hour < 12:
        hour += 12
    elif meridiem == "a" and hour == 12:
        hour = 0
    elif meridiem is None:
        # "8-5": a bare closing hour earlier than the opening hour is in the afternoon.
        if end and start is not None and hour * 60 + minute <= start:
            hour += 12
        elif not end and hour < 6:
            hour += 12
    return hour * 60 + minute


def parse_hours(text: str) -> dict[str, DayHours]:
    """
    Parse strings like "Mon-Sat 9am-6pm, Sun closed" or "Saturday: by appointment only".
    Days the text doesn't cover are absent from the result.
    """
    result: dict[str, DayHours] = {}
    for seg in _SEGMENT_SPLIT.split(text or ""):
        seg = (seg or "").strip().rstrip(".")
        if not seg or seg.lower() in _DAY_ALIASES:
            continue
        if re.search(r"\b(daily|every\s*day|all\s*week|7\s*days)\b", seg, re.I):
            days = list(WEEKDAYS)
        elif (rng := _RANGE_RE.search(seg)):
            s = WEEKDAYS.index(_DAY_ALIASES[rng.group(1).lower()])
            e = WEEKDAYS.index(_DAY_ALIASES[rng.group(2).lower()])
            days = [WEEKDAYS[i % 7] for i in range(s, e + 1 if e >= s else e + 8)]
        else:
            days = [_DAY_ALIASES[d.lower()] for d in re.findall(r"\b" + _DAY + r"\b", seg, re.I)]
        if not days:
            continue

        if re.search(r"\bclosed\b", seg, re.I):
            hours = DayHours(text="closed")
        elif (tm := _TIME_RANGE_RE.search(seg)):
            opens = _minutes(tm.group(1), end=False)
            closes = _minutes(tm.group(2), end=True, start=opens)
            hours = DayHours(text=f"{tm.group(1).strip()} to {tm.group(2).strip()}", opens=opens, closes=closes)
        elif re.search(r"appointment", seg, re.I):
            hours = DayHours(text="by appointment only", by_appointment=True)
        else:
            continue
        for day in days:
            result[day] = hours
    return result


def is_open_now(hours: dict[str, DayHours], now: datetime) -> bool | None:
    """
    True/False when today's hours are known, None when they aren't.
    """
    today = hours.get(WEEKDAYS[now.weekday()])
    if today is None:
        return None
    if today.opens is None or today.closes is None:
        return False
    minute = now.hour * 60 + now.minute
    return today.opens <= minute < today.closes


def next_open_day(hours: dict[str, DayHours], now: datetime) -> tuple[str, DayHours] | None:
    """
    The next day after today that is open (or by appointment), labelled "tomorrow" or by weekday name.
    """
    for offset in range(1, 8):
        day = WEEKDAYS[(now + timedelta(days=offset)).weekday()]
        h = hours.get(day)
        if h is not None and not h.closed:
            return ("tomorrow" if offset == 1 else day), h
    return None


def schedule_block(hours_text: str, now: datetime) -> str:
    hours = parse_hours(hours_text)
    if not hours:
        return f"- (could not parse hours; raw hours: {hours_text or 'not listed'})"
    lines = []
    for offset in range(7):
        d = now + timedelta(days=offset)
        day = WEEKDAYS[d.weekday()]
        label = "TODAY" if offset == 0 else "TOMORROW" if offset == 1 else day
        h = hours.get(day)
        if h is None:
            status = "hours not listed - follow the business notes for this day"
        elif h.by_appointment:
            status = "BY APPOINTMENT ONLY - offer a specific booked time; don't tell them to just stop by"
        elif h.closed:
            status = "CLOSED - never offer a time on this day"
        else:
            status = h.text
        lines.append(f"- {label} ({day} {d.strftime('%b %d')}): {status}")
    return "\n".join(lines)


def open_status_line(hours_text: str, now: datetime) -> str:
    hours = parse_hours(hours_text)
    state = is_open_now(hours, now)
    today = hours.get(WEEKDAYS[now.weekday()])
    nxt = next_open_day(hours, now)
    next_phrase = f"{nxt[0]} ({nxt[1].text})" if nxt else "the next day we're open"
    if state is True:
        return (f"OPEN/CLOSED RIGHT NOW: OPEN (today's hours: {today.text}). "
                "If asked whether we're open, say yes.")
    if state is False:
        return (f"OPEN/CLOSED RIGHT NOW: CLOSED (today: {today.text if today else 'closed'}). "
                "Don't volunteer it unless they ask about coming in now/today; when it is relevant, "
                f"turn it into a booking for the next open time: {next_phrase}. "
                "Being closed never limits what you can answer.")
    return ""

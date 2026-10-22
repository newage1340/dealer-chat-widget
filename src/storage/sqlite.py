"""
SQLite persistence for contacts, conversations, appointments, notices and usage.

All timestamps are stored as UTC ISO-8601 strings. A connection is opened per operation,
which keeps the store safe to share across Flask request threads and scheduler jobs.
"""

import json
import sqlite3
import uuid
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core import (
    Address,
    AddressKind,
    Appointment,
    AppointmentStatus,
    Channel,
    Contact,
    ContactId,
    ConversationKey,
    Message,
    Role,
    TenantId,
    UsageRecord,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS contacts (
    id                TEXT PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    first_name        TEXT NOT NULL DEFAULT '',
    last_name         TEXT NOT NULL DEFAULT '',
    email             TEXT NOT NULL DEFAULT '',
    phone             TEXT NOT NULL DEFAULT '',
    terms_accepted_at TEXT,
    attributes        TEXT NOT NULL DEFAULT '{}',
    created_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS contacts_phone ON contacts (tenant_id, phone);

CREATE TABLE IF NOT EXISTS contact_addresses (
    tenant_id  TEXT NOT NULL,
    kind       TEXT NOT NULL,
    value      TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, kind, value)
);

CREATE TABLE IF NOT EXISTS messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id  TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    role       TEXT NOT NULL,
    text       TEXT NOT NULL,
    channel    TEXT NOT NULL,
    session_id TEXT,
    meta       TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_conv ON messages (tenant_id, contact_id, id);

CREATE TABLE IF NOT EXISTS conversation_state (
    tenant_id  TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    state      TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, contact_id)
);

CREATE TABLE IF NOT EXISTS appointments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id    TEXT NOT NULL,
    contact_id   TEXT NOT NULL,
    time_text    TEXT NOT NULL,
    starts_at    TEXT,
    subject      TEXT NOT NULL,
    location     TEXT NOT NULL DEFAULT '',
    details      TEXT NOT NULL DEFAULT '{}',
    status       TEXT NOT NULL,
    sent_notices TEXT NOT NULL DEFAULT '[]',
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS appointments_conv ON appointments (tenant_id, contact_id);

CREATE TABLE IF NOT EXISTS notices (
    tenant_id  TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    kind       TEXT NOT NULL,
    sent_at    TEXT NOT NULL,
    PRIMARY KEY (tenant_id, contact_id, kind)
);

CREATE TABLE IF NOT EXISTS usage (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL,
    service     TEXT NOT NULL,
    operation   TEXT NOT NULL,
    purpose     TEXT NOT NULL,
    units       TEXT NOT NULL,
    cost_usd    REAL,
    latency_ms  REAL,
    tenant_id   TEXT,
    channel     TEXT,
    occurred_at TEXT NOT NULL
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class SqliteStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # --- contacts -------------------------------------------------------------------

    def _contact(self, row: sqlite3.Row, addresses: tuple[Address, ...] = ()) -> Contact:
        return Contact(
            id                = ContactId(row["id"]),
            tenant_id         = TenantId(row["tenant_id"]),
            first_name        = row["first_name"],
            last_name         = row["last_name"],
            email             = row["email"],
            phone             = row["phone"],
            addresses         = addresses,
            terms_accepted_at = _dt(row["terms_accepted_at"]),
            attributes        = json.loads(row["attributes"] or "{}"),
        )

    def get_contact(self, tenant_id: TenantId, contact_id: ContactId) -> Contact | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM contacts WHERE tenant_id=? AND id=?", (tenant_id, contact_id)).fetchone()
            if row is None:
                return None
            addrs = conn.execute(
                "SELECT kind, value FROM contact_addresses WHERE tenant_id=? AND contact_id=?", (tenant_id, contact_id)
            ).fetchall()
        return self._contact(row, tuple(Address(AddressKind(a["kind"]), a["value"]) for a in addrs))

    def find_contact(self, tenant_id: TenantId, address: Address) -> Contact | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT contact_id FROM contact_addresses WHERE tenant_id=? AND kind=? AND value=?",
                (tenant_id, str(address.kind), address.value),
            ).fetchone()
        return self.get_contact(tenant_id, ContactId(row["contact_id"])) if row else None

    def contact_for_address(self, tenant_id: TenantId, address: Address, *, phone: str = "") -> Contact:
        """
        The contact reachable at `address`, created on first contact.
        Phone addresses also seed Contact.phone, since a texting or calling number is reachable.
        """
        existing = self.find_contact(tenant_id, address)
        if existing is not None:
            return existing
        contact_id = ContactId(uuid.uuid4().hex)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO contacts (id, tenant_id, phone, created_at) VALUES (?, ?, ?, ?)",
                (contact_id, tenant_id, phone, _iso(utcnow())),
            )
            conn.execute(
                "INSERT OR IGNORE INTO contact_addresses (tenant_id, kind, value, contact_id) VALUES (?, ?, ?, ?)",
                (tenant_id, str(address.kind), address.value, contact_id),
            )
        contact = self.get_contact(tenant_id, contact_id)
        assert contact is not None
        return contact

    def update_contact(self, contact: Contact, changes: Mapping[str, Any]) -> Contact:
        columns = {"first_name", "last_name", "email", "phone", "terms_accepted_at"}
        sets, args = [], []
        attributes = dict(contact.attributes)
        for key, value in changes.items():
            if key in columns:
                sets.append(f"{key}=?")
                args.append(_iso(value) if isinstance(value, datetime) else (value or ""))
            else:
                attributes[key] = value
        sets.append("attributes=?")
        args.append(json.dumps(attributes))
        with self._conn() as conn:
            conn.execute(f"UPDATE contacts SET {', '.join(sets)} WHERE tenant_id=? AND id=?",
                         (*args, contact.tenant_id, contact.id))
        updated = self.get_contact(contact.tenant_id, contact.id)
        assert updated is not None
        return updated

    def contacts_with_phone(self, tenant_id: TenantId, phone: str) -> list[ContactId]:
        with self._conn() as conn:
            rows = conn.execute("SELECT id FROM contacts WHERE tenant_id=? AND phone=?", (tenant_id, phone)).fetchall()
        return [ContactId(r["id"]) for r in rows]

    def delete_contact(self, key: ConversationKey) -> None:
        """
        Remove a contact and everything tied to their conversation.
        """
        args = (key.tenant_id, key.contact_id)
        with self._conn() as conn:
            for table in ("messages", "conversation_state", "appointments", "notices", "contact_addresses"):
                conn.execute(f"DELETE FROM {table} WHERE tenant_id=? AND contact_id=?", args)
            conn.execute("DELETE FROM contacts WHERE tenant_id=? AND id=?", args)

    # --- messages -------------------------------------------------------------------

    def add_message(self, key: ConversationKey, role: Role, text: str, channel: Channel,
                    *, session_id: str | None = None, meta: Mapping[str, Any] | None = None) -> Message:
        now = utcnow()
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO messages (tenant_id, contact_id, role, text, channel, session_id, meta, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (key.tenant_id, key.contact_id, str(role), text, str(channel), session_id, json.dumps(meta or {}), _iso(now)),
            )
        return Message(key=key, role=role, text=text, channel=channel, created_at=now, id=cur.lastrowid, meta=dict(meta or {}))

    def history(self, key: ConversationKey, limit: int = 40, *, session_id: str | None = None) -> tuple[Message, ...]:
        sql = "SELECT * FROM messages WHERE tenant_id=? AND contact_id=?"
        args: list[Any] = [key.tenant_id, key.contact_id]
        if session_id is not None:
            sql += " AND session_id=?"
            args.append(session_id)
        with self._conn() as conn:
            rows = conn.execute(sql + " ORDER BY id DESC LIMIT ?", (*args, limit)).fetchall()
        return tuple(
            Message(key=key, role=Role(r["role"]), text=r["text"], channel=Channel(r["channel"]),
                    created_at=_dt(r["created_at"]), id=r["id"], meta=json.loads(r["meta"] or "{}"))
            for r in reversed(rows)
        )

    def conversations_since(self, since: datetime) -> list[tuple[ConversationKey, Role, datetime, datetime | None]]:
        """
        (key, role of last message, time of last message, time of last user message)
        for every conversation active since `since`.
        """
        sql = """
            SELECT m.tenant_id, m.contact_id, m.role, m.created_at,
                   (SELECT MAX(u.created_at) FROM messages u
                     WHERE u.tenant_id=m.tenant_id AND u.contact_id=m.contact_id AND u.role='user') AS last_user_at
            FROM messages m
            JOIN (SELECT tenant_id, contact_id, MAX(id) AS max_id FROM messages GROUP BY tenant_id, contact_id) last
              ON last.max_id = m.id
            WHERE m.created_at >= ?
        """
        with self._conn() as conn:
            rows = conn.execute(sql, (_iso(since),)).fetchall()
        return [(ConversationKey(TenantId(r["tenant_id"]), ContactId(r["contact_id"])), Role(r["role"]),
                 _dt(r["created_at"]), _dt(r["last_user_at"])) for r in rows]

    # --- conversation state ---------------------------------------------------------

    def get_state(self, key: ConversationKey) -> dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute("SELECT state FROM conversation_state WHERE tenant_id=? AND contact_id=?",
                               (key.tenant_id, key.contact_id)).fetchone()
        return json.loads(row["state"]) if row else {}

    def update_state(self, key: ConversationKey, updates: Mapping[str, Any], deletes: frozenset[str] = frozenset()) -> dict[str, Any]:
        state = self.get_state(key)
        state.update(updates)
        for k in deletes:
            state.pop(k, None)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO conversation_state (tenant_id, contact_id, state, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (tenant_id, contact_id) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at",
                (key.tenant_id, key.contact_id, json.dumps(state, default=str), _iso(utcnow())),
            )
        return state

    # --- appointments ---------------------------------------------------------------

    def _appointment(self, r: sqlite3.Row) -> Appointment:
        return Appointment(
            id           = r["id"],
            key          = ConversationKey(TenantId(r["tenant_id"]), ContactId(r["contact_id"])),
            time_text    = r["time_text"],
            starts_at    = _dt(r["starts_at"]),
            subject      = r["subject"],
            location     = r["location"],
            details      = json.loads(r["details"] or "{}"),
            status       = AppointmentStatus(r["status"]),
            sent_notices = frozenset(json.loads(r["sent_notices"] or "[]")),
            created_at   = _dt(r["created_at"]),
        )

    def save_appointment(self, appointment: Appointment, *, replace_existing: bool = False) -> Appointment:
        key = appointment.key
        with self._conn() as conn:
            if replace_existing:
                conn.execute("UPDATE appointments SET status=? WHERE tenant_id=? AND contact_id=? AND status=?",
                             (str(AppointmentStatus.CANCELLED), key.tenant_id, key.contact_id, str(AppointmentStatus.CONFIRMED)))
            cur = conn.execute(
                "INSERT INTO appointments (tenant_id, contact_id, time_text, starts_at, subject, location, details, "
                "status, sent_notices, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (key.tenant_id, key.contact_id, appointment.time_text, _iso(appointment.starts_at), appointment.subject,
                 appointment.location, json.dumps(dict(appointment.details)), str(appointment.status),
                 json.dumps(sorted(appointment.sent_notices)), _iso(appointment.created_at)),
            )
        return replace(appointment, id=cur.lastrowid)

    def latest_appointment(self, key: ConversationKey) -> Appointment | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM appointments WHERE tenant_id=? AND contact_id=? AND status=? ORDER BY id DESC LIMIT 1",
                (key.tenant_id, key.contact_id, str(AppointmentStatus.CONFIRMED)),
            ).fetchone()
        return self._appointment(row) if row else None

    def upcoming_appointments(self, until: datetime) -> list[Appointment]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM appointments WHERE status=? AND starts_at IS NOT NULL AND starts_at >= ? AND starts_at <= ?",
                (str(AppointmentStatus.CONFIRMED), _iso(utcnow()), _iso(until)),
            ).fetchall()
        return [self._appointment(r) for r in rows]

    def mark_appointment_notice(self, appointment_id: int, notice: str) -> None:
        with self._conn() as conn:
            row = conn.execute("SELECT sent_notices FROM appointments WHERE id=?", (appointment_id,)).fetchone()
            if row is None:
                return
            notices = set(json.loads(row["sent_notices"] or "[]")) | {notice}
            conn.execute("UPDATE appointments SET sent_notices=? WHERE id=?", (json.dumps(sorted(notices)), appointment_id))

    # --- conversation-level notices (follow-ups etc.) -------------------------------

    def notice_sent(self, key: ConversationKey, kind: str) -> bool:
        with self._conn() as conn:
            row = conn.execute("SELECT 1 FROM notices WHERE tenant_id=? AND contact_id=? AND kind=?",
                               (key.tenant_id, key.contact_id, kind)).fetchone()
        return row is not None

    def mark_notice(self, key: ConversationKey, kind: str) -> None:
        with self._conn() as conn:
            conn.execute("INSERT OR REPLACE INTO notices (tenant_id, contact_id, kind, sent_at) VALUES (?, ?, ?, ?)",
                         (key.tenant_id, key.contact_id, kind, _iso(utcnow())))

    def clear_notice(self, key: ConversationKey, kind: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM notices WHERE tenant_id=? AND contact_id=? AND kind=?",
                         (key.tenant_id, key.contact_id, kind))

    # --- usage (UsageSink) ----------------------------------------------------------

    def record(self, record: UsageRecord) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO usage (provider, service, operation, purpose, units, cost_usd, latency_ms, tenant_id, channel, occurred_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (record.provider, str(record.service), record.operation, record.purpose,
                 json.dumps({str(k): v for k, v in record.units.items()}), record.cost_usd, record.latency_ms,
                 record.tenant_id, str(record.channel) if record.channel else None, _iso(record.occurred_at)),
            )

    def usage_summary(self, since: datetime) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT provider, service, purpose, COUNT(*) AS calls, SUM(cost_usd) AS cost_usd, AVG(latency_ms) AS avg_latency_ms "
                "FROM usage WHERE occurred_at >= ? GROUP BY provider, service, purpose ORDER BY calls DESC",
                (_iso(since),),
            ).fetchall()
        return [dict(r) for r in rows]

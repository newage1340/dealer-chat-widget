/**
 * Table rows as core types, and the SQL fragments that go with them.
 */

import {
    AppointmentId,
    AppointmentStatus,
    assertNever,
    BlockSource,
    CallHandling,
    CallState,
    CallVendor,
    Channel,
    ContactId,
    ConversationId,
    JobId,
    Language,
    MessageId,
    PhoneNumber,
    Role,
    SessionId,
    TenantId,
    type Appointment,
    type Block,
    type Classification,
    type Contact,
    type ContactChanges,
    type Conversation,
    type Message,
    type Session,
} from "../../core/index.ts";
import type { Row } from "../db.ts";
import type { ClaimedJob, SessionKey } from "../ports.ts";
import {
    AddressJson,
    CallStateJson,
    ClassificationJson,
    date,
    InstantsJson,
    ItemIdsJson,
    JobBodyJson,
    json,
    jsonColumn,
    maybeDate,
    member,
    NoticesJson,
    SessionMeterJson,
    str,
    StringsJson,
    TextRecordJson,
} from "./columns.ts";

/**
 * A contact row plus its addresses, as one `addresses` JSON column.
 */
export const CONTACT_COLUMNS = `c.*, (
    select coalesce(json_agg(json_build_object('kind', a.kind, 'value', a.value)), '[]'::json)
    from contact_addresses a where a.contact_id = c.id
) as addresses`;

export function toContact(r: Row): Contact {
    const block: Block | null = r["block_source"] === null || r["block_source"] === undefined
        ? null
        : {
            until:     maybeDate(r["block_until"]),
            source:    member(BlockSource, r["block_source"]),
            reason:    str(r["block_reason"]),
            createdAt: date(r["block_created_at"]),
        };
    return {
        id:                ContactId(str(r["id"])),
        tenantId:          TenantId(str(r["tenant_id"])),
        firstName:         str(r["first_name"]),
        lastName:          str(r["last_name"]),
        email:             str(r["email"]),
        phone:             PhoneNumber.parse(str(r["phone"])),
        addresses:         jsonColumn(AddressJson.array(), r["addresses"] ?? [], "contact addresses"),
        optedOut:          Boolean(r["opted_out"]),
        termsAcceptedAt:   maybeDate(r["terms_accepted_at"]),
        preferredLanguage: r["preferred_language"] === null || r["preferred_language"] === undefined
            ? null
            : member(Language, r["preferred_language"]),
        block,
        abuseStrikes:      jsonColumn(InstantsJson, r["abuse_strikes"] ?? [], "abuse strikes"),
        attributes:        jsonColumn(TextRecordJson, r["attributes"] ?? {}, "contact attributes"),
    };
}

export function toConversation(r: Row): Conversation {
    return {
        id:            ConversationId(str(r["id"])),
        tenantId:      TenantId(str(r["tenant_id"])),
        contactId:     ContactId(str(r["contact_id"])),
        createdAt:     date(r["created_at"]),
        lastMessageAt: maybeDate(r["last_message_at"]),
    };
}

export function toSession(r: Row): Session {
    const base = {
        id:             SessionId(str(r["id"])),
        conversationId: ConversationId(str(r["conversation_id"])),
        tenantId:       TenantId(str(r["tenant_id"])),
        startedAt:      date(r["started_at"]),
        endedAt:        maybeDate(r["ended_at"]),
        sentUrls:       jsonColumn(StringsJson, r["sent_urls"] ?? [], "session sent_urls"),
        language:       member(Language, r["language"]),
        meter:          jsonColumn(SessionMeterJson, r["meter"], "session meter"),
    };
    const channel = member(Channel, r["channel"]);
    switch (channel) {
        case "web":
            return { ...base, channel, externalId: str(r["external_id"]) };
        case "sms":
            return { ...base, channel, externalId: null };
        case "voice":
            return {
                ...base,
                channel,
                externalId: str(r["external_id"]),
                vendor:     member(CallVendor, r["vendor"]),
                handling:   r["handling"] === null || r["handling"] === undefined
                    ? CallHandling.Assistant
                    : member(CallHandling, r["handling"]),
                call:       r["call"] === null || r["call"] === undefined
                    ? CallState.start()
                    : jsonColumn(CallStateJson, r["call"], "session call"),
            };
        default:
            return assertNever(channel);
    }
}

export function toMessage(r: Row): Message {
    return {
        id:             MessageId(str(r["id"])),
        conversationId: ConversationId(str(r["conversation_id"])),
        sessionId:      SessionId(str(r["session_id"])),
        role:           member(Role, r["role"]),
        text:           str(r["text"]),
        channel:        member(Channel, r["channel"]),
        createdAt:      date(r["created_at"]),
    };
}

export function toAppointment(r: Row): Appointment {
    return {
        id:             AppointmentId(str(r["id"])),
        tenantId:       TenantId(str(r["tenant_id"])),
        contactId:      ContactId(str(r["contact_id"])),
        conversationId: ConversationId(str(r["conversation_id"])),
        timeText:       str(r["time_text"]),
        startsAt:       maybeDate(r["starts_at"]),
        subject:        str(r["subject"]),
        location:       str(r["location"]),
        details:        jsonColumn(TextRecordJson, r["details"] ?? {}, "appointment details"),
        itemIds:        jsonColumn(ItemIdsJson, r["item_ids"] ?? [], "appointment item_ids"),
        status:         member(AppointmentStatus, r["status"]),
        sentNotices:    jsonColumn(NoticesJson, r["sent_notices"] ?? [], "appointment sent_notices"),
        createdAt:      date(r["created_at"]),
    };
}

export function toClassification(r: Row): Classification {
    return jsonColumn(ClassificationJson, r["data"], "classification");
}

export function toJob(r: Row): ClaimedJob {
    return {
        ...jsonColumn(JobBodyJson, r["body"], "job body"),
        id:        JobId(str(r["id"])),
        attempts:  Number(r["attempts"]),
        tenantId:  TenantId(str(r["tenant_id"])),
        dedupeKey: str(r["dedupe_key"]),
        runAt:     maybeDate(r["run_at"]),
    };
}

/**
 * Column assignments for a contact update, as SQL fragments and parameters.
 */
export function contactAssignments(changes: ContactChanges, firstParam: number): { sets: string[]; params: unknown[] } {
    const sets: string[] = [];
    const params: unknown[] = [];
    const set = (column: string, value: unknown, cast = "") => {
        params.push(value);
        sets.push(`${column} = $${firstParam + params.length - 1}${cast}`);
    };
    if (changes.firstName !== undefined) set("first_name", changes.firstName);
    if (changes.lastName !== undefined) set("last_name", changes.lastName);
    if (changes.email !== undefined) set("email", changes.email);
    if (changes.phone !== undefined) set("phone", changes.phone);
    if (changes.optedOut !== undefined) set("opted_out", changes.optedOut);
    if (changes.termsAcceptedAt !== undefined) set("terms_accepted_at", changes.termsAcceptedAt);
    if (changes.preferredLanguage !== undefined) set("preferred_language", changes.preferredLanguage);
    if (changes.abuseStrikes !== undefined) set("abuse_strikes", json(changes.abuseStrikes), "::jsonb");
    if (changes.attributes !== undefined) set("attributes", json(changes.attributes), "::jsonb");
    if (changes.block !== undefined) {
        set("block_until", changes.block?.until ?? null);
        set("block_source", changes.block?.source ?? null);
        set("block_reason", changes.block?.reason ?? null);
        set("block_created_at", changes.block?.createdAt ?? null);
    }
    return { sets, params };
}

export function sessionKeyExternal(key: SessionKey): string | null {
    return key.channel === "sms"
        ? null
        : key.externalId;
}

/**
 * The exact prompt a turn would get, for a sample customer, so staff can see what the assistant is told.
 */

import {
    Address,
    assertNever,
    ContactId,
    ConversationId,
    LocalTime,
    PhoneNumber,
    SessionId,
    SessionMeter,
    type Channel,
    type Contact,
    type Conversation,
    type Session,
    type Tenant,
} from "../core/index.ts";
import type { Services } from "../pipeline/services.ts";
import type { TurnContext } from "../turn/index.ts";

/**
 * The customer message a prompt preview uses when none is given. Part of the prompt (the matching
 * catalog details) depends on what the customer said, so a preview always needs one.
 */
export const SAMPLE_MESSAGE = "Hi, I have a question.";

const SAMPLE_PHONE = PhoneNumber.parse("+13175550199");

/**
 * A new session on `channel`, as a first message would open it.
 */
function previewSession(channel: Channel, tenant: Tenant, conversation: Conversation, now: Date): Session {
    const base = {
        id:             SessionId("preview"),
        conversationId: conversation.id,
        tenantId:       tenant.id,
        startedAt:      now,
        endedAt:        null,
        sentUrls:       [],
        language:       tenant.defaultLanguage,
        meter:          SessionMeter.start(now),
    };
    switch (channel) {
        case "web":
            return { ...base, channel, externalId: "preview" };
        case "sms":
            return { ...base, channel, externalId: null };
        case "voice":
            return { ...base, channel, externalId: "preview", vendor: "twilio", handling: "assistant", call: { status: "listening", turns: 0, silentTurns: 0 } };
        default:
            return assertNever(channel);
    }
}

/**
 * The prompt a turn would get for a sample customer (Alex, with a phone on file), as text.
 */
export async function previewPrompt(services: Services, tenant: Tenant, channel: Channel, message: string): Promise<string> {
    const now = services.clock();
    const sender = channel === "web" || SAMPLE_PHONE === null
        ? Address.webSession("preview")
        : { kind: "phone" as const, value: SAMPLE_PHONE };
    const contact: Contact = {
        id:                ContactId("preview"),
        tenantId:          tenant.id,
        firstName:         "Alex",
        lastName:          "",
        email:             "",
        phone:             SAMPLE_PHONE,
        addresses:         [sender],
        optedOut:          false,
        termsAcceptedAt:   null,
        preferredLanguage: null,
        block:             null,
        abuseStrikes:      [],
        attributes:        {},
    };
    const conversation: Conversation = {
        id:            ConversationId("preview"),
        tenantId:      tenant.id,
        contactId:     contact.id,
        createdAt:     now,
        lastMessageAt: null,
    };
    const ctx: TurnContext = {
        inbound:       {
            channel,
            sender,
            text:              message,
            receivedAt:        now,
            dedupeKey:         "preview",
            externalSessionId: null,
            confidence:        null,
            language:          null,
            forwardedFrom:     null,
        },
        tenant,
        pack:          services.registry.get(tenant.domain),
        catalog:       await services.store.tenants.catalog(tenant.id),
        contact,
        conversation,
        session:       previewSession(channel, tenant, conversation, now),
        history:       [],
        appointment:   null,
        costThisMonth: 0,
        now,
        local:         LocalTime.of(now, tenant.timezone),
    };
    const messages = services.composer.turn(ctx, { kind: "ok" });
    return messages.map(m => `##### ${m.role}\n${"text" in m ? m.text : m.output}`).join("\n\n");
}

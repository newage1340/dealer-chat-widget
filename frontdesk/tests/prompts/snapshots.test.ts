/**
 * The exact prompts the model gets, pinned as files under __snapshots__/.
 *
 * These are a safety net for refactoring the composer: any change to what the model reads shows up
 * as a file diff. When a prompt change is intended, review the diff and run `pnpm test -u` to accept it.
 * Every pack is covered on every channel (with its demo tenant and catalog where one exists),
 * plus the per-turn situations that change the prompt (appointments, wind-downs, crisis, privacy, language).
 */

import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { beforeAll, describe, expect, it } from "vitest";

import { fetchRows, mapColumns } from "../../src/catalogs/index.ts";
import { ROOT } from "../../src/config.ts";
import {
    AppointmentId,
    Channel,
    ContactId,
    ConversationId,
    LocalTime,
    MessageId,
    PhoneNumber,
    SessionId,
    SessionMeter,
    type Appointment,
    type BudgetVerdict,
    type CatalogItem,
    type Contact,
    type Conversation,
    type Language,
    type Message,
    type PromptVariant,
    type Session,
    type Tenant,
} from "../../src/core/index.ts";
import { Catalog, DomainRegistry, FollowupKind, type DomainPack } from "../../src/domains/index.ts";
import { PromptComposer } from "../../src/prompts/composer.ts";
import type { ChatMessage } from "../../src/providers/index.ts";
import { parseTenant } from "../../src/tenants/schema.ts";
import type { TurnContext } from "../../src/turn/index.ts";
import { tenant as fixtureTenant } from "../support/fixtures.ts";

const NOW = new Date("2026-10-21T15:00:00Z");
const CALLER = PhoneNumber.parse("+13175550111");
const SNAPSHOTS = new URL("./__snapshots__/", import.meta.url);

let registry: DomainRegistry;
let composer: PromptComposer;
const tenants = new Map<string, Tenant>();
const catalogs = new Map<string, CatalogItem[]>();

/**
 * The pack's demo tenant (tenants/demo/<pack>.json), or a bare one for packs without a demo.
 */
async function demoTenant(pack: DomainPack): Promise<Tenant> {
    const text = await readFile(join(ROOT, "tenants", "demo", `${pack.id}.json`), "utf8").catch(() => null);
    if (text === null) {
        return fixtureTenant({ id: `demo-${pack.id}`, slug: `demo-${pack.id}`, domain: pack.id });
    }
    const parsed = parseTenant(JSON.parse(text));
    if (!parsed.ok) {
        throw new Error(`tenants/demo/${pack.id}.json: ${JSON.stringify(parsed.error)}`);
    }
    return parsed.value;
}

/**
 * The tenant's catalog from its bundled dataset (no network), normalized the way a sync would.
 */
async function demoCatalog(tenant: Tenant, pack: DomainPack): Promise<CatalogItem[]> {
    const source = tenant.settings.catalog.source;
    if (source.kind !== "dataset") {
        return [];
    }
    const rows = await fetchRows(source);
    if (rows === null || !rows.ok) {
        throw new Error(`dataset ${source.name}: ${rows === null ? "nothing to fetch" : rows.error}`);
    }
    return mapColumns(rows.value, tenant.settings.catalog.fieldMap)
        .map((row, i) => Catalog.normalize(row, pack.catalog, tenant.id, i, NOW))
        .filter(item => Object.keys(item.fields).length > 0);
}

type Situation = {
    readonly channel:      Channel;
    readonly message?:     string;
    readonly language?:    Language;
    readonly appointment?: Appointment;
    readonly verdict?:     BudgetVerdict;
    readonly tenant?:      (tenant: Tenant) => Tenant;
    readonly catalog?:     (items: readonly CatalogItem[]) => readonly CatalogItem[];
};

function session(channel: Channel, tenant: Tenant, conversation: Conversation, language: Language): Session {
    const base = {
        id:             SessionId("snapshot"),
        conversationId: conversation.id,
        tenantId:       tenant.id,
        startedAt:      NOW,
        endedAt:        null,
        sentUrls:       [],
        language,
        meter:          SessionMeter.start(NOW),
    };
    switch (channel) {
        case "web":
            return { ...base, channel, externalId: "snapshot" };
        case "sms":
            return { ...base, channel, externalId: null };
        case "voice":
            return { ...base, channel, externalId: "snapshot", vendor: "twilio", handling: "assistant", call: { status: "listening", turns: 1, silentTurns: 0 } };
    }
}

/**
 * A mid-conversation turn: a known customer (first name and phone on file) on their second message.
 */
function context(pack: DomainPack, situation: Situation): TurnContext {
    const base = tenants.get(pack.id)!;
    const tenant = situation.tenant?.(base) ?? base;
    const language = situation.language ?? tenant.defaultLanguage;
    const sender = situation.channel === "web" || CALLER === null
        ? { kind: "web_session" as const, value: "snapshot" }
        : { kind: "phone" as const, value: CALLER };
    const contact: Contact = {
        id:                ContactId("snapshot"),
        tenantId:          tenant.id,
        firstName:         "Maria",
        lastName:          "",
        email:             "",
        phone:             CALLER,
        addresses:         [sender],
        optedOut:          false,
        termsAcceptedAt:   NOW,
        preferredLanguage: null,
        block:             null,
        abuseStrikes:      [],
        attributes:        {},
    };
    const conversation: Conversation = {
        id:            ConversationId("snapshot"),
        tenantId:      tenant.id,
        contactId:     contact.id,
        createdAt:     NOW,
        lastMessageAt: NOW,
    };
    const said = (role: Message["role"], text: string, i: number): Message => ({
        id:             MessageId(`m${i}`),
        conversationId: conversation.id,
        sessionId:      SessionId("snapshot"),
        role,
        text,
        channel:        situation.channel,
        createdAt:      NOW,
    });
    return {
        inbound:       {
            channel:           situation.channel,
            sender,
            text:              situation.message ?? pack.widget.topics[0]?.message ?? "Hi, I have a question.",
            receivedAt:        NOW,
            dedupeKey:         "snapshot",
            externalSessionId: null,
            confidence:        null,
            language:          null,
            forwardedFrom:     null,
        },
        tenant,
        pack,
        catalog:       situation.catalog?.(catalogs.get(pack.id) ?? []) ?? catalogs.get(pack.id) ?? [],
        contact,
        conversation,
        session:       session(situation.channel, tenant, conversation, language),
        history:       [
            said("user", "Hi there", 1),
            said("assistant", "Hi Maria! How can I help you today?", 2),
        ],
        appointment:   situation.appointment ?? null,
        costThisMonth: 0,
        now:           NOW,
        local:         LocalTime.of(NOW, tenant.timezone),
    };
}

function render(messages: readonly ChatMessage[]): string {
    return messages.map(m => {
        const head = m.role === "system"
            ? `##### system (${m.cacheable ? "cached" : "per turn"})`
            : `##### ${m.role}`;
        return `${head}\n${"text" in m ? m.text : m.output}`;
    }).join("\n\n") + "\n";
}

async function expectSnapshot(name: string, text: string): Promise<void> {
    await expect(text).toMatchFileSnapshot(new URL(name, SNAPSHOTS).pathname);
}

function appointment(tenant: Tenant, startsAt: Date, timeText: string): Appointment {
    return {
        id:             AppointmentId("snapshot"),
        tenantId:       tenant.id,
        contactId:      ContactId("snapshot"),
        conversationId: ConversationId("snapshot"),
        timeText,
        startsAt,
        subject:        "a leaking kitchen faucet",
        location:       "",
        details:        {},
        itemIds:        [],
        status:         "confirmed",
        sentNotices:    [],
        createdAt:      NOW,
    };
}

beforeAll(async () => {
    registry = await DomainRegistry.fromDirectory();
    composer = await PromptComposer.load();
    for (const id of registry.ids()) {
        const pack = registry.get(id);
        const tenant = await demoTenant(pack);
        tenants.set(id, tenant);
        catalogs.set(id, await demoCatalog(tenant, pack));
    }
});

describe("turn prompts", () => {
    it("are unchanged for every pack on every channel", async () => {
        for (const id of registry.ids()) {
            const pack = registry.get(id);
            for (const channel of Object.values(Channel)) {
                await expectSnapshot(`turn/${id}.${channel}.txt`, render(composer.turn(context(pack, { channel }), { kind: "ok" })));
            }
        }
    });

    it("are unchanged for every prompt variant that brings its own templates", async () => {
        const pack = registry.get(registry.ids()[0]!);
        const own = composer.variants.filter((v: PromptVariant) => Object.keys(v.templates).length > 0);
        for (const variant of own) {
            for (const channel of Object.values(Channel)) {
                await expectSnapshot(`variant/${variant.id}.${channel}.txt`, render(composer.turn(context(pack, { channel }), { kind: "ok" }, variant)));
            }
        }
    });

    it("are unchanged in the situations that change the per-turn part", async () => {
        const pack = registry.get(registry.ids().includes("plumbing" as DomainPack["id"]) ? "plumbing" as DomainPack["id"] : registry.ids()[0]!);
        const tenant = tenants.get(pack.id)!;
        const situations: Record<string, readonly [Situation, BudgetVerdict]> = {
            "upcoming-appointment": [{ channel: "sms", appointment: appointment(tenant, new Date("2026-10-23T14:00:00Z"), "Friday at 10 AM") }, { kind: "ok" }],
            "past-appointment":     [{ channel: "sms", appointment: appointment(tenant, new Date("2026-10-19T14:00:00Z"), "Monday at 10 AM") }, { kind: "ok" }],
            "closing-duration":     [{ channel: "sms" }, { kind: "closing", reason: "duration", turnsLeft: 3 }],
            "closing-last-voice":   [{ channel: "voice" }, { kind: "closing", reason: "duration", turnsLeft: 1 }],
            "closing-abusive":      [{ channel: "web" }, { kind: "closing", reason: "abusive", turnsLeft: 2 }],
            "crisis":               [{
                channel: "sms",
                message: "I don't want to be here anymore, I want to end my life",
                tenant:  t => ({ ...t, settings: { ...t.settings, privacy: { ...t.settings.privacy, crisisSupport: true } } }),
            }, { kind: "ok" }],
            "sensitive":            [{
                channel: "web",
                tenant:  t => ({ ...t, settings: { ...t.settings, privacy: { ...t.settings.privacy, sensitive: true } } }),
            }, { kind: "ok" }],
            "spanish-session":      [{ channel: "voice", language: "es", message: "Hola, tengo una fuga debajo del fregadero" }, { kind: "ok" }],
            "casual-honest":        [{
                channel: "sms",
                tenant:  t => ({ ...t, settings: { ...t.settings, style: { professionalism: "casual", honestAboutAi: true } } }),
            }, { kind: "ok" }],
            "no-catalog-match":     [{ channel: "sms", message: "Do you sell motorcycles?" }, { kind: "ok" }],
            // Past FULL_CATALOG_LIMIT the list is left out and the model searches instead.
            "large-catalog":        [{
                channel: "sms",
                catalog: items => Array.from({ length: 6 }, () => items).flat(),
            }, { kind: "ok" }],
        };
        for (const [name, [situation, verdict]] of Object.entries(situations)) {
            await expectSnapshot(`situation/${name}.txt`, render(composer.turn(context(pack, situation), verdict)));
        }
    });
});

describe("classification prompts", () => {
    it("are unchanged for every pack", async () => {
        const transcript = "Customer: Hi, my kitchen faucet is leaking\nAssistant: I can help with that. When works for a visit?";
        for (const id of registry.ids()) {
            await expectSnapshot(`classify/${id}.txt`, render(composer.classify(tenants.get(id)!, registry.get(id), transcript)));
        }
    });
});

describe("follow-up texts", () => {
    it("are unchanged for every pack, kind and a few languages", async () => {
        const lines: string[] = [];
        for (const id of registry.ids()) {
            const tenant = tenants.get(id)!;
            const pack = registry.get(id);
            for (const kind of Object.values(FollowupKind)) {
                for (const language of ["en", "es", "vi"] as const) {
                    const text = composer.followup(tenant, pack, kind, language, null, { visit_time: "Friday at 10 AM", subject: "a repair" })
                        ?? "(none)";
                    lines.push(`${id} ${kind} ${language}: ${text}`);
                }
            }
        }
        await expectSnapshot("followups.txt", lines.join("\n") + "\n");
    });
});

/**
 * The tenant file format and the stored tenant stay in step.
 *
 * `toTenant` (file -> tenant) and `fromTenant` (tenant -> file, for the settings pages) are written
 * separately, so a setting added to one and forgotten in the other would silently reset on every save.
 * `Tenant.upgrade` fills in settings added after a tenant was stored; its behavior is pinned here
 * so it can move (into the schema) without changing.
 */

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { ROOT } from "../../src/config.ts";
import { DEFAULT_CATALOG_SETTINGS, DEFAULT_PRIVACY, Language, NO_BILLING, Tenant } from "../../src/core/index.ts";
import { fromTenant, parseTenant } from "../../src/tenants/schema.ts";
import { plumber } from "../support/fixtures.ts";

function roundTrip(tenant: Tenant): Tenant {
    const parsed = parseTenant(fromTenant(tenant));
    if (!parsed.ok) {
        throw new Error(JSON.stringify(parsed.error));
    }
    return parsed.value;
}

/**
 * A tenant with every setting moved off its default.
 */
const FULL = plumber({
    phone:           "317-555-0299",
    address:         "1 Main St",
    website:         "https://circlecity.example",
    agentName:       "Rita",
    voiceAgentName:  "Rae",
    defaultLanguage: "es",
    staffLanguage:   "vi",
    branding:        { color: "#123456", logoUrl: "https://circlecity.example/logo.png" },
    profile:         { service_area: "Indianapolis", emergency: "yes" },
    options:         { fee_phrase: "a $79 trip charge", free_estimates: true, radius: 25 },
    schedule:        [{ day: 1, startMinute: 480, endMinute: 1020 }, { day: 6, startMinute: 540, endMinute: 780 }],
    plan:            { extraLines: 2, smsNotifications: true },
    settings:        {
        skipProfileGate:     true,
        transferTo:          "317-555-0777",
        coldFollowupMinutes: 45,
        textAlwaysOn:        false,
        links:               [{ label: "Coupons", url: "https://circlecity.example/c", trigger: "coupon" }],
        callFallbacks:       { offSchedule: "forward", lineBusy: "reject" },
        notifications:       { transcript: ["email", "sms"], booking: ["sms"], handoff: ["email"], budget_alert: ["email"] },
        maxSessionMinutes:   20,
        recordingDisclosure: true,
        recordingNotice:     "We record calls.",
        languages:           ["en", "es", "vi"],
        privacy:             { sensitive: true, crisisSupport: true, redact: ["card"] },
        style:               { professionalism: "formal", honestAboutAi: true },
        calls:               { silenceSeconds: 7, stillTherePrompts: 2, hiddenCallerId: "voicemail" },
        catalog:             { source: { kind: "json_url", url: "https://x.example/items.json", itemsPath: "data.items", refreshMinutes: 30 }, fieldMap: { Title: "name" } },
    },
});

describe("tenant file round trip", () => {
    it("keeps every setting of a fully configured tenant", () => {
        // Guard against the fixture quietly falling back to defaults (which would round-trip trivially).
        expect(FULL).toMatchObject({
            schedule: { kind: "windows" },
            plan:     { subscription: { extraLines: 2, smsNotifications: true } },
            settings: { calls: { silenceSeconds: 7 }, catalog: { source: { kind: "json_url" } }, recordingDisclosure: { enabled: true } },
        });
        expect(roundTrip(FULL)).toEqual(FULL);
    });

    it("keeps every demo tenant", async () => {
        const dir = join(ROOT, "tenants", "demo");
        for (const file of await readdir(dir)) {
            const parsed = parseTenant(JSON.parse(await readFile(join(dir, file), "utf8")));
            expect(parsed.ok, file).toBe(true);
            if (parsed.ok) {
                expect(roundTrip(parsed.value), file).toEqual(parsed.value);
            }
        }
    });
});

describe("Tenant.upgrade", () => {
    it("leaves a current tenant unchanged", () => {
        expect(Tenant.upgrade(FULL)).toEqual(FULL);
    });

    it("fills in settings a tenant stored before they existed", () => {
        const { languages: _l, privacy: _p, style: _s, catalog: _c, calls: _calls, ...older } = FULL.settings;
        const { billing: _b, ...rest } = FULL;
        const upgraded = Tenant.upgrade({ ...rest, settings: older } as unknown as Tenant);
        expect(upgraded.billing).toEqual(NO_BILLING);
        expect(upgraded.settings.languages).toEqual(Object.values(Language));
        expect(upgraded.settings.privacy).toEqual(DEFAULT_PRIVACY);
        expect(upgraded.settings.style).toEqual({ professionalism: null, honestAboutAi: null });
        expect(upgraded.settings.catalog).toEqual(DEFAULT_CATALOG_SETTINGS);
        expect(upgraded.settings.calls).toEqual({ silenceSeconds: 5, stillTherePrompts: 1, hiddenCallerId: "assistant" });
    });

    it("clamps stored values into the current platform bounds", () => {
        const stored: Tenant = {
            ...FULL,
            settings: {
                ...FULL.settings,
                calls:  { ...FULL.settings.calls, silenceSeconds: 60, stillTherePrompts: -3 },
                limits: { ...FULL.settings.limits, maxSessionSeconds: 3 * 3600 },
            },
        };
        const upgraded = Tenant.upgrade(stored);
        expect(upgraded.settings.calls).toMatchObject({ silenceSeconds: 8, stillTherePrompts: 0 });
        expect(upgraded.settings.limits.maxSessionSeconds).toBe(45 * 60);
    });
});

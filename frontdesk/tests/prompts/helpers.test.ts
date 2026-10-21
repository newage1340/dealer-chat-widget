import { describe, expect, it } from "vitest";

import { DomainId, LocalTime, TenantId } from "../../src/core/index.ts";
import { Catalog, DomainRegistry } from "../../src/domains/index.ts";
import { Hours } from "../../src/prompts/hours.ts";
import { Speech } from "../../src/prompts/speech.ts";

const TZ = "America/Indiana/Indianapolis";
// Wednesday 2026-10-21 11:00 local
const WED_11 = LocalTime.of(new Date("2026-10-21T15:00:00Z"), TZ);

describe("LocalTime", () => {
    it("reads wall-clock time and formats it", () => {
        expect(WED_11).toMatchObject({ year: 2026, month: 10, day: 21, weekday: 3, hour: 11, minute: 0 });
        expect(LocalTime.long(WED_11)).toBe("Wednesday, October 21, 2026 at 11:00 AM");
    });

    it("parses local ISO times in the tenant's zone, across daylight saving", () => {
        expect(LocalTime.parseLocalIso("2026-10-24T14:00:00", TZ)?.toISOString()).toBe("2026-10-24T18:00:00.000Z");
        expect(LocalTime.parseLocalIso("2026-12-24T14:00", TZ)?.toISOString()).toBe("2026-12-24T19:00:00.000Z");
        expect(LocalTime.parseLocalIso("Saturday at 2", TZ)).toBeNull();
    });
});

describe("Hours", () => {
    const text = "Mon-Fri 8-5, Sat 9am-1pm, Sun closed";

    it("parses ranges, bare afternoon hours and closed days", () => {
        const h = Hours.parse(text);
        expect(h.Monday).toMatchObject({ opens: 8 * 60, closes: 17 * 60 });
        expect(h.Saturday).toMatchObject({ opens: 9 * 60, closes: 13 * 60 });
        expect(h.Sunday && Hours.isClosed(h.Sunday)).toBe(true);
        expect(Hours.parse("Saturday: by appointment only").Saturday?.byAppointment).toBe(true);
    });

    it("knows whether it is open and what comes next", () => {
        const h = Hours.parse(text);
        expect(Hours.isOpenNow(h, WED_11)).toBe(true);
        const saturdayEvening = LocalTime.of(new Date("2026-10-24T22:00:00Z"), TZ);
        expect(Hours.isOpenNow(h, saturdayEvening)).toBe(false);
        expect(Hours.nextOpenDay(h, saturdayEvening)?.label).toBe("Monday");
    });

    it("writes a seven-day schedule starting today", () => {
        const block = Hours.scheduleBlock(text, WED_11).split("\n");
        expect(block[0]).toBe("- TODAY (Wednesday Oct 21): 8 to 5");
        expect(block[4]).toBe("- Sunday (Sunday Oct 25): CLOSED - never offer a time on this day");
    });
});

describe("Catalog", async () => {
    const automotive = (await DomainRegistry.fromDirectory()).get(DomainId("automotive"));
    const spec = automotive.catalog;
    const now = new Date();
    const items = [
        { Year: "2017", Make: "Buick", Model: "Encore", Trim: "Preferred", Price: "12995", Mileage: "64000", Description: "Compact SUV, one owner", Stock: "A1" },
        { Year: "2019", Make: "Ford", Model: "F-150", Trim: "XLT", Price: "", Mileage: "88000", Description: "Pickup truck", Stock: "B2" },
    ].map((row, i) => Catalog.normalize(row, spec, TenantId("t"), i, now));

    it("normalizes rows through aliases and formats values", () => {
        expect(Catalog.title(items[0]!, spec)).toBe("2017 Buick Encore Preferred");
        expect(Catalog.summary(items[0]!, spec)).toContain("$12,995");
    });

    it("matches by title and searchable fields, folding plurals", () => {
        expect(Catalog.match(items, "any SUVs?", spec).map(i => Catalog.title(i, spec))).toEqual(["2017 Buick Encore Preferred"]);
        expect(Catalog.match(items, "the ford truck", spec)[0]?.id).toBe(items[1]!.id);
    });

    it("anchors on the item a message names", () => {
        expect(Catalog.anchor(["tell me about the 2017 Buick Encore"], items, spec)?.id).toBe(items[0]!.id);
        expect(Catalog.anchor(["what do you have"], items, spec)).toBeNull();
    });
});

describe("Speech", () => {
    it("spells years, numbers and phone digits", () => {
        expect(Speech.spellYear(2017)).toBe("twenty seventeen");
        expect(Speech.spellYear(2005)).toBe("two thousand five");
        expect(Speech.speakBigNumbers("only $7,500 today")).toBe("only seven thousand five hundred dollars today");
        expect(Speech.spokenDigits("+13175550100")).toBe("3 1 7, 5 5 5, 0 1 0 0");
    });
});

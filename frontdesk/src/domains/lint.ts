/**
 * Checks on a pack that has already parsed, for mistakes the schema can't see.
 * They are warnings: the pack still loads, but something in it probably won't work as written.
 */

import type { DomainPack } from "./pack.ts";

/**
 * `toolNames` are the tools the model can call; passed in so packs don't depend on the tools module.
 */
export function lintPack(pack: DomainPack, toolNames: ReadonlySet<string>): string[] {
    const warnings: string[] = [];
    const texts: [string, string][] = [
        ["prompts.chat", pack.prompts.chat],
        ["prompts.voice", pack.prompts.voice],
        ["domain.goal", pack.goal],
        ...pack.booking.intake.map((f, i): [string, string] => [`booking.intake.${i}.ask`, f.ask]),
        ...pack.prompts.toggles.flatMap((t, i): [string, string][] => [
            [`prompts.toggles.${i}`, [t.chat, t.voice, t.elseChat, t.elseVoice].join("\n")],
        ]),
    ];
    for (const [path, text] of texts) {
        for (const [, name] of text.matchAll(/`([a-z]+_[a-z_]+)`/g)) {
            if (!toolNames.has(name!)) {
                warnings.push(`${path} mentions \`${name}\`, but there is no tool by that name`);
            }
        }
    }
    if (Object.keys(pack.followups).length === 0) {
        warnings.push("followups: none defined, so bookings get no confirmation or reminder text");
    }
    if (pack.catalog.priceField === null) {
        warnings.push("catalog.price_field is not set, so price_items can't add up prices");
    }
    const hints = pack.voice.speechHints.map(h => h.toLowerCase());
    const repeated = [...new Set(hints.filter((h, i) => hints.indexOf(h) !== i))];
    if (repeated.length > 0) {
        warnings.push(`voice.speech_hints repeats: ${repeated.join(", ")}`);
    }
    if (pack.voice.speechHints.join(",").length > 2000) {
        warnings.push("voice.speech_hints is longer than the 2,000 characters speech recognizers accept; extras are ignored");
    }
    return warnings;
}

/**
 * The TOML shape of a domain pack, as a zod schema, and its conversion to DomainPack.
 *
 * Keys stay snake_case to match the files (and the Python version, so existing packs load unchanged).
 * Unknown keys are errors, so a typo in a hand-written or model-written pack is caught, not ignored.
 * Cross-references (catalog fields named by templates, aliases and topics; placeholders in prompt text)
 * are checked after the shape, and every problem is reported at once.
 */

import { z } from "zod";

import { DomainId, FieldType, Language, type LocalizedText } from "../core/index.ts";
import type { DomainPack } from "./pack.ts";
import { FOLLOWUP_FIELDS, Template } from "./template.ts";

const text = z.string();
const flag = z.boolean().default(false);
const texts = z.array(z.string()).default([]);
const optionalName = z.string().optional().transform(v => v || null);

const LANGUAGES = Object.values(Language) as [Language, ...Language[]];

/**
 * A plain string (English) or a table of translations that includes English:
 * `confirm = "..."` or `[followups.confirm] en = "..." es = "..."`.
 */
const localized = z.preprocess(
    v => typeof v === "string"
        ? { en: v }
        : v,
    z.partialRecord(z.enum(LANGUAGES), z.string())
        .refine(t => typeof t.en === "string", { message: "translations must include English (en)" })
        .transform(t => t as LocalizedText),
);

const DomainSection = z.strictObject({
    id:                       text.regex(/^[a-z][a-z0-9_-]*$/, "lowercase letters, digits, - and _"),
    name:                     text,
    description:              text.default(""),
    business_noun:            text,
    staff_noun:               text,
    agent_role:               text,
    voice_agent_role:         text.default("receptionist"),
    default_agent_name:       text,
    default_voice_agent_name: text,
    goal:                     text,
    upsell_topics:            text.default(""),
});

const ProfileQuestion = z.strictObject({
    key:      text,
    label:    text,
    question: text,
    required: flag,
});

const CatalogField = z.strictObject({
    name:       text,
    type:       z.enum(Object.values(FieldType) as [FieldType, ...FieldType[]]).default("text"),
    label:      text.default(""),
    required:   flag,
    searchable: flag,
    empty_text: text.default(""),
    in_details: z.boolean().default(true),
});

const Sendable = z.strictObject({
    field:   text,
    label:   text,
    trigger: text,
});

const CatalogSection = z.strictObject({
    item_noun:        text,
    item_noun_plural: text,
    spoken_noun:      text.optional(),
    heading:          text.optional(),
    intro:            text.default(""),
    empty_text:       text.optional(),
    title_template:   text,
    id_field:         optionalName,
    aliases:          z.record(z.string(), z.string()).default({}),
    summary_parts:    texts,
    summary_suffix:   text.default(""),
    url_field:        optionalName,
    price_field:      optionalName,
    fields:           z.array(CatalogField).min(1, "needs at least one [[catalog.fields]] entry"),
    sendables:        z.array(Sendable).default([]),
});

const IntakeField = z.strictObject({
    key:      text,
    label:    text,
    ask:      text,
    required: flag,
    channels: z.array(z.enum(["chat", "voice"])).min(1).default(["chat", "voice"]),
});

const BookingSection = z.strictObject({
    visit_noun:       text,
    subject_label:    text,
    subject_hint:     text,
    general_subject:  text,
    location:         z.enum(["business", "customer"]).default("business"),
    skip_intake_when: text.default(""),
    intake:           z.array(IntakeField).default([]),
});

const WidgetTopic = z.strictObject({
    section:        text,
    label:          text,
    message:        text,
    requires_field: optionalName,
    requires_any:   texts,
});

const WidgetSection = z.strictObject({
    header_title:      text,
    tagline:           text.default("Instant answers, 24/7."),
    input_placeholder: text,
    icon:              text.default("💬"),
    capabilities:      text,
    topics:            z.array(WidgetTopic).default([]),
});

const Pronunciation = z.strictObject({
    pattern: text.refine(isRegex, "not a valid regular expression"),
    say:     text,
});

const VoiceSection = z.strictObject({
    speech_hints:   z.union([z.array(z.string()), z.string()]).default([]),
    speak_years:    flag,
    pronunciations: z.array(Pronunciation).default([]),
});

const PromptToggle = z.strictObject({
    setting:    text,
    chat:       text.default(""),
    voice:      text.default(""),
    else_chat:  text.default(""),
    else_voice: text.default(""),
});

const PromptsSection = z.strictObject({
    chat:          text,
    voice:         text,
    summary_focus: text.default(""),
    toggles:       z.array(PromptToggle).default([]),
});

const Topic = z.strictObject({
    id:          text.regex(/^[a-z][a-z0-9_]*$/, "lowercase letters, digits and _"),
    label:       text,
    description: text,
});

const Followups = z.strictObject({
    confirm:  localized.optional(),
    reminder: localized.optional(),
    cold:     localized.optional(),
});

/**
 * The whole file, before cross-reference checks.
 */
export const PackFile = z.strictObject({
    domain:    DomainSection,
    profile:   z.array(ProfileQuestion).default([]),
    catalog:   CatalogSection,
    booking:   BookingSection,
    widget:    WidgetSection,
    voice:     VoiceSection.default({ speech_hints: [], speak_years: false, pronunciations: [] }),
    prompts:   PromptsSection,
    topics:    z.array(Topic).min(1, "needs at least one [[topics]] entry for classification"),
    followups: Followups.default({}),
});
export type PackFile = z.output<typeof PackFile>;

function isRegex(pattern: string): boolean {
    try {
        new RegExp(pattern, "i");
        return true;
    } catch {
        return false;
    }
}

/**
 * A problem found in a pack file. `path` is dotted, e.g. "catalog.fields.2.name".
 */
export type PackProblem = {
    readonly path:    string;
    readonly message: string;
};

/**
 * Cross-reference problems in a pack that already has the right shape.
 */
export function crossCheck(file: PackFile): PackProblem[] {
    const problems: PackProblem[] = [];
    const add = (path: string, message: string) => problems.push({ path, message });
    const fieldNames = new Set(file.catalog.fields.map(f => f.name));

    const checkField = (name: string | null, path: string) => {
        if (name !== null && !fieldNames.has(name)) {
            add(path, `refers to unknown catalog field '${name}'`);
        }
    };
    const duplicates = (keys: readonly string[], path: string) => {
        const seen = new Set<string>();
        for (const key of keys) {
            if (seen.has(key)) {
                add(path, `duplicate '${key}'`);
            }
            seen.add(key);
        }
    };

    duplicates(file.catalog.fields.map(f => f.name), "catalog.fields");
    duplicates(file.profile.map(q => q.key), "profile");
    duplicates(file.booking.intake.map(f => f.key), "booking.intake");
    duplicates(file.topics.map(t => t.id), "topics");
    if (file.topics.some(t => t.id === "other")) {
        add("topics", "'other' is added automatically; remove it");
    }

    checkField(file.catalog.id_field, "catalog.id_field");
    checkField(file.catalog.url_field, "catalog.url_field");
    checkField(file.catalog.price_field, "catalog.price_field");
    for (const [raw, target] of Object.entries(file.catalog.aliases)) {
        checkField(target, `catalog.aliases.${raw}`);
    }
    file.catalog.sendables.forEach((s, i) => {
        checkField(s.field, `catalog.sendables.${i}.field`);
        if (!isRegex(s.trigger)) {
            add(`catalog.sendables.${i}.trigger`, "not a valid regular expression");
        }
    });
    file.widget.topics.forEach((t, i) => checkField(t.requires_field, `widget.topics.${i}.requires_field`));

    const catalogTemplates: [string, string][] = [
        ["catalog.title_template", file.catalog.title_template],
        ["catalog.summary_suffix", file.catalog.summary_suffix],
        ...file.catalog.summary_parts.map((p, i): [string, string] => [`catalog.summary_parts.${i}`, p]),
    ];
    for (const [path, template] of catalogTemplates) {
        const fields = Template.fields(template);
        if (!fields.ok) {
            add(path, fields.error);
            continue;
        }
        fields.value.forEach(name => checkField(name, path));
    }

    const promptTexts: [string, string][] = [
        ["domain.goal", file.domain.goal],
        ["prompts.chat", file.prompts.chat],
        ["prompts.voice", file.prompts.voice],
        ["prompts.summary_focus", file.prompts.summary_focus],
        ["booking.skip_intake_when", file.booking.skip_intake_when],
        ["widget.capabilities", file.widget.capabilities],
        ...file.booking.intake.map((f, i): [string, string] => [`booking.intake.${i}.ask`, f.ask]),
        ...file.prompts.toggles.flatMap((t, i): [string, string][] => [
            [`prompts.toggles.${i}.chat`, t.chat],
            [`prompts.toggles.${i}.voice`, t.voice],
            [`prompts.toggles.${i}.else_chat`, t.else_chat],
            [`prompts.toggles.${i}.else_voice`, t.else_voice],
        ]),
    ];
    for (const [path, template] of promptTexts) {
        Template.problems(template).forEach(message => add(path, message));
    }
    for (const [kind, translations] of Object.entries(file.followups)) {
        for (const [language, template] of Object.entries(translations ?? {})) {
            Template.problems(template, FOLLOWUP_FIELDS).forEach(message => add(`followups.${kind}.${language}`, message));
        }
    }
    return problems;
}

/**
 * Convert a checked pack file to the shape the rest of the code uses.
 */
export function toPack(file: PackFile): DomainPack {
    const c = file.catalog;
    const hints = typeof file.voice.speech_hints === "string"
        ? file.voice.speech_hints.split(",")
        : file.voice.speech_hints;
    return {
        id:                    DomainId(file.domain.id),
        name:                  file.domain.name,
        description:           file.domain.description,
        businessNoun:          file.domain.business_noun,
        staffNoun:             file.domain.staff_noun,
        agentRole:             file.domain.agent_role,
        voiceAgentRole:        file.domain.voice_agent_role,
        defaultAgentName:      file.domain.default_agent_name,
        defaultVoiceAgentName: file.domain.default_voice_agent_name,
        goal:                  file.domain.goal,
        upsellTopics:          file.domain.upsell_topics,
        profile:               file.profile,
        catalog: {
            itemNoun:       c.item_noun,
            itemNounPlural: c.item_noun_plural,
            spokenNoun:     c.spoken_noun ?? c.item_noun,
            heading:        c.heading ?? c.item_noun_plural.toUpperCase(),
            intro:          c.intro,
            emptyText:      c.empty_text ?? `(No ${c.item_noun_plural} listed yet.)`,
            fields:         c.fields.map(f => ({
                name:       f.name,
                type:       f.type,
                label:      f.label,
                required:   f.required,
                searchable: f.searchable,
                emptyText:  f.empty_text,
                inDetails:  f.in_details,
            })),
            titleTemplate:  c.title_template,
            idField:        c.id_field,
            aliases:        Object.fromEntries(Object.entries(c.aliases).map(([k, v]) => [k.toLowerCase(), v])),
            summaryParts:   c.summary_parts,
            summarySuffix:  c.summary_suffix,
            urlField:       c.url_field,
            priceField:     c.price_field,
            sendables:      c.sendables,
        },
        booking: {
            visitNoun:      file.booking.visit_noun,
            subjectLabel:   file.booking.subject_label,
            subjectHint:    file.booking.subject_hint,
            generalSubject: file.booking.general_subject,
            location:       file.booking.location,
            skipIntakeWhen: file.booking.skip_intake_when,
            intake:         file.booking.intake,
        },
        widget: {
            headerTitle:      file.widget.header_title,
            tagline:          file.widget.tagline,
            inputPlaceholder: file.widget.input_placeholder,
            icon:             file.widget.icon,
            capabilities:     file.widget.capabilities,
            topics:           file.widget.topics.map(t => ({
                section:       t.section,
                label:         t.label,
                message:       t.message,
                requiresField: t.requires_field,
                requiresAny:   t.requires_any,
            })),
        },
        voice: {
            speechHints:    hints.map(h => h.trim()).filter(Boolean),
            speakYears:     file.voice.speak_years,
            pronunciations: file.voice.pronunciations,
        },
        prompts: {
            chat:         file.prompts.chat,
            voice:        file.prompts.voice,
            summaryFocus: file.prompts.summary_focus,
            toggles:      file.prompts.toggles.map(t => ({
                setting:   t.setting,
                chat:      t.chat,
                voice:     t.voice,
                elseChat:  t.else_chat,
                elseVoice: t.else_voice,
            })),
        },
        topics:    file.topics,
        followups: Object.fromEntries(
            Object.entries(file.followups).filter(([, v]) => v !== undefined)
        ),
    };
}

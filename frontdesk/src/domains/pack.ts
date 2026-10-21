/**
 * Domain packs: everything that makes the assistant a car dealer, a plumber, a salon, ...
 *
 * A pack is one TOML file with a fixed shape (see packs/_template.toml).
 * The universal base prompts and code never mention a business type;
 * a pack supplies the vocabulary, catalog shape, booking intake, widget topics, voice hints,
 * classification topics and the domain-specific prompt sections appended to the base.
 * These are the parsed, validated forms; schema.ts reads the TOML into them.
 */

import type { DomainId, FieldSpec, LocalizedText, Professionalism, ValueOf } from "../core/index.ts";

/**
 * A question each tenant answers when onboarding (e.g. "Do you offer financing?").
 * Answers live in `Tenant.profile[key]` and are shown to the model under `label`.
 */
export type ProfileQuestion = {
    readonly key:      string;
    readonly label:    string;
    readonly question: string;
    readonly required: boolean;
};

export const IntakeChannel = {
    Chat:  "chat",
    Voice: "voice",
} as const;
export type IntakeChannel = ValueOf<typeof IntakeChannel>;

/**
 * Something the assistant collects before confirming a booking (e.g. trade-in, service address).
 * `ask` tells the model what to ask and when it can be skipped.
 * It also becomes a parameter of the booking tool, required when `required` is set.
 */
export type IntakeField = {
    readonly key:      string;
    readonly label:    string;
    readonly ask:      string;
    readonly required: boolean;
    readonly channels: readonly IntakeChannel[];
};

/**
 * Where a visit happens: the customer comes in, or staff go to them.
 */
export const VisitLocation = {
    Business: "business",
    Customer: "customer",
} as const;
export type VisitLocation = ValueOf<typeof VisitLocation>;

export type BookingSpec = {
    readonly visitNoun:      string;
    readonly subjectLabel:   string;
    readonly subjectHint:    string;
    readonly generalSubject: string;
    readonly location:       VisitLocation;
    readonly skipIntakeWhen: string;
    readonly intake:         readonly IntakeField[];
};

/**
 * A catalog field the system sends on request instead of letting the model write it
 * (listing URLs, history reports). `trigger` is a pattern matched against the customer's message.
 */
export type SendableField = {
    readonly field:   string;
    readonly label:   string;
    readonly trigger: string;
};

/**
 * The shape of a domain's catalog (vehicles, services, ...).
 * `titleTemplate`, `summaryParts` and `summarySuffix` use `{field}` placeholders;
 * a summary part is dropped when every field it references is empty and has no `emptyText`.
 * `aliases` maps raw source keys (lowercased, e.g. scraped "carfaxurl") to field names.
 */
export type CatalogSpec = {
    readonly itemNoun:       string;
    readonly itemNounPlural: string;
    readonly spokenNoun:     string;
    readonly heading:        string;
    readonly intro:          string;
    readonly emptyText:      string;
    readonly fields:         readonly FieldSpec[];
    readonly titleTemplate:  string;
    readonly idField:        string | null;
    readonly aliases:        Readonly<Record<string, string>>;
    readonly summaryParts:   readonly string[];
    readonly summarySuffix:  string;
    readonly urlField:       string | null;
    readonly priceField:     string | null;
    readonly sendables:      readonly SendableField[];
};

/**
 * A sidebar shortcut in the web widget. When `requiresField` is set, the topic is shown only if
 * some catalog item's field contains one of `requiresAny` (case-insensitive).
 */
export type WidgetTopic = {
    readonly section:       string;
    readonly label:         string;
    readonly message:       string;
    readonly requiresField: string | null;
    readonly requiresAny:   readonly string[];
};

export type WidgetSpec = {
    readonly headerTitle:      string;
    readonly tagline:          string;
    readonly inputPlaceholder: string;
    readonly icon:             string;
    readonly capabilities:     string;
    readonly topics:           readonly WidgetTopic[];
};

/**
 * `pattern` is a case-insensitive regular expression; matches are spoken as `say`.
 */
export type Pronunciation = {
    readonly pattern: string;
    readonly say:     string;
};

/**
 * `speechHints` are words the speech recognizer should expect (makes, models, trade terms).
 * `speakYears` reads "2017" as "twenty seventeen".
 */
export type VoiceSpec = {
    readonly speechHints:    readonly string[];
    readonly speakYears:     boolean;
    readonly pronunciations: readonly Pronunciation[];
};

/**
 * A domain prompt section included only when a tenant setting is truthy (`chat` / `voice`)
 * or falsy (`elseChat` / `elseVoice`).
 */
export type PromptToggle = {
    readonly setting:   string;
    readonly chat:      string;
    readonly voice:     string;
    readonly elseChat:  string;
    readonly elseVoice: string;
};

/**
 * `summaryFocus` tells the classifier what staff care about most when writing the gist.
 */
export type DomainPrompts = {
    readonly chat:         string;
    readonly voice:        string;
    readonly summaryFocus: string;
    readonly toggles:      readonly PromptToggle[];
};

/**
 * One category a closed conversation can be classified under, for metrics and email subjects.
 * `description` tells the classifier when it applies. Every pack also gets an implicit "other".
 */
export type ClassificationTopic = {
    readonly id:          string;
    readonly label:       string;
    readonly description: string;
};

export const FollowupKind = {
    Confirm:  "confirm",
    Reminder: "reminder",
    Cold:     "cold",
} as const;
export type FollowupKind = ValueOf<typeof FollowupKind>;

/**
 * Defaults a tenant can override in its settings.
 * `professionalism` null keeps the original pairing: polished in text, casual on calls.
 * `sensitive` turns on the discretion rules for businesses that owe clients confidentiality (see PrivacySettings).
 */
export type PackDefaults = {
    readonly professionalism: Professionalism | null;
    readonly honestAboutAi:   boolean;
    readonly sensitive:       boolean;
};

export type DomainPack = {
    readonly id:                    DomainId;
    readonly name:                  string;
    readonly description:           string;
    readonly businessNoun:          string;
    readonly staffNoun:             string;
    readonly agentRole:             string;
    readonly voiceAgentRole:        string;
    readonly defaultAgentName:      string;
    readonly defaultVoiceAgentName: string;
    readonly goal:                  string;
    readonly upsellTopics:          string;
    readonly defaults:              PackDefaults;
    readonly profile:               readonly ProfileQuestion[];
    readonly catalog:               CatalogSpec;
    readonly booking:               BookingSpec;
    readonly widget:                WidgetSpec;
    readonly voice:                 VoiceSpec;
    readonly prompts:               DomainPrompts;
    readonly topics:                readonly ClassificationTopic[];
    readonly followups:             Readonly<Partial<Record<FollowupKind, LocalizedText>>>;
};

export const OTHER_TOPIC: ClassificationTopic = {
    id:          "other",
    label:       "Other",
    description: "Anything that fits none of the other topics.",
};

export const DomainPack = {
    field(pack: DomainPack, name: string): FieldSpec | null {
        return pack.catalog.fields.find(f => f.name === name) ?? null;
    },

    /**
     * The pack's classification topics plus the implicit "other".
     */
    topics(pack: DomainPack): readonly ClassificationTopic[] {
        return [...pack.topics, OTHER_TOPIC];
    },

    intakeFor(pack: DomainPack, channel: IntakeChannel): readonly IntakeField[] {
        return pack.booking.intake.filter(f => f.channels.includes(channel));
    },
} as const;

/**
 * Domain packs: business-type configuration layered on the universal base.
 */

export {
    DomainPackError,
    DomainRegistry,
    loadPack,
    PACKS_DIR,
    parsePack,
    parsePackToml,
} from "./load.ts";
export {
    DomainPack,
    FollowupKind,
    IntakeChannel,
    OTHER_TOPIC,
    VisitLocation,
    type BookingSpec,
    type CatalogSpec,
    type ClassificationTopic,
    type DomainPrompts,
    type IntakeField,
    type ProfileQuestion,
    type Pronunciation,
    type PromptToggle,
    type SendableField,
    type VoiceSpec,
    type WidgetSpec,
    type WidgetTopic,
} from "./pack.ts";
export {
    PackFile,
    type PackProblem,
} from "./schema.ts";
export {
    FOLLOWUP_FIELDS,
    PROMPT_FIELDS,
    Template,
} from "./template.ts";

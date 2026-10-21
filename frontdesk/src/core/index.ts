/**
 * Domain-neutral types every other module builds on.
 */

export {
    Appointment,
    AppointmentNotice,
    AppointmentStatus,
} from "./appointment.ts";
export {
    Bound,
    PLATFORM_BOUNDS,
} from "./bounds.ts";
export {
    Abuse,
    Budget,
    ClosingReason,
    DEFAULT_ABUSE_POLICY,
    DEFAULT_SESSION_LIMITS,
    RefusalReason,
    SessionMeter,
    TurnAssessment,
    type AbusePolicy,
    type Admission,
    type BudgetInput,
    type BudgetVerdict,
    type CapStatus,
    type Closing,
    type SessionLimits,
} from "./budget.ts";
export {
    CallEndReason,
    CallState,
    type CallDirective,
    type CallEvent,
    type CallLimits,
    type InvalidCallTransition,
} from "./call.ts";
export {
    CatalogItem,
    DEFAULT_CATALOG_SETTINGS,
    FieldType,
    type CatalogSettings,
    type CatalogSource,
    type CatalogSyncStatus,
    type FieldSpec,
    type FieldValue,
} from "./catalog.ts";
export {
    Address,
    Channel,
    PhoneNumber,
} from "./channel.ts";
export {
    Block,
    Contact,
    type ContactChanges,
} from "./contact.ts";
export {
    CallVendor,
    Role,
    type Conversation,
    type Message,
    type Session,
} from "./conversation.ts";
export {
    CallFallback,
    CallHandling,
    Coverage,
    DEFAULT_CALL_FALLBACKS,
    type CallFallbacks,
    type CallRoute,
} from "./coverage.ts";
export {
    type DomainEvent,
    type EventBody,
    type EventKind,
    type EventMeta,
    type EventOf,
    type HandoffReason,
    type SessionCloseReason,
} from "./events.ts";
export {
    AppointmentId,
    CatalogItemId,
    ContactId,
    ConversationId,
    DomainId,
    EventId,
    JobId,
    MessageId,
    SessionId,
    TenantId,
} from "./ids.ts";
export {
    type Inbound,
    type TenantRef,
} from "./inbound.ts";
export {
    type Job,
    type JobBody,
    type JobKind,
    type JobMeta,
    type JobOf,
} from "./jobs.ts";
export {
    assertNever,
    type Brand,
    type ValueOf,
    type Variant,
} from "./lang.ts";
export {
    LANGUAGE_NAMES,
    Language,
    LanguageGuess,
    LanguageTag,
    LocalizedText,
} from "./language.ts";
export {
    DEFAULT_PRICING,
    Plan,
    type PlanPricing,
    type Subscription,
} from "./plan.ts";
export {
    Crisis,
    DEFAULT_PRIVACY,
    Pii,
    PiiKind,
    type PrivacySettings,
    type Redacted,
} from "./privacy.ts";
export {
    CONTROL_VARIANT,
    PromptVariant,
    type EvalRecord,
    type EvalSummary,
    type QualityRow,
    type TurnMetric,
} from "./quality.ts";
export {
    AsyncQueue,
} from "./queue.ts";
export {
    Result,
} from "./result.ts";
export {
    LocalClock,
    ServiceSchedule,
    type ScheduleProblem,
    type Weekday,
    type WeeklyWindow,
} from "./schedule.ts";
export {
    NotifyVia,
    Professionalism,
    DEFAULT_CALL_SETTINGS,
    StaffNotice,
    Tenant,
    type AgentStyle,
    type Branding,
    type CallSettings,
    type RecordingDisclosure,
    type TenantLink,
    type TenantSettings,
} from "./tenant.ts";
export {
    LocalTime,
    WEEKDAY_NAMES,
    type WeekdayName,
} from "./time.ts";
export {
    Sentiment,
    SessionOutcome,
    Urgency,
    type Classification,
    type Transcript,
} from "./transcript.ts";
export {
    Purpose,
    ServiceKind,
    UsageUnit,
    type UsageRecord,
} from "./usage.ts";
export {
    MAX_VOICEMAIL_SECONDS,
    type Voicemail,
} from "./voicemail.ts";

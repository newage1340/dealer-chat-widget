/**
 * Domain-neutral types every other module builds on.
 */

export {
    Appointment,
    AppointmentNotice,
    AppointmentStatus,
} from "./appointment.ts";
export {
    Budget,
    BudgetReason,
    SessionMeter,
    TurnAssessment,
    type BudgetVerdict,
    type ContactLimits,
    type Limits,
    type SessionLimits,
    type Spend,
    type TenantLimits,
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
    FieldType,
    type FieldSpec,
    type FieldValue,
} from "./catalog.ts";
export {
    Address,
    Channel,
    PhoneNumber,
} from "./channel.ts";
export {
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
    Language,
    LanguageTag,
    LocalizedText,
} from "./language.ts";
export {
    Result,
} from "./result.ts";
export {
    NotifyVia,
    StaffNotice,
    Tenant,
    type Branding,
    type Plan,
    type TenantLink,
    type TenantSettings,
} from "./tenant.ts";
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

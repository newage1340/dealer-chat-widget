/**
 * Facts recorded as conversations happen.
 *
 * A turn never sends texts or alerts itself; it records events.
 * Subscribers react to them after the reply has gone out (texts, staff alerts, summaries, follow-ups),
 * and an event's id doubles as the idempotency key for everything that follows from it.
 */

import type { Appointment } from "./appointment.ts";
import type { CapStatus, ClosingReason, RefusalReason, TurnAssessment } from "./budget.ts";
import type { CallEndReason } from "./call.ts";
import type { Address, Channel, PhoneNumber } from "./channel.ts";
import type { Block, ContactChanges } from "./contact.ts";
import type { AppointmentId, ContactId, ConversationId, EventId, MessageId, SessionId, TenantId } from "./ids.ts";
import type { Variant } from "./lang.ts";
import type { Classification } from "./transcript.ts";

/**
 * Why the assistant handed a conversation to staff.
 */
export type HandoffReason =
    | "transfer"
    | "take_message"
    | "missed_call"
    | "crisis";

/**
 * Why a session closed. Calls end for call reasons;
 * chats and text threads close when they go idle or the visitor clears them.
 */
export type SessionCloseReason =
    | CallEndReason
    | "idle"
    | "cleared";

export type EventBody =
    | { readonly kind: "message_received";
		readonly messageId: MessageId;
		readonly channel: Channel
	}
    | { readonly kind: "reply_sent";
		readonly messageId: MessageId;
		readonly channel: Channel
	}
    | { readonly kind: "contact_updated";
		readonly contactId: ContactId;
		readonly changes: ContactChanges
	}
    | { readonly kind: "opted_out";
		readonly contactId: ContactId
	}
    | { readonly kind: "opted_in";
		readonly contactId: ContactId
	}
    | { readonly kind: "appointment_booked";
		readonly appointment: Appointment;
		readonly replaces: AppointmentId | null
	}
    | { readonly kind: "appointment_cancelled";
		readonly appointmentId: AppointmentId
	}
    | { readonly kind: "link_requested";
		readonly to: Address;
		readonly label: string;
		readonly url: string
	}
    | { readonly kind: "handoff_requested";
		readonly reason: HandoffReason;
		readonly note: string
	}
    | { readonly kind: "call_started";
		readonly from: Address
	}
    | { readonly kind: "voicemail_left";
		readonly recordingUrl: string;
		readonly durationSeconds: number
	}
    | { readonly kind: "call_forwarded";
		readonly to: PhoneNumber;
		readonly reason: RefusalReason
	}
    | { readonly kind: "turn_flagged";
		readonly assessment: TurnAssessment;
		readonly note: string
	}
    | { readonly kind: "session_closing";
		readonly reason: ClosingReason
	}
    | { readonly kind: "tenant_cap";
		readonly status: CapStatus
	}
    | { readonly kind: "contact_blocked";
		readonly contactId: ContactId;
		readonly block: Block
	}
    | { readonly kind: "contact_unblocked";
		readonly contactId: ContactId
	}
    | { readonly kind: "session_closed";
		readonly reason: SessionCloseReason;
		readonly turns: number
	}
    | { readonly kind: "session_classified";
		readonly classification: Classification
	};

export type EventKind = EventBody["kind"];

/**
 * `causedBy` is the event or inbound dedupe key this event resulted from, for tracing.
 */
export type EventMeta = {
    readonly id:             EventId;
    readonly tenantId:       TenantId;
    readonly conversationId: ConversationId;
    readonly sessionId:      SessionId | null;
    readonly occurredAt:     Date;
    readonly causedBy:       string | null;
};

export type DomainEvent = EventMeta & EventBody;

export type EventOf<K extends EventKind> = EventMeta & Variant<EventBody, K>;

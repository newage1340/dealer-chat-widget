/**
 * Conversations, the sessions inside them, and the messages they hold.
 *
 * A Conversation is one contact's long-lived thread with one tenant.
 * It is channel-agnostic: SMS and calls from the same phone land in the same conversation.
 * A Session is one bounded interaction inside it: a call, a widget visit, a burst of texts.
 * State that must not leak between interactions (links already sent, call progress) lives on the Session.
 */

import type { CallState } from "./call.ts";
import type { Channel } from "./channel.ts";
import type { ContactId, ConversationId, MessageId, SessionId, TenantId } from "./ids.ts";
import type { ValueOf } from "./lang.ts";

export type Conversation = {
    readonly id:            ConversationId;
    readonly tenantId:      TenantId;
    readonly contactId:     ContactId;
    readonly createdAt:     Date;
    readonly lastMessageAt: Date | null;
};

/**
 * Who placed a call: the carrier network (Twilio) or a voice platform (Vapi),
 * which differ in how call control is returned to them.
 */
export const CallVendor = {
    Twilio: "twilio",
    Vapi:   "vapi",
} as const;
export type CallVendor = ValueOf<typeof CallVendor>;

type SessionBase = {
    readonly id:             SessionId;
    readonly conversationId: ConversationId;
    readonly tenantId:       TenantId;
    readonly startedAt:      Date;
    readonly endedAt:        Date | null;
    readonly sentUrls:       readonly string[];
};

/**
 * `externalId` is the vendor's id for the interaction (CallSid, Vapi call id, widget session id),
 * used to find the session again on the next webhook.
 */
export type Session = SessionBase & (
    | { readonly channel: "web";
		readonly externalId: string
	}
    | { readonly channel: "sms";
		readonly externalId: null
	}
    | { readonly channel: "voice";
		readonly externalId: string;
		readonly vendor: CallVendor;
		readonly call: CallState
	}
);

export const Role = {
    User:      "user",
    Assistant: "assistant",
} as const;
export type Role = ValueOf<typeof Role>;

/**
 * One stored line of conversation history.
 * Tool calls and their results are not stored here; they are recorded as events.
 */
export type Message = {
    readonly id:             MessageId;
    readonly conversationId: ConversationId;
    readonly sessionId:      SessionId;
    readonly role:           Role;
    readonly text:           string;
    readonly channel:        Channel;
    readonly createdAt:      Date;
};

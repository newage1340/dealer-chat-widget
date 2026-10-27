/**
 * Messages callers leave when the assistant is not answering.
 * A voicemail is a voice session like any other: once transcribed, its text becomes the session's one message,
 * so it is classified and emailed to staff through the same transcript pipeline as a conversation.
 */

import type { Address } from "./channel.ts";
import type { ConversationId, SessionId, TenantId } from "./ids.ts";

/**
 * `recordingUrl` points at the vendor's copy of the audio (Twilio), linked from the staff email.
 */
export type Voicemail = {
    readonly sessionId:       SessionId;
    readonly tenantId:        TenantId;
    readonly conversationId:  ConversationId;
    readonly from:            Address;
    readonly recordingUrl:    string;
    readonly durationSeconds: number;
    readonly receivedAt:      Date;
};

/**
 * Longest message we record; long enough for a real message, short enough to bound the cost.
 */
export const MAX_VOICEMAIL_SECONDS = 120;

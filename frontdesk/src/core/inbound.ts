/**
 * A message as it arrives from a channel. The channel has already found the tenant
 * (by the number texted or called, or the widget's slug); the contact is resolved by the engine.
 * Channel adapters build these from vendor payloads (validated at the edge),
 * so nothing past this point sees a vendor's wire format.
 */

import type { Address, Channel, PhoneNumber } from "./channel.ts";
import type { Language } from "./language.ts";

/**
 * `dedupeKey` is unique per delivery attempt's underlying message
 * (Twilio MessageSid, CallSid plus turn number, widget message id).
 * A webhook retried by the vendor carries the same key and is answered from the first result.
 * `externalSessionId` ties the message to a vendor session (CallSid, widget session); null for SMS.
 * `confidence` is the speech recognizer's confidence for voice input, null elsewhere.
 * `language` is the language a multilingual recognizer detected, when it reports one.
 * `forwardedFrom` is the number that forwarded a call to us, when the carrier reports it.
 */
export type Inbound = {
    readonly channel:           Channel;
    readonly sender:            Address;
    readonly text:              string;
    readonly receivedAt:        Date;
    readonly dedupeKey:         string;
    readonly externalSessionId: string | null;
    readonly confidence:        number | null;
    readonly language:          Language | null;
    readonly forwardedFrom:     PhoneNumber | null;
};

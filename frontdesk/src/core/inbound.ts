/**
 * A message as it arrives from a channel, before tenant and contact are resolved.
 * Channel adapters build these from vendor payloads (validated at the edge),
 * so nothing past this point sees a vendor's wire format.
 */

import type { Address, Channel } from "./channel.ts";

/**
 * How an inbound message names its tenant:
 * phone channels by the number that was texted or called, the web widget by its slug.
 */
export type TenantRef =
    | { readonly kind: "endpoint";
		readonly address: Address
	}
    | { readonly kind: "slug";
		readonly slug: string
	};

/**
 * `dedupeKey` is unique per delivery attempt's underlying message
 * (Twilio MessageSid, CallSid plus turn number, widget message id).
 * A webhook retried by the vendor carries the same key and is answered from the first result.
 * `externalSessionId` ties the message to a vendor session (CallSid, widget session); null for SMS.
 * `confidence` is the speech recognizer's confidence for voice input, null elsewhere.
 */
export type Inbound = {
    readonly channel:           Channel;
    readonly tenant:            TenantRef;
    readonly sender:            Address;
    readonly text:              string;
    readonly receivedAt:        Date;
    readonly dedupeKey:         string;
    readonly externalSessionId: string | null;
    readonly confidence:        number | null;
};

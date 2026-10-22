/**
 * Outbound text messaging (Twilio today; any SMS vendor tomorrow).
 */

import type { PhoneNumber, Purpose, TenantId, UsageRecord } from "../core/index.ts";

export type OutboundSms = {
    readonly to:       PhoneNumber;
    readonly from:     PhoneNumber;
    readonly text:     string;
    readonly purpose:  Purpose;
    readonly tenantId: TenantId;
};

export type SendReceipt = {
    readonly providerMessageId: string;
    readonly usage:             UsageRecord;
};

export interface MessagingProvider {
    readonly name: string;

    /**
     * Throws ProviderError on failure.
     */
    send(message: OutboundSms, signal?: AbortSignal): Promise<SendReceipt>;
}

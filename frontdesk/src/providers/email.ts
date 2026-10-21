/**
 * Outbound email (transcripts, staff notices).
 * Far cheaper than SMS per message, and with no length limit.
 */

import type { Purpose, TenantId, UsageRecord } from "../core/index.ts";

/**
 * `html` is optional; every email has a plain-text body.
 * `replyTo` lets staff answer a transcript email and reach the business inbox.
 */
export type OutboundEmail = {
    readonly to:       readonly string[];
    readonly from:     string;
    readonly replyTo:  string | null;
    readonly subject:  string;
    readonly text:     string;
    readonly html:     string | null;
    readonly purpose:  Purpose;
    readonly tenantId: TenantId;
};

export type EmailReceipt = {
    readonly providerMessageId: string;
    readonly usage:             UsageRecord;
};

export interface EmailProvider {
    readonly name: string;

    /**
     * Throws ProviderError on failure.
     */
    send(email: OutboundEmail, signal?: AbortSignal): Promise<EmailReceipt>;
}

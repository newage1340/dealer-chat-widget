/**
 * Appointments a contact books with a tenant.
 */

import type { AppointmentId, CatalogItemId, ContactId, ConversationId, TenantId } from "./ids.ts";
import type { ValueOf } from "./lang.ts";

export const AppointmentStatus = {
    Confirmed: "confirmed",
    Cancelled: "cancelled",
} as const;
export type AppointmentStatus = ValueOf<typeof AppointmentStatus>;

/**
 * Notices that can go out about an appointment; each is sent at most once.
 */
export const AppointmentNotice = {
    Confirmation: "confirmation",
    Reminder:     "reminder",
    Followup:     "followup",
} as const;
export type AppointmentNotice = ValueOf<typeof AppointmentNotice>;

/**
 * A booked visit.
 * `timeText` keeps the time as the customer said it ("tomorrow after work");
 * `startsAt` is set only when it could be resolved to an instant.
 * `location` is where the visit happens when it is not at the business.
 * `details` holds the domain's intake answers (e.g. trade-in, problem description).
 */
export type Appointment = {
    readonly id:             AppointmentId;
    readonly tenantId:       TenantId;
    readonly contactId:      ContactId;
    readonly conversationId: ConversationId;
    readonly timeText:       string;
    readonly startsAt:       Date | null;
    readonly subject:        string;
    readonly location:       string;
    readonly details:        Readonly<Record<string, string>>;
    readonly itemIds:        readonly CatalogItemId[];
    readonly status:         AppointmentStatus;
    readonly sentNotices:    readonly AppointmentNotice[];
    readonly createdAt:      Date;
};

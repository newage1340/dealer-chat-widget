/**
 * Identifiers shared across the system.
 * Each is branded, so a ContactId can never be passed where a TenantId is expected.
 * The same-named functions are the only sanctioned way to make one from a string.
 */

import type { Brand } from "./lang.ts";

export type TenantId       = Brand<string, "TenantId">;
export type DomainId       = Brand<string, "DomainId">;
export type ContactId      = Brand<string, "ContactId">;
export type ConversationId = Brand<string, "ConversationId">;
export type SessionId      = Brand<string, "SessionId">;
export type MessageId      = Brand<string, "MessageId">;
export type AppointmentId  = Brand<string, "AppointmentId">;
export type CatalogItemId  = Brand<string, "CatalogItemId">;
export type EventId        = Brand<string, "EventId">;
export type JobId          = Brand<string, "JobId">;

export const TenantId       = (value: string) => value as TenantId;
export const DomainId       = (value: string) => value as DomainId;
export const ContactId      = (value: string) => value as ContactId;
export const ConversationId = (value: string) => value as ConversationId;
export const SessionId      = (value: string) => value as SessionId;
export const MessageId      = (value: string) => value as MessageId;
export const AppointmentId  = (value: string) => value as AppointmentId;
export const CatalogItemId  = (value: string) => value as CatalogItemId;
export const EventId        = (value: string) => value as EventId;
export const JobId          = (value: string) => value as JobId;

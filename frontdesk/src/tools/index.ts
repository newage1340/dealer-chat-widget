/**
 * The tools the model acts through. Each records facts (events, contact changes, a call directive);
 * nothing here sends, books or writes anything itself.
 *
 * `toolsFor` picks the set for a turn: some only make sense on calls, some only when the pack or tenant
 * has something to offer (links), and the booking tool's parameters come from the pack's intake fields.
 */

import { jsonSchemaOf, type ToolDefinition } from "../providers/index.ts";
import type { Tool, TurnContext } from "../turn/index.ts";
import { bookAppointment } from "./booking.ts";
import { priceItems, searchCatalog } from "./catalog.ts";
import { saveCustomerDetails } from "./contact.ts";
import {
    endCall,
    flagConversation,
    requestCallback,
    switchLanguage,
    transferCall,
} from "./conversation.ts";
import { linkOptions, sendLink } from "./links.ts";

/**
 * Every tool name, for checks that prompt text only mentions tools that exist.
 */
export const TOOL_NAMES: ReadonlySet<string> = new Set([
    "save_customer_details", "book_appointment", "send_link", "search_catalog", "price_items",
    "request_callback", "switch_language", "flag_conversation", "transfer_call", "end_call",
]);

export function toolsFor(ctx: TurnContext): Tool[] {
    const voice = ctx.inbound.channel === "voice";
    // The same tools, in the same order, for every tenant of a type on a channel: tool definitions are part of the cached prefix.
    return [
        saveCustomerDetails(ctx),
        bookAppointment(ctx),
        sendLink(ctx, linkOptions(ctx)),
        searchCatalog(ctx),
        priceItems(ctx),
        requestCallback(),
        switchLanguage(ctx),
        flagConversation(),
        ...(voice ? [transferCall(ctx), endCall()] : []),
    ];
}

export function definitions(tools: readonly Tool[]): ToolDefinition[] {
    return tools.map(t => ({ name: t.name, description: t.description, parameters: jsonSchemaOf(t.parameters) }));
}

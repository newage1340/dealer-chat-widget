/**
 * Recording what customers say about themselves.
 */

import { z } from "zod";

import { Address, PhoneNumber, type ContactChanges, type Mutable } from "../core/index.ts";
import { Tool, ToolResult, type TurnContext } from "../turn/index.ts";

export function firstWord(name: string): string {
    return name.trim().split(/\s+/)[0] ?? "";
}

/**
 * Every field the tool can take; `phone` is only offered on the web, where we don't already have the number.
 */
const CustomerDetails = z.object({
    first_name: z.string().optional().describe("First name only"),
    last_name:  z.string().optional(),
    email:      z.string().optional(),
    phone:      z.string().optional().describe("Their phone number, if they gave one"),
});

export function saveCustomerDetails(ctx: TurnContext): Tool {
    const web = ctx.inbound.channel === "web";
    return Tool.define({
        name:        "save_customer_details",
        description: "Record details the customer just told you about themselves. Include only what they said this turn.",
        followUp:    false,
        parameters:  web
            ? CustomerDetails
            : CustomerDetails.omit({ phone: true }),
        run: (args: z.output<typeof CustomerDetails>) => {
            const changes: Mutable<ContactChanges> = {};
            const problems: string[] = [];
            if (args.first_name?.trim()) {
                changes.firstName = firstWord(args.first_name);
            }
            if (args.last_name?.trim()) {
                changes.lastName = args.last_name.trim();
            }
            if (args.email?.trim()) {
                const email = Address.email(args.email);
                if (email === null) {
                    problems.push(`'${args.email}' is not a valid email address`);
                } else {
                    changes.email = email.value;
                }
            }
            if (args.phone?.trim()) {
                const phone = PhoneNumber.parse(args.phone);
                if (phone === null) {
                    problems.push(`'${args.phone}' is not a valid phone number`);
                } else {
                    changes.phone = phone;
                }
            }
            if (problems.length > 0 && Object.keys(changes).length === 0) {
                return ToolResult.error(problems.join("; "));
            }
            return ToolResult.output(
                { saved: Object.keys(changes), problems },
                {
                    contactChanges: changes,
                    events:         Object.keys(changes).length > 0
                        ? [{ kind: "contact_updated", contactId: ctx.contact.id, changes }]
                        : [],
                },
            );
        },
    });
}

/**
 * Sending real links (catalog pages, the tenant's own links) instead of letting the model write URLs.
 */

import { z } from "zod";

import { Catalog } from "../domains/index.ts";
import { Tool, ToolResult, type TurnContext } from "../turn/index.ts";

export type LinkOption =
    | { readonly kind: "catalog";
		readonly label: string;
		readonly field: string
	}
    | { readonly kind: "tenant";
		readonly label: string;
		readonly url: string
	};

export function linkOptions(ctx: TurnContext): LinkOption[] {
    return [
        ...ctx.pack.catalog.sendables.map((s): LinkOption => ({ kind: "catalog", label: s.label, field: s.field })),
        ...ctx.tenant.settings.links.map((l): LinkOption => ({ kind: "tenant", label: l.label, url: l.url })),
    ];
}

/**
 * The link names are listed in the prompt, not in the schema, so every tenant of a type shares one tool definition
 * (and the prompt cache that covers it).
 */
export function sendLink(ctx: TurnContext, options: readonly LinkOption[]): Tool {
    const spec = ctx.pack.catalog;
    return Tool.define({
        name:        "send_link",
        promise:     /\b(send|text)(ing)?\b[^.!?]{0,40}\b(link|listing|photos?|pictures?|page|report)\b/i,
        description: "Send the customer a real link. Say you're sending it; never write the URL yourself.",
        followUp:    false,
        parameters:  z.object({
            link: z.string().describe("Which link: one of the names under LINKS YOU CAN SEND"),
            item: z.string().optional().describe(`For ${spec.itemNoun} links: which ${spec.itemNoun}, by name`),
        }),
        run: args => {
            const option = options.find(o => o.label.toLowerCase() === args.link.trim().toLowerCase());
            if (option === undefined) {
                return ToolResult.error(options.length === 0
                    ? "This business has no links to send. Give the information directly instead."
                    : `There's no link called '${args.link}'. The links are: ${options.map(o => o.label).join(", ")}.`);
            }
            let url: string;
            let label: string;
            if (option.kind === "tenant") {
                url = option.url;
                label = option.label;
            } else {
                const history = [...ctx.history].reverse().map(m => m.text);
                const item = (args.item ? Catalog.match(ctx.catalog, args.item, spec, 1)[0] : undefined)
                    ?? Catalog.anchor([ctx.inbound.text, ...history.slice(0, 4)], ctx.catalog, spec)
                    ?? Catalog.match(ctx.catalog, ctx.inbound.text, spec, 1)[0];
                if (item === undefined) {
                    return ToolResult.error(`Which ${spec.itemNoun}? Ask the customer, then call send_link with the item.`);
                }
                const value = item.fields[option.field];
                if (value === null || value === undefined || value === "") {
                    return ToolResult.error(`There's no ${option.label} for the ${Catalog.title(item, spec)}.`);
                }
                url = String(value);
                label = `${option.label} for the ${Catalog.title(item, spec)}`;
            }
            if (ctx.session.sentUrls.includes(url)) {
                return ToolResult.output({ alreadySent: label });
            }
            return ToolResult.output(
                { sending: label },
                {
                    events:   [{ kind: "link_requested", to: ctx.inbound.sender, label, url }],
                    sentUrls: [url],
                },
            );
        },
    });
}

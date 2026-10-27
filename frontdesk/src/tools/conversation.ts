/**
 * Steering the conversation itself: language, flags, handoffs to staff, and call control.
 */

import { z } from "zod";

import { Language, LANGUAGE_NAMES, Tenant, TurnAssessment } from "../core/index.ts";
import { Tool, ToolResult, type TurnContext } from "../turn/index.ts";

/**
 * The schema lists every language so all tenants share one definition; the tenant's offered languages
 * are in the prompt, and asking for another one gets an error the model passes on politely.
 */
export function switchLanguage(ctx: TurnContext): Tool {
    return Tool.define({
        name:        "switch_language",
        description: "The customer wants to (or started to) talk in another language we offer (see LANGUAGE). Continue in it, completely, from this reply on.",
        followUp:    false,
        parameters:  z.object({ language: z.enum(Language).describe(Object.values(Language).map(l => `${l}=${LANGUAGE_NAMES[l]}`).join(", ")) }),
        run: args => {
            if (!Tenant.offers(ctx.tenant, args.language)) {
                const offered = ctx.tenant.settings.languages.filter(l => Tenant.offers(ctx.tenant, l)).map(l => LANGUAGE_NAMES[l]);
                return ToolResult.error(`We don't offer ${LANGUAGE_NAMES[args.language]}. Say kindly that we can help in ${offered.join(" or ")}, and continue in ${LANGUAGE_NAMES[ctx.tenant.defaultLanguage]}.`);
            }
            return ToolResult.output({ language: args.language }, { language: args.language });
        },
    });
}

export function flagConversation(): Tool {
    return Tool.define({
        name:        "flag_conversation",
        description: "This turn was clearly not a real customer request (prank, nonsense, trolling) or was abusive.",
        followUp:    false,
        parameters:  z.object({
            kind: z.enum([TurnAssessment.OffTopic, TurnAssessment.Abusive]),
            note: z.string().describe("A few words on what happened"),
        }),
        run: args => ToolResult.output(
            { flagged: args.kind },
            {
                assessment: args.kind,
                events:     [{ kind: "turn_flagged", assessment: args.kind, note: args.note }],
            },
        ),
    });
}

export function requestCallback(): Tool {
    return Tool.define({
        name:        "request_callback",
        promise:     /\b(have someone (call|reach|get back)|someone will (call|reach|get back)|pass (this|that|it) (along|on)|let the team know)\b/i,
        description: "The customer wants someone from the team to get back to them. The team is notified right away.",
        followUp:    false,
        parameters:  z.object({ note: z.string().describe("What they want and anything the team should know, in one or two sentences") }),
        run: args => ToolResult.output(
            { notified: true },
            { events: [{ kind: "handoff_requested", reason: "take_message", note: args.note }] },
        ),
    });
}

export function transferCall(ctx: TurnContext): Tool {
    return Tool.define({
        name:        "transfer_call",
        promise:     /\b(connect(ing)? you|transfer(ring)? you|put(ting)? you through|get(ting)? you (to|over to) (someone|a (live|real) person))\b/i,
        description: "Connect the caller to a live person now (emergency, safety, or they insist on a person).",
        followUp:    false,
        parameters:  z.object({ reason: z.string().describe("Why, in a few words") }),
        run: args => {
            const target = ctx.tenant.settings.transferTo ?? ctx.tenant.phone;
            if (target === null || ctx.inbound.forwardedFrom === target) {
                return ToolResult.error("Nobody can take a live call right now. Apologize, take a message with request_callback, and tell them the team will call back as soon as possible.");
            }
            return ToolResult.output(
                { transferring: true },
                {
                    directive: { kind: "transfer", to: target },
                    events:    [{ kind: "handoff_requested", reason: "transfer", note: args.reason }],
                },
            );
        },
    });
}

export function endCall(): Tool {
    return Tool.define({
        name:        "end_call",
        description: "Hang up after this reply. Only when the conversation is over and you've said goodbye.",
        followUp:    false,
        parameters:  z.object({}),
        run: () => ToolResult.output({ ending: true }, { directive: { kind: "end" } }),
    });
}

/**
 * The unit of capability: something the model can do during a turn.
 *
 * A tool is a schema (what the model must supply) and a function (what it means).
 * It returns output for the model plus the facts it established;
 * it never sends, books or writes anything itself.
 * Domain packs choose which tools a tenant gets, and can shape their schemas
 * (e.g. the booking tool requires the pack's intake fields).
 */

import type { z } from "zod";

import type { CallDirective, ContactChanges, EventBody, Language, TurnAssessment } from "../core/index.ts";
import type { TurnContext } from "./context.ts";

/**
 * `output` is returned to the model as the tool result (JSON-serializable).
 * `directive` asks the call to transfer or end once the reply has been spoken; null leaves it alone.
 * `assessment` and `language` let a tool report on the turn (flag_conversation, switch_language);
 * null leaves them alone.
 * `failed` results always go back to the model, so it can fix the problem or tell the customer.
 */
export type ToolResult = {
    readonly output:         unknown;
    readonly failed:         boolean;
    readonly events:         readonly EventBody[];
    readonly contactChanges: ContactChanges;
    readonly sentUrls:       readonly string[];
    readonly directive:      CallDirective | null;
    readonly assessment:     TurnAssessment | null;
    readonly language:       Language | null;
};

/**
 * `followUp` tools return information the model needs before it can reply (a search, a booking check),
 * so the model is called again with the result. Other tools just record something.
 * `promise` matches words that claim the tool's action ("connecting you now"); a reply that says them
 * without calling the tool gets one corrective round, because words alone do nothing.
 */
export interface Tool<Schema extends z.ZodType = z.ZodType> {
    readonly name:        string;
    readonly description: string;
    readonly parameters:  Schema;
    readonly followUp:    boolean;
    readonly promise?:    RegExp;
    run(args: z.output<Schema>, ctx: TurnContext): ToolResult | Promise<ToolResult>;
}

export const Tool = {
    /**
     * Identity function that lets TypeScript infer `args` from `parameters`.
     */
    define<Schema extends z.ZodType>(tool: Tool<Schema>): Tool<Schema> {
        return tool;
    },
} as const;

export const ToolResult = {
    output(output: unknown, extra: Partial<Omit<ToolResult, "output" | "failed">> = {}): ToolResult {
        return {
            output,
            failed:         false,
            events:         extra.events ?? [],
            contactChanges: extra.contactChanges ?? {},
            sentUrls:       extra.sentUrls ?? [],
            directive:      extra.directive ?? null,
            assessment:     extra.assessment ?? null,
            language:       extra.language ?? null,
        };
    },

    /**
     * A failure the model should see and act on (e.g. "that day is closed").
     */
    error(message: string): ToolResult {
        return { ...ToolResult.output({ error: message }), failed: true };
    },
} as const;

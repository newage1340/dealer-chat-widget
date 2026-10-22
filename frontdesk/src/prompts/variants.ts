/**
 * The prompt A/B test (variants/variants.json): which variants exist, how conversations are split
 * between them, and the base templates each one uses.
 */

import { readFile } from "node:fs/promises";

import { z } from "zod";

import { CONTROL_VARIANT, type PromptVariant } from "../core/index.ts";

const VARIANTS_DIR = new URL("./variants/", import.meta.url);

/**
 * The two universal base templates, by the file name a variant uses to replace one.
 */
export const BASE_TEMPLATE_FILES = {
    chat:  "base_chat.md",
    voice: "base_voice.md",
} as const;

export type BaseTemplates = {
    readonly chat:  string;
    readonly voice: string;
};

const VariantFile = z.strictObject({
    _comment:   z.string().optional(),
    experiment: z.string().min(1),
    variants:   z.array(z.strictObject({
        id:          z.string().regex(/^[a-z0-9][a-z0-9_-]*$/),
        weight:      z.number().min(0),
        description: z.string().default(""),
        templates:   z.partialRecord(z.enum(Object.values(BASE_TEMPLATE_FILES)), z.string()).default({}),
        temperature: z.number().min(0).max(1).nullable().default(null),
        tier:        z.enum(["fast", "capable"]).nullable().default(null),
    })).min(1),
});

/**
 * `templates` holds each variant's base templates (its own files, else the standard ones).
 */
export type Experiment = {
    readonly name:      string;
    readonly variants:  readonly PromptVariant[];
    readonly templates: ReadonlyMap<string, BaseTemplates>;
};

export const Experiment = {
    /**
     * No experiment: everyone gets the control variant and the standard templates.
     */
    none(base: BaseTemplates): Experiment {
        return { name: "none", variants: [CONTROL_VARIANT], templates: new Map([[CONTROL_VARIANT.id, base]]) };
    },

    async load(base: BaseTemplates): Promise<Experiment> {
        const file = VariantFile.parse(JSON.parse(await readFile(new URL("variants.json", VARIANTS_DIR), "utf8")));
        const templates = new Map<string, BaseTemplates>();
        for (const v of file.variants) {
            const own = async (kind: keyof BaseTemplates) => {
                const name = v.templates[BASE_TEMPLATE_FILES[kind]];
                return name === undefined
                    ? base[kind]
                    : await readFile(new URL(name, VARIANTS_DIR), "utf8");
            };
            templates.set(v.id, { chat: await own("chat"), voice: await own("voice") });
        }
        return { name: file.experiment, variants: file.variants, templates };
    },
} as const;

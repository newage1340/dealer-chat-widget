/**
 * Generating new domain packs by prompting a model with the authoring "prompt leader".
 *
 * The leader (authoring_prompt.md) carries the template shape and a complete example pack.
 * The model's TOML is validated with the same loader the app uses; validation problems are
 * fed back for one repair attempt.
 */

import { access, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { LlmProvider, type ChatMessage } from "../providers/index.ts";
import { DomainPackError, PACKS_DIR, parsePackToml } from "./load.ts";
import type { DomainPack } from "./pack.ts";

const LEADER = new URL("./authoring_prompt.md", import.meta.url);
const EXAMPLE_PACK = "plumbing.toml";

export async function authoringPrompt(businessType: string, extraContext = ""): Promise<string> {
    const leader = await readFile(LEADER, "utf8");
    return leader
        .replace("{business_type}", businessType)
        .replace("{extra_context}", extraContext ? `Additional context from the operator: ${extraContext}` : "")
        .replace("{template}", await readFile(join(PACKS_DIR, "_template.toml"), "utf8"))
        .replace("{example}", await readFile(join(PACKS_DIR, EXAMPLE_PACK), "utf8"));
}

function stripFence(text: string): string {
    const m = /```(?:toml)?\s*\n([\s\S]*?)```/.exec(text);
    return `${(m?.[1] ?? text).trim()}\n`;
}

/**
 * Ask the model for a pack. Returns the TOML text and the parsed pack, or throws DomainPackError.
 */
export async function generatePack(
    llm: LlmProvider,
    businessType: string,
    extraContext = "",
    attempts = 2
): Promise<{ text: string; pack: DomainPack }>
{
    const messages: ChatMessage[] = [{ role: "user", text: await authoringPrompt(businessType, extraContext) }];
    let problems: { path: string; message: string }[] = [];
    for (let i = 0; i < attempts; i++) {
        const response = await LlmProvider.complete(llm, {
            messages,
            tools:       [],
            format:      { kind: "text" },
            tier:        "capable",
            maxTokens:   8000,
            temperature: 0.4,
            timeoutMs:   180_000,
            purpose:     "authoring",
            tenantId:    null,
            channel:     null,
        });
        const text = stripFence(response.text);
        const result = parsePackToml(text);
        if (result.ok) {
            return { text, pack: result.value };
        }
        problems = result.error;
        messages.push(
            { role: "assistant", text, toolCalls: [] },
            { role: "user", text: `That pack failed validation:\n${problems.map(p => `- ${p.path}: ${p.message}`).join("\n")}\nReturn the corrected complete TOML document only.` },
        );
    }
    throw new DomainPackError("<generated>", problems);
}

export async function savePack(text: string, pack: DomainPack, overwrite = false): Promise<string> {
    const path = join(PACKS_DIR, `${pack.id}.toml`);
    const exists = await access(path).then(() => true, () => false);
    if (exists && !overwrite) {
        throw new Error(`${path} already exists`);
    }
    await writeFile(path, text, "utf8");
    return path;
}

/**
 * `{placeholder}` templates used throughout pack text.
 *
 * The syntax matches the Python version, so existing packs load unchanged:
 * `{name}` is replaced, `{{` and `}}` are literal braces.
 * Prompt text may use the fields below plus `setting_<key>` and `profile_<key>`,
 * which resolve to the tenant's settings and onboarding answers.
 */

import { Result } from "../core/index.ts";

export const PROMPT_FIELDS: ReadonlySet<string> = new Set([
    "business_name",
    "business_noun",
    "agent_name",
    "agent_role",
    "staff_noun",
    "item_noun",
    "item_noun_plural",
    "spoken_item_noun",
    "visit_noun",
    "general_subject",
    "subject_label",
    "phone",
    "address",
    "website",
]);

/**
 * Extra fields available only to follow-up texts.
 */
export const FOLLOWUP_FIELDS: ReadonlySet<string> = new Set([
    "first_name",
    "visit_time",
    "subject",
]);

const OPEN_PREFIXES = ["setting_", "profile_"] as const;

type Token =
    | { readonly kind: "text";
		readonly text: string
	}
    | { readonly kind: "field";
		readonly name: string
	};

function tokenize(template: string): Result<Token[], string> {
    const tokens: Token[] = [];
    let text = "";
    let i = 0;
    while (i < template.length) {
        const c = template[i]!;
        const next = template[i + 1];
        if ((c === "{" && next === "{") || (c === "}" && next === "}")) {
            text += c;
            i += 2;
            continue;
        }
        if (c === "}") {
            return Result.err("single '}' (write a literal brace as '}}')");
        }
        if (c === "{") {
            const end = template.indexOf("}", i);
            if (end === -1) {
                return Result.err("unclosed '{' (write a literal brace as '{{')");
            }
            const name = template.slice(i + 1, end).trim();
            if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
                return Result.err(`bad placeholder '{${name}}'`);
            }
            if (text) {
                tokens.push({ kind: "text", text });
                text = "";
            }
            tokens.push({ kind: "field", name });
            i = end + 1;
            continue;
        }
        text += c;
        i += 1;
    }
    if (text) {
        tokens.push({ kind: "text", text });
    }
    return Result.ok(tokens);
}

export const Template = {
    /**
     * The placeholder names a template uses, or why it cannot be parsed.
     */
    fields(template: string): Result<string[], string> {
        return Result.map(
            tokenize(template),
            tokens => tokens.flatMap(t => t.kind === "field" ? [t.name] : [])
        );
    },

    /**
     * Problems with a piece of prompt text: unparseable braces or unknown placeholders.
     */
    problems(template: string, extra: ReadonlySet<string> = new Set()): string[] {
        const fields = Template.fields(template);
        if (!fields.ok) {
            return [fields.error];
        }
        return fields.value
            .filter(name =>
                !PROMPT_FIELDS.has(name)
                && !extra.has(name)
                && !OPEN_PREFIXES.some(p => name.startsWith(p))
            )
            .map(name => `unknown placeholder {${name}}`);
    },

    /**
     * Fill a template. Missing values render as empty strings;
     * validation at load time is what guarantees the names are real.
     */
    render(template: string, values: Readonly<Record<string, string>>): string {
        const tokens = tokenize(template);
        if (!tokens.ok) {
            return template;
        }
        return tokens.value
            .map(t => t.kind === "text"
                ? t.text
                : values[t.name] ?? "")
            .join("");
    },
} as const;

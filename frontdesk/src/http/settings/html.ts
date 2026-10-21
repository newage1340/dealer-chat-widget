/**
 * Escaped HTML templates and plain form controls for the settings pages.
 *
 * The markup is deliberately bare: semantic elements, no classes, no inline styles, no scripts,
 * so whoever styles it starts from a clean slate (public/settings/settings.css is the hook).
 */

export type Html = { readonly html: string };

export function raw(text: string): Html {
    return { html: text };
}

function escape(text: string): string {
    return text
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
}

function render(value: unknown): string {
    if (value === null || value === undefined || value === false) {
        return "";
    }
    if (Array.isArray(value)) {
        return value.map(render).join("");
    }
    if (typeof value === "object" && "html" in value) {
        return (value as Html).html;
    }
    return escape(String(value));
}

/**
 * A template whose interpolations are escaped, except nested `html` / `raw` values. Arrays are joined.
 */
export function html(strings: TemplateStringsArray, ...values: unknown[]): Html {
    let out = strings[0] ?? "";
    values.forEach((value, i) => {
        out += render(value) + (strings[i + 1] ?? "");
    });
    return raw(out);
}

export function page(title: string, body: Html): string {
    return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escape(title)}</title>
<link rel="stylesheet" href="/settings/static/settings.css">
</head>
<body>
${body.html}
</body>
</html>
`;
}

// --- form controls ---------------------------------------------------------------------

/**
 * A labelled control. Labels are bold; text boxes start on the line below their label,
 * with the help text in a block to their right.
 */
export function field(label: string, control: Html, help = ""): Html {
    if (control.html.startsWith("<textarea")) {
        if (!help) {
            return html`<p><label><strong>${label}</strong><br>${control}</label></p>\n`;
        }
        // A two-cell table keeps the help text beside the box as one block, without CSS.
        const id = /\sid="([^"]*)"/.exec(control.html)?.[1] ?? "";
        return html`<div><p><label for="${id}"><strong>${label}</strong></label></p>
<table role="presentation"><tr><td>${control}</td><td><small>${help}</small></td></tr></table></div>\n`;
    }
    return html`<p><label><strong>${label}</strong> ${control}</label>${help ? html` <small>${help}</small>` : ""}</p>\n`;
}

export function input(name: string, value: string | number | null | undefined, type = "text", extra = ""): Html {
    return html`<input type="${type}" name="${name}" value="${value ?? ""}"${raw(extra ? ` ${extra}` : "")}>`;
}

export function textarea(name: string, value: string, rows = 4): Html {
    return html`<textarea id="field-${name}" name="${name}" rows="${rows}" cols="60">${value}</textarea>`;
}

export function select(name: string, options: readonly (readonly [string, string])[], selected: string): Html {
    return html`<select name="${name}">${options.map(([value, label]) =>
        html`<option value="${value}"${raw(value === selected ? " selected" : "")}>${label}</option>`)}</select>`;
}

export function checkbox(name: string, checked: boolean, value = "on"): Html {
    return html`<input type="checkbox" name="${name}" value="${value}"${raw(checked ? " checked" : "")}>`;
}

export function check(label: string, name: string, checked: boolean, value = "on"): Html {
    return html`<label>${checkbox(name, checked, value)} ${label}</label>\n`;
}

/**
 * A form that posts to `action`, with a submit button.
 */
export function form(action: string, body: Html, submit = "Save"): Html {
    return html`<form method="post" action="${action}">\n${body}<p><button type="submit">${submit}</button></p>\n</form>\n`;
}

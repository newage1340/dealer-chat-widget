/**
 * Small string helpers shared by prompts, staff notices and pages.
 */

export const Text = {
    /**
     * "oil change" -> "Oil change"
     */
    capitalize(text: string): string {
        return text.charAt(0).toUpperCase() + text.slice(1);
    },
} as const;

/**
 * Something wrong with a file or form, found while checking it.
 */

/**
 * `path` is dotted ("catalog.fields.2.name"), or "" when the problem is with the whole thing.
 */
export type Problem = {
    readonly path:    string;
    readonly message: string;
};

export const Problem = {
    /**
     * Problems from a failed zod parse (`error.issues`).
     */
    fromIssues(issues: readonly { readonly path: readonly PropertyKey[]; readonly message: string }[]): Problem[] {
        return issues.map(issue => ({
            path:    issue.path.map(String).join("."),
            message: issue.message,
        }));
    },

    /**
     * "settings.calls.silenceSeconds: Too big"; the message alone when the problem has no path.
     */
    format(problem: Problem): string {
        return problem.path
            ? `${problem.path}: ${problem.message}`
            : problem.message;
    },
} as const;

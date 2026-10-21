import { defineConfig } from "vitest/config";

// Each test file starts its own in-process Postgres (PGlite); the first test in a file pays for that.
export default defineConfig({
    test: {
        testTimeout: 30_000,
    },
});

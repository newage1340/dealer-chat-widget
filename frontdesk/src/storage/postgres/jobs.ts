/**
 * The job queue: claimed with a lease (a crashed worker's jobs come back), retried with backoff by the worker.
 */

import type { Db } from "../db.ts";
import type { JobQueue } from "../ports.ts";
import { toJob } from "./rows.ts";
import { insertJobs } from "./writes.ts";

export function jobQueue(db: Db): JobQueue {
    return {
        enqueue: jobs => db.transaction(tx => insertJobs(tx, jobs)),

        claim: async (limit, leaseMs, now) => (await db.query(
            `update jobs set status = 'running', attempts = attempts + 1, lease_until = $3
             where id in (
                 select id from jobs
                 where (status = 'pending' and (run_at is null or run_at <= $2))
                    or (status = 'running' and lease_until < $2)
                 order by coalesce(run_at, created_at)
                 limit $1
                 for update skip locked
             )
             returning *`,
            [limit, now, new Date(now.getTime() + leaseMs)],
        )).map(toJob),

        complete: async id => {
            await db.query("update jobs set status = 'done', lease_until = null where id = $1", [id]);
        },

        fail: async (id, error, retryAt) => {
            await db.query(
                `update jobs set status = $2, run_at = coalesce($3, run_at), last_error = $4, lease_until = null
                 where id = $1`,
                [id, retryAt === null ? "failed" : "pending", retryAt, error.slice(0, 2000)],
            );
        },
    };
}

/**
 * Database schema, as ordered migrations. Each runs once, inside a transaction.
 * Add new migrations at the end; never edit one that has shipped.
 */

import type { Db } from "./db.ts";

const MIGRATIONS: readonly { readonly version: number; readonly sql: string }[] = [
    {
        version: 1,
        sql: `
create table tenants (
    id          text primary key,
    slug        text not null unique,
    data        jsonb not null,
    updated_at  timestamptz not null default now()
);

create table tenant_endpoints (
    channel     text not null,
    address     text not null,
    tenant_id   text not null references tenants(id) on delete cascade,
    primary key (channel, address)
);

create table catalog_items (
    tenant_id   text not null references tenants(id) on delete cascade,
    id          text not null,
    position    integer not null,
    fields      jsonb not null,
    updated_at  timestamptz not null,
    primary key (tenant_id, id)
);

create table contacts (
    id                  text primary key,
    tenant_id           text not null,
    first_name          text not null default '',
    last_name           text not null default '',
    email               text not null default '',
    phone               text,
    opted_out           boolean not null default false,
    terms_accepted_at   timestamptz,
    preferred_language  text,
    block_until         timestamptz,
    block_source        text,
    block_reason        text,
    block_created_at    timestamptz,
    abuse_strikes       jsonb not null default '[]',
    attributes          jsonb not null default '{}',
    created_at          timestamptz not null
);
create index contacts_tenant on contacts (tenant_id);

create table contact_addresses (
    tenant_id   text not null,
    address     text not null,
    kind        text not null,
    value       text not null,
    contact_id  text not null references contacts(id) on delete cascade deferrable initially deferred,
    primary key (tenant_id, address)
);
create index contact_addresses_contact on contact_addresses (contact_id);

create table conversations (
    id               text primary key,
    tenant_id        text not null,
    contact_id       text not null unique references contacts(id) on delete cascade,
    created_at       timestamptz not null,
    last_message_at  timestamptz
);

create table sessions (
    id                text primary key,
    conversation_id   text not null references conversations(id) on delete cascade,
    tenant_id         text not null,
    channel           text not null,
    external_id       text,
    vendor            text,
    handling          text,
    call              jsonb,
    language          text not null,
    meter             jsonb not null,
    sent_urls         jsonb not null default '[]',
    started_at        timestamptz not null,
    last_activity_at  timestamptz not null,
    ended_at          timestamptz
);
create unique index sessions_open_external on sessions (tenant_id, channel, external_id)
    where ended_at is null and external_id is not null;
create index sessions_conversation on sessions (conversation_id, started_at desc);
create index sessions_open on sessions (channel, last_activity_at) where ended_at is null;

create table messages (
    id               text primary key,
    conversation_id  text not null references conversations(id) on delete cascade,
    session_id       text not null references sessions(id) on delete cascade,
    role             text not null,
    text             text not null,
    channel          text not null,
    created_at       timestamptz not null
);
create index messages_conversation on messages (conversation_id, created_at);
create index messages_session on messages (session_id, created_at);

create table appointments (
    id               text primary key,
    tenant_id        text not null,
    contact_id       text not null references contacts(id) on delete cascade,
    conversation_id  text not null references conversations(id) on delete cascade,
    time_text        text not null,
    starts_at        timestamptz,
    subject          text not null,
    location         text not null default '',
    details          jsonb not null default '{}',
    item_ids         jsonb not null default '[]',
    status           text not null,
    sent_notices     jsonb not null default '[]',
    created_at       timestamptz not null
);
create index appointments_conversation on appointments (conversation_id, created_at desc);

create table turns (
    dedupe_key  text primary key,
    reply       text not null,
    session_id  text not null,
    created_at  timestamptz not null
);

create table events (
    id               text primary key,
    tenant_id        text not null,
    conversation_id  text references conversations(id) on delete cascade,
    session_id       text,
    kind             text not null,
    body             jsonb not null,
    occurred_at      timestamptz not null,
    caused_by        text
);
create index events_tenant on events (tenant_id, occurred_at desc);

create table jobs (
    id           text primary key,
    tenant_id    text not null,
    kind         text not null,
    body         jsonb not null,
    dedupe_key   text not null unique,
    status       text not null default 'pending',
    run_at       timestamptz,
    attempts     integer not null default 0,
    lease_until  timestamptz,
    last_error   text,
    created_at   timestamptz not null default now()
);
create index jobs_ready on jobs (status, run_at);

create table classifications (
    session_id  text primary key references sessions(id) on delete cascade,
    outcome     text not null,
    data        jsonb not null
);

create table voicemails (
    session_id        text primary key references sessions(id) on delete cascade,
    tenant_id         text not null,
    conversation_id   text not null,
    sender            jsonb not null,
    recording_url     text not null,
    duration_seconds  integer not null,
    received_at       timestamptz not null
);

create table usage (
    id           bigserial primary key,
    provider     text not null,
    model        text not null,
    service      text not null,
    purpose      text not null,
    occurred_at  timestamptz not null,
    units        jsonb not null,
    cost_usd     double precision,
    latency_ms   double precision,
    tenant_id    text,
    channel      text
);
create index usage_tenant on usage (tenant_id, occurred_at);
`,
    },
    {
        version: 2,
        sql: `
create table catalog_syncs (
    tenant_id   text primary key references tenants(id) on delete cascade,
    synced_at   timestamptz not null,
    items       integer not null,
    error       text
);
`,
    },
    {
        version: 3,
        sql: `
create table turn_metrics (
    id             bigserial primary key,
    tenant_id      text not null,
    domain         text not null,
    session_id     text not null,
    channel        text not null,
    variant        text not null,
    model          text not null,
    occurred_at    timestamptz not null,
    latency_ms     integer not null,
    first_text_ms  integer,
    rounds         integer not null,
    tools          jsonb not null,
    tool_failures  integer not null,
    corrected      boolean not null,
    fallback       boolean not null,
    input_tokens   integer not null,
    cached_tokens  integer not null,
    output_tokens  integer not null,
    cost_usd       double precision not null,
    reply_chars    integer not null,
    language       text not null,
    switched       boolean not null
);
create index turn_metrics_time on turn_metrics (occurred_at);
create index turn_metrics_session on turn_metrics (session_id);

create table eval_runs (
    id          bigserial primary key,
    run_id      text not null,
    scenario    text not null,
    variant     text not null,
    model       text not null,
    passed      boolean not null,
    failures    jsonb not null,
    cost_usd    double precision not null,
    ran_at      timestamptz not null
);
create index eval_runs_scenario on eval_runs (scenario, ran_at);
`,
    },
    {
        version: 4,
        sql: `
create table accounts (
    id             text primary key,
    tenant_id      text not null references tenants(id) on delete cascade,
    email          text not null unique,
    name           text not null default '',
    password_hash  text not null,
    created_at     timestamptz not null,
    last_login_at  timestamptz
);
create index accounts_tenant on accounts (tenant_id);

create table onboarding_submissions (
    tenant_id     text primary key references tenants(id) on delete cascade,
    submitted_at  timestamptz not null,
    reviewed_at   timestamptz
);
`,
    },
];

export async function migrate(db: Db): Promise<number> {
    await db.exec(`create table if not exists schema_migrations (
        version     integer primary key,
        applied_at  timestamptz not null default now()
    )`);
    const rows = await db.query<{ version: number }>("select version from schema_migrations");
    const applied = new Set(rows.map(r => Number(r.version)));
    let count = 0;
    for (const m of MIGRATIONS) {
        if (applied.has(m.version)) {
            continue;
        }
        await db.transaction(async tx => {
            await tx.exec(m.sql);
            await tx.query("insert into schema_migrations (version) values ($1)", [m.version]);
        });
        count++;
    }
    return count;
}

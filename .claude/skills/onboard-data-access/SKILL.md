---
name: onboard-data-access
description: Add a new data plane (database access layer) to this platform, change an existing plane's table allowlist or row cap, or bring data-access code up to repo standard. Use when asked to "add a data plane", "give the agent a new database", "let the tool read table X", "expose a new database", "add a db-access service", or when debugging plane 400/403/404/503 errors. Covers AgentDataPlane, the SQL guard semantics, RLS session variables, seed SQL, compose wiring, and verification. For the tools that call the plane, see the onboard-mcp skill.
---

# Onboarding a data plane

Full tutorial: [docs/onboarding-new-agent-and-mcp.md](../../../docs/onboarding-new-agent-and-mcp.md)
§4 — a worked `finance` example. **`finance` is teaching material and does not
exist in the repo.** Real planes are `world-db-access` and
`procurement-db-access`. Design rationale:
[docs/agent-services.md](../../../docs/agent-services.md).

## What a data plane is for

A plane is the **only** thing on the platform holding database credentials. One
plane owns exactly one database, so a fault or compromise in one domain cannot
reach another's data. MCP servers are credential-free and delegate every read
here; the plane applies the authoritative guard.

**There is almost no code to write.** You declare an `AgentDataPlane` and
`create_data_access_app` builds the entire service. If you find yourself writing
a route, a query function, or a connection pool, stop — you're rebuilding the
runtime.

## Step 0 — Do you actually need one?

Skip if:

- the tools read an **existing** database → reuse `world-db-access` /
  `procurement-db-access`; nothing to build.
- the tools read **no** database (`report-mcp`) → no plane at all.
- you only need one more table from a database that already has a plane → this
  is a one-line change to that plane's `allowed_tables` frozenset. Make it
  deliberately (it widens the blast radius of every tool on that database), then
  jump to Step 4.

Naming: service `<domain>-db-access`, `database` field is the short domain word
(`world`, not `world_db`), DB name snake_case (`finance_db`), env var
`<DOMAIN>_DATABASE_URL`. Pick a free host port from `docker-compose.yml`.

## Step 1 — Seed SQL

`docker/postgres/init/0X-create-<name>-database.sql`, mirroring
`02-create-procurement-database.sql`: idempotent `create database` via `\gexec`,
`\connect`, `create table if not exists`, demo rows with
`on conflict ... do update`.

## Step 2 — Declare the plane

```
apps/data_access/<name>/__init__.py     (empty)
apps/data_access/<name>/main.py
```

```python
from apps.data_access.runtime import AgentDataPlane, create_data_access_app

FINANCE_DB_TABLES = frozenset({"invoices"})

DEFINITION = AgentDataPlane(
    database="finance",                   # the value callers must send in body.database
    service_name="finance-db-access",     # appears in traces and /health
    allowed_tables=FINANCE_DB_TABLES,     # SQL touching anything else -> 403
    url_env=("FINANCE_DATABASE_URL",),    # candidate env vars, tried in order
    title="Finance Database Access Layer",
    # max_rows=500 is the default cap; override only with a stated reason.
)

app = create_data_access_app(DEFINITION)
```

Field notes ([apps/data_access/runtime.py](../../../apps/data_access/runtime.py)):

- `url_env` is a **fallback chain** — `resolve_database_url` returns the first
  non-empty env var. `world` uses `("WORLD_DATABASE_URL", "DATABASE_URL")` so
  the generic var still works. A new plane should normally list only its own.
- If **none** of the `url_env` vars is set, the app raises `RuntimeError` during
  lifespan startup — the container fails to boot rather than starting broken.
- `allowed_tables` must contain **bare table names**. Schema-qualified SQL
  (`public.city`) matches `city`, and aliases (`city c`) don't need entries.

Reference: [apps/data_access/world/main.py](../../../apps/data_access/world/main.py),
[apps/data_access/procurement/main.py](../../../apps/data_access/procurement/main.py).

## Step 3 — What the runtime gives you (and its exact semantics)

`create_data_access_app` builds:

- `GET /health` → `{status, protocol: "ptvn.dataplane/v1", service, database}`
- `POST /query`, body `{"database": "...", "sql": "..."}`, with **required**
  `x-tenant-id` and `x-user-id` headers (FastAPI `Header()` — missing headers
  are a 422, not a 400).

The guard, in order — know these codes, they are what you'll be debugging:

| Check | Failure |
|---|---|
| `body.database` != the plane's `database` | **404** "only serves '<x>'" |
| SQL fails to parse | **400** "SQL parse failed" |
| more than one statement | **400** "Exactly one SQL statement is allowed" |
| INSERT/UPDATE/DELETE/DROP/ALTER/CREATE, or no SELECT | **400** "Only read-only SELECT queries are allowed" |
| any table outside `allowed_tables` | **403** "Tables are not allowed: [...]" |
| connection pool not initialized | **503** "Database is not configured" |

Then execution: the SQL is wrapped as
`select * from (<your sql>) q limit <max_rows>`, run inside a
`readonly=True` transaction, with `app.tenant_id` and `app.user_id` set via
`set_config(..., true)` (transaction-local) so Postgres RLS policies apply.
Returns `{"rows": [...]}`.

> **CTE gotcha.** Table extraction uses sqlglot `exp.Table`, which counts a
> **CTE name as a table**. `with recent as (select * from city) select * from
> recent` yields `{city, recent}` — so it 403s unless `recent` is in the
> allowlist. Verified against this repo's sqlglot. Either avoid CTEs in tool
> SQL, or accept that the CTE name must be allowlisted (which is ugly — prefer
> subqueries, which extract cleanly).

> **Empty `database` skips the ownership check.** The guard is
> `if body.database and body.database != definition.database`, and `QueryIn.database`
> defaults to `""`. An omitted `database` field therefore passes straight to SQL
> validation instead of 404ing. `query_data_plane` always sends it, so this
> doesn't bite in the real flow — but don't rely on the check as a caller-side
> assertion.

## Step 4 — Wiring

- Compose services `<name>-db-init` (runs the seed SQL, `restart: "no"`,
  depends on `postgres` healthy) and `<name>-db-access` (standard
  `<<: *app` / `<<: *app-env` anchors, health check, `<DOMAIN>_DATABASE_URL`
  pointing at the **container-internal** host `postgres:5432`).
- `<name>-db-access` `depends_on`: `postgres` healthy **and** `<name>-db-init`
  `service_completed_successfully`.
- `.env` **and** `.env.example`: the host-based DSN (`localhost:5432`) plus
  `<DOMAIN>_DATA_PLANE_URL=http://localhost:<port>`.
- The consuming MCP server gets `<DOMAIN>_DATA_PLANE_URL` in its compose env
  (container-internal: `http://<name>-db-access:<port>`) and `depends_on` the
  plane, health-gated.

> **There is no data-plane registry.** `parse_data_planes` exists in the runtime
> and is unit-tested, but nothing in `apps/` calls it — there is no
> `DATA_PLANES` env var to update. Planes are addressed directly by each MCP
> server through its own `<DOMAIN>_DATA_PLANE_URL`. Don't go looking for a
> registry to register in.

Planes need **no Casbin rows of their own**. Access is governed upstream by
`datasource:<perm> read` (checked by the orchestrator) and `mcp:<server> execute`.
The plane trusts the identity headers the worker forwards — it authorizes
nothing, it only guards the SQL.

## Step 5 — Verify

```bash
docker compose up -d --build <name>-db-access
curl -s http://localhost:<port>/health

curl -s -X POST http://localhost:<port>/query -H 'Content-Type: application/json' \
  -H 'x-tenant-id: demo-tenant' -H 'x-user-id: demo-user' \
  -d '{"database": "<name>", "sql": "select ... limit 3"}'
```

Run the negative checks once — they're the whole point of the service:
`"database": "world"` → 404 · `delete from <table>` → 400 ·
`select * from pg_user` → 403.

Then the suite:

```bash
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests
```

> The manual's `python -m unittest discover -s tests -t . -v` **fails** on
> Python 3.11+: `tests/` has no `__init__.py`, so `-t .` reports "Start
> directory is not importable". Bare `python` also misses the deps — use the
> venv interpreter.

Tests follow [tests/test_data_access.py](../../../tests/test_data_access.py).
The key idiom: `httpx.ASGITransport` **does not run FastAPI's lifespan**, so no
pool exists — guard checks run before the pool is touched, and a *valid* query
returns **503**. That 503 is how you assert validation passed without a live
database. Cover: health identity, foreign database → 404, non-SELECT → 400,
multi-statement → 400, disallowed table → 403, valid query → 503.

## Checklist

- [ ] `docker/postgres/init/0X-create-<name>-database.sql` — idempotent, demo rows
- [ ] `apps/data_access/<name>/__init__.py` + `main.py` — `AgentDataPlane` + `create_data_access_app`, no hand-written routes
- [ ] `allowed_tables` lists bare table names, deliberately scoped
- [ ] `max_rows` left at 500 unless there's a stated reason
- [ ] Compose `<name>-db-init` + `<name>-db-access` with health check and correct `depends_on`
- [ ] `<DOMAIN>_DATABASE_URL` (container-internal in compose, localhost in `.env`/`.env.example`)
- [ ] `<DOMAIN>_DATA_PLANE_URL` in `.env`/`.env.example` **and** on the consuming MCP server
- [ ] `/health` correct; the three negative checks return 404/400/403
- [ ] Tests added; suite green

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| container exits on boot with `requires one of these env vars` | no `url_env` candidate set | set `<DOMAIN>_DATABASE_URL` in the compose env |
| 404 "only serves '<x>'" | tool sends the wrong `database` | the 3rd argument of `query_data_plane` must equal `AgentDataPlane.database` |
| 403 on a valid-looking SELECT | table not in `allowed_tables` — **or** a CTE name being counted as a table | add the table deliberately; prefer subqueries over CTEs |
| 400 "Only read-only SELECT" | non-SELECT, or a statement with no SELECT node | tools should pre-validate via `parse_sql_argument` |
| 422 on `/query` | missing `x-tenant-id` / `x-user-id` headers | the worker forwards them in the real flow; add them for manual curl |
| 503 "Database is not configured" | pool absent — lifespan didn't run (normal under `ASGITransport`) or startup failed | in tests this means validation **passed**; in production check startup logs |
| rows silently truncated | `max_rows` cap (default 500) wraps every query | raise it deliberately, or aggregate in SQL |
| RLS policies don't apply | reading the DB outside this plane, or policies not defined on the table | all reads must go through `/query`; `set_config` is transaction-local |

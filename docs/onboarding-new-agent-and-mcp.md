# Onboarding Manual: Adding a New Agent + MCP Server

This is the hands-on, step-by-step tutorial for adding a **new agent** and a
**new MCP tool server** to the platform. It is written so that a new AI
engineer can follow it top to bottom on their first week, with every function
they must implement spelled out with its exact signature.

It complements the two design documents:

- `docs/agent-services.md` — *why* the agent architecture looks like this.
- `docs/mcp-services.md` — *why* the MCP architecture looks like this.

Throughout the manual we build one complete worked example — a **finance**
vertical — consisting of:

| Piece | Service name | Port | Source location |
|---|---|---|---|
| Database (demo data) | `finance_db` (inside the shared postgres) | 5432 | `docker/postgres/init/03-create-finance-database.sql` |
| Data plane (holds DB credentials) | `finance-db-access` | 8009 | `apps/data_access/finance/main.py` |
| MCP server (the tools) | `finance-mcp` | 8013 | `apps/mcp/finance/main.py` |
| Agent (the planner) | `finance-agent` | 8008 | `apps/agents/finance/main.py` |

Copy the pattern and substitute your own domain name everywhere you see
`finance`.

---

## 1. The big picture (read this first)

```mermaid
flowchart LR
    user["User (JWT from Keycloak)"] -->|"POST /agents/finance-agent/runs"| gateway["Gateway :8000<br/>authn, rate limit,<br/>mints policy subjects"]
    gateway -->|"trusted headers"| orch["Orchestrator :8001<br/>AgentRegistry + McpRegistry<br/>Casbin enforcement<br/>run state, approvals"]
    orch -->|"POST /runs"| agent["finance-agent :8008<br/>plans, returns AgentDecision<br/>NO side effects"]
    orch -->|"tool.requested"| kafka["Kafka"]
    kafka --> worker["mcp-worker<br/>routes by input.server"]
    worker -->|"tools/call (JSON-RPC)"| mcp["finance-mcp :8013<br/>NO DB credentials"]
    mcp -->|"POST /query"| plane["finance-db-access :8009<br/>holds finance_db credentials<br/>final SQL guard + RLS"]
    worker -->|"tool.completed"| kafka --> orch
```

Four golden rules — everything in this manual follows from them:

1. **Agents decide, they never do.** An agent receives a message, plans, and
   returns an `AgentDecision` JSON object. It holds no Kafka, database, or
   tool credentials. If your agent code ever opens a DB connection, you are
   doing it wrong.
2. **The orchestrator enforces.** Every decision is checked against Casbin
   policy (`policy/casbin_policy.csv`) before any side effect happens. You
   onboard by *adding policy rows*, never by bypassing the check.
3. **MCP is the only tool transport.** Every executable decision names
   `tool="mcp"` and addresses a registered MCP server + tool name. The MCP
   worker delivers the call; agents never call MCP servers directly.
4. **Only data planes touch databases.** MCP servers are credential-free and
   delegate every read to the one data plane that owns the database. The
   plane enforces the final guard: single read-only SELECT, table allowlist,
   row cap, tenant-scoped RLS session.

The full run lifecycle (worth memorizing):

1. Gateway authenticates the JWT, checks `agent:<id> invoke`, and forwards the
   run to the orchestrator with trusted headers (`x-tenant-id`, `x-user-id`,
   `x-policy-subjects`, `x-allowed-permissions`).
2. Orchestrator resolves the agent in `AGENT_SERVICES`, calls its
   `POST /runs`, and gets back a decision.
3. Orchestrator enforces Casbin on the decision (`datasource:* read` +
   `mcp:<server> execute`), then publishes `tool.requested` to Kafka (or
   parks/denies/completes the run, depending on the decision action).
4. The MCP worker consumes the event, looks up `input.server` in its own
   `MCP_SERVICES` registry, and invokes `tools/call` on that server,
   forwarding `x-request-id` / `x-tenant-id` / `x-user-id`.
5. Data-reading tools call the owning data plane's `POST /query`; the plane
   validates and executes the SQL with RLS session variables set.
6. The worker publishes `tool.completed`; the orchestrator settles the run.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Agent card** | `GET /.well-known/agent-card` — JSON identity document the orchestrator fetches to discover an agent (id, workflow, actions, required permissions). |
| **MCP card** | `GET /.well-known/mcp-card` — the MCP analogue; lists the server's tools and required permissions. |
| **AgentDecision** | The JSON an agent returns from `POST /runs`: what it wants to happen (`tool`, `final`, `approval`, `async`, `deny`). |
| **Casbin subject** | Who is asking: `user:<id>` plus the JWT's role names (e.g. `role:finance-analyst`). Minted by the gateway; agents cannot change them. |
| **Casbin object** | What is being accessed: `agent:<id>`, `datasource:<permission>`, `mcp:<server-id>`. |
| **Data plane** | A tiny FastAPI service (from `apps/data_access/runtime.py`) that holds credentials for exactly one database and guards every query. |
| **Registry** | `AGENT_SERVICES` / `MCP_SERVICES` env vars: comma-separated `id=base-url` pairs. Adding an entry is how a service joins the platform — no orchestrator code changes. |
| **MCP worker** | `apps/workers/mcp_worker.py` — Kafka consumer that executes `tool.requested` events by calling the named MCP server. |
| **Supervisor router** | The virtual `assistant` agent served by the orchestrator itself; classifies a message and delegates to a registered agent. New agents join it automatically via their card description. |

---

## 3. Naming worksheet (fill this in before you code)

Decide these once; they must stay consistent across every file you touch:

| Item | Rule | Finance example |
|---|---|---|
| Agent id | kebab-case, ends in `-agent` | `finance-agent` |
| Workflow name | short lowercase word, used in spans/audit | `finance` |
| MCP server id | kebab-case, ends in `-mcp` | `finance-mcp` |
| Permission name | `<domain>-db`; **must** equal the Casbin object suffix `datasource:<name>` | `finance-db` |
| Data plane service | `<domain>-db-access` | `finance-db-access` |
| Database name | snake_case | `finance_db` |
| Data plane URL env | `<DOMAIN>_DATA_PLANE_URL` | `FINANCE_DATA_PLANE_URL` |
| Database URL env | `<DOMAIN>_DATABASE_URL` | `FINANCE_DATABASE_URL` |
| Ports | any free host ports (taken: 8000, 8001, 8004–8007, 8010–8012) | agent 8008, plane 8009, mcp 8013 |
| Keycloak/Casbin role | `role:<domain>-analyst` | `role:finance-analyst` |

> **Why the permission name matters:** the agent puts
> `required_permission="finance-db"` in its decision, the orchestrator checks
> the caller holds `datasource:finance-db read` in Casbin, and the MCP tool
> advertises the same name on its card. One string, three places — keep it
> identical.

---

## 4. Part 1 — Database and data plane (only if you read a NEW database)

Skip this part entirely if your tools reuse an existing data plane
(`world-db-access` / `procurement-db-access`) or read no database at all
(like `report-mcp`).

### Step 4.1 — Seed the database

Create `docker/postgres/init/03-create-finance-database.sql`. Mirror the
shape of `02-create-procurement-database.sql`: idempotent create, tables,
demo rows.

```sql
select 'create database finance_db'
where not exists (
    select 1
    from pg_database
    where datname = 'finance_db'
)\gexec

\connect finance_db

create table if not exists invoices (
    invoice_id text primary key,
    supplier_name text not null,
    business_unit text not null,
    invoice_date date not null,
    due_date date not null,
    status text not null,          -- 'open' | 'paid' | 'overdue'
    amount numeric(14, 2) not null
);

insert into invoices (invoice_id, supplier_name, business_unit,
                      invoice_date, due_date, status, amount) values
    ('INV-2026-0001', 'Siam Industrial Parts', 'Manufacturing',
     date '2026-05-02', date '2026-06-01', 'paid',    128400.00),
    ('INV-2026-0002', 'Bangkok Packaging Group', 'Retail',
     date '2026-05-20', date '2026-06-19', 'open',     84250.00),
    ('INV-2026-0003', 'Mekong Logistics Network', 'Distribution',
     date '2026-04-11', date '2026-05-11', 'overdue', 212300.00)
on conflict (invoice_id) do update set
    supplier_name = excluded.supplier_name,
    business_unit = excluded.business_unit,
    invoice_date = excluded.invoice_date,
    due_date = excluded.due_date,
    status = excluded.status,
    amount = excluded.amount;
```

### Step 4.2 — Create the data plane service

There are **no functions to implement** for a data plane — you only declare
an `AgentDataPlane` and the shared runtime builds the whole service
(`/query`, `/health`, SQL guard, RLS session, row cap).

Create the package:

```
apps/data_access/finance/__init__.py     (empty file)
apps/data_access/finance/main.py
```

`apps/data_access/finance/main.py`:

```python
"""Finance agent's database access layer.

Holds credentials for the finance database only. Runs as its own service
(`finance-db-access`) so a fault or compromise here cannot reach other
domains' data.
"""

from apps.data_access.runtime import AgentDataPlane, create_data_access_app


FINANCE_DB_TABLES = frozenset({"invoices"})

DEFINITION = AgentDataPlane(
    database="finance",                      # the `database` value callers must send
    service_name="finance-db-access",        # service name in traces/health
    allowed_tables=FINANCE_DB_TABLES,        # SQL touching other tables -> 403
    url_env=("FINANCE_DATABASE_URL",),       # env var(s) holding the DSN
    title="Finance Database Access Layer",
    # max_rows=500 is the default row cap; override only with a reason.
)

app = create_data_access_app(DEFINITION)
```

What the runtime gives you for free (see
`apps/data_access/runtime.py:create_data_access_app`):

- `POST /query` with body `{"database": "finance", "sql": "select ..."}` and
  required `x-tenant-id` / `x-user-id` headers.
- Refuses requests for any other database (`404`), non-SELECT or multi
  statement SQL (`400`), tables outside the allowlist (`403`).
- Wraps every query in `select * from (...) q limit <max_rows>` and sets
  `app.tenant_id` / `app.user_id` in the Postgres session so RLS policies
  apply.

### Step 4.3 — Compose entries for the database init and the plane

Add to `docker-compose.yml` (copy the `procurement-db-init` /
`procurement-db-access` blocks and rename):

```yaml
  finance-db-init:
    image: ghusta/postgres-world-db:2.15.0
    restart: "no"
    networks:
      - backend
    environment:
      PGPASSWORD: world123
    volumes:
      - ./docker/postgres/init:/docker-entrypoint-initdb.d:ro
    command:
      - sh
      - -c
      - psql -h postgres -U world -d world-db -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/03-create-finance-database.sql
    depends_on:
      postgres:
        condition: service_healthy

  finance-db-access:
    <<: *app
    command: uvicorn apps.data_access.finance.main:app --host 0.0.0.0 --port 8009
    environment:
      <<: *app-env
      # Container-internal host, like the other planes.
      FINANCE_DATABASE_URL: postgresql://world:world123@postgres:5432/finance_db
    ports:
      - "8009:8009"
    depends_on:
      postgres:
        condition: service_healthy
      finance-db-init:
        condition: service_completed_successfully
      otel-collector:
        condition: service_started
    healthcheck:
      test:
        - CMD-SHELL
        - python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8009/health', timeout=5)"
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
```

Also add the host-dev DSN to `.env.example` (and your `.env`):

```bash
FINANCE_DATABASE_URL=postgresql://world:world123@localhost:5432/finance_db
FINANCE_DATA_PLANE_URL=http://localhost:8009
```

### Step 4.4 — Verify the plane alone

```bash
docker compose up -d --build finance-db-access
curl -s http://localhost:8009/health
# {"status":"ok","protocol":"...","service":"finance-db-access","database":"finance"}

curl -s -X POST http://localhost:8009/query \
  -H 'Content-Type: application/json' \
  -H 'x-tenant-id: demo-tenant' -H 'x-user-id: demo-user' \
  -d '{"database": "finance", "sql": "select invoice_id, amount from invoices limit 3"}'
# {"rows":[...]}
```

Negative checks worth trying once, to see the guard work:
`"database": "world"` → 404; `"sql": "delete from invoices"` → 400;
`select * from pg_user` → 403.

---

## 5. Part 2 — The MCP server (the tools)

### Step 5.1 — Create the package

```
apps/mcp/finance/__init__.py     (empty file)
apps/mcp/finance/main.py
```

### Step 5.2 — Implement one handler function per tool

This is the core coding work. **A tool handler is one async function with
exactly this signature:**

```python
async def my_tool(arguments: dict[str, Any], context: McpToolContext) -> dict[str, Any]:
```

- `arguments` — the raw JSON object the caller sent. **Never trust it.**
  Validate every field before use.
- `context` — an `McpToolContext` with:
  - `context.tenant_id` / `context.user_id` — identity forwarded from the
    gateway (may be `None`; `query_data_plane` refuses to run without them).
  - `context.request_id` — the run's request id (useful for job-style tools
    that mint references, see `report-mcp`).
  - `context.http` — a shared `httpx.AsyncClient` for upstream calls.
- Return a JSON-serializable `dict`. It becomes the tool's
  `structuredContent` and the agent reads it from
  `result["output"]`.
- Raise `McpToolError("human-readable reason")` for anything the caller did
  wrong or any upstream refusal. It becomes an `isError: true` tool result
  (visible to the LLM), **not** a crash. Any other exception is also caught
  and reported the same way, but `McpToolError` gives a clean message.

Rules for handlers that build SQL (read `apps/mcp/world/main.py` for the
reference style):

- Only interpolate values you validated against a **closed vocabulary**: an
  enum tuple, a bounded integer (`parse_limit_argument`), or a
  shape-checked code (`code.isalpha()` etc.). Free-text user input must
  never be concatenated into SQL.
- For an LLM-written query, accept it via `parse_sql_argument(arguments)`
  (single read-only SELECT, length-capped, sqlglot-parsed) and let the data
  plane apply the final guard.
- All reads go through `query_data_plane(context, PLANE_URL, "<database>", sql)`
  — never open a DB connection in an MCP server.

`apps/mcp/finance/main.py` — complete file:

```python
"""Finance MCP service.

Read-only tools over the finance database. Holds no database credentials:
every tool builds a guarded SELECT and delegates to the finance data plane
(`finance-db-access`), which enforces the final read-only, table-allowlisted,
tenant-scoped SQL guard.
"""

import os
from typing import Any

from apps.mcp.runtime import (
    McpServerDefinition,
    McpTool,
    McpToolContext,
    McpToolError,
    create_mcp_app,
    parse_limit_argument,
    query_data_plane,
)


FINANCE_DATA_PLANE_URL = os.getenv(
    "FINANCE_DATA_PLANE_URL",
    "http://localhost:8009",
).rstrip("/")

INVOICE_STATUSES = ("open", "paid", "overdue")


async def list_recent_invoices(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    limit = parse_limit_argument(arguments)          # bounded int, 1..50
    status = arguments.get("status")
    where = ""
    if status is not None:
        if status not in INVOICE_STATUSES:           # closed vocabulary only
            raise McpToolError(
                f"Unknown status: {status}. Expected one of: {list(INVOICE_STATUSES)}",
            )
        where = f"where status = '{status}' "

    sql = (
        "select invoice_id, supplier_name, business_unit, "
        "invoice_date, due_date, status, amount "
        "from invoices "
        f"{where}"
        f"order by invoice_date desc limit {limit}"
    )
    rows = await query_data_plane(context, FINANCE_DATA_PLANE_URL, "finance", sql)
    return {"rows": rows, "row_count": len(rows)}


async def overdue_summary(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    sql = (
        "select supplier_name, count(invoice_id) as overdue_invoices, "
        "sum(amount) as overdue_amount "
        "from invoices where status = 'overdue' "
        "group by supplier_name order by overdue_amount desc limit 50"
    )
    rows = await query_data_plane(context, FINANCE_DATA_PLANE_URL, "finance", sql)
    return {"rows": rows, "row_count": len(rows)}


DEFINITION = McpServerDefinition(
    server_id="finance-mcp",
    name="Finance MCP Service",
    description=(
        "Read-only tools over the finance database: recent invoices and "
        "overdue-payment summaries. All reads go through the "
        "credential-isolated finance data plane."
    ),
    version="1.0.0",
    tools=(
        McpTool(
            name="list_recent_invoices",
            description=(
                "List the most recent invoices, optionally filtered to one "
                "status (open, paid, overdue)."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10,
                        "description": "How many invoices to return.",
                    },
                    "status": {
                        "type": "string",
                        "enum": list(INVOICE_STATUSES),
                        "description": "Restrict results to one status.",
                    },
                },
                "additionalProperties": False,
            },
            handler=list_recent_invoices,
            required_permission="finance-db",
        ),
        McpTool(
            name="overdue_summary",
            description="Summarize overdue invoice amounts per supplier.",
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            handler=overdue_summary,
            required_permission="finance-db",
        ),
    ),
)

app = create_mcp_app(DEFINITION)
```

### Step 5.3 — What every field means

`McpTool` (from `apps/mcp/runtime.py`):

| Field | What it is | Gotchas |
|---|---|---|
| `name` | The tool name agents call. snake_case. | Must be unique within the server. |
| `description` | Shown to the LLM planner via `tools/list`. | Write it for a model: say what it does and when to use it. |
| `input_schema` | JSON Schema for `arguments`. | Documentation + client-side hint. **Your handler must still validate** — the runtime does not enforce the schema. |
| `handler` | The async function from Step 5.2. | Signature must be `(arguments, context) -> dict`. |
| `required_permission` | The `datasource:*` permission the tool ultimately reads. | Advertised on the card and in `tools/list` `_meta`; keeps Casbin policy alignable. Omit for tools that read nothing (see `report-mcp`). |

`McpServerDefinition`: `server_id` (must equal the id you register in
`MCP_SERVICES`), `name`, `description` (becomes the MCP `initialize`
"instructions"), `version`, `tools` (a tuple).

`create_mcp_app(DEFINITION)` gives you for free:

- `GET /.well-known/mcp-card` — discovery document.
- `POST /mcp` — full JSON-RPC 2.0 endpoint (`initialize`, `ping`,
  `tools/list`, `tools/call`), with tracing spans per tool call.
- `GET /health` — liveness.

### Step 5.4 — Compose service

```yaml
  finance-mcp:
    <<: *app
    command: uvicorn apps.mcp.finance.main:app --host 0.0.0.0 --port 8013
    environment:
      <<: *app-env
      FINANCE_DATA_PLANE_URL: http://finance-db-access:8009
    ports:
      - "8013:8013"
    depends_on:
      finance-db-access:
        condition: service_healthy
      otel-collector:
        condition: service_started
    healthcheck:
      test:
        - CMD-SHELL
        - python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8013/health', timeout=5)"
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
```

### Step 5.5 — Register the server in `MCP_SERVICES`

`MCP_SERVICES` is read by **two** services — the orchestrator (for discovery
and Casbin-time validation) and the mcp-worker (for execution). Update both
defaults in `docker-compose.yml`:

```yaml
# under services.orchestrator.environment AND services.mcp-worker.environment
MCP_SERVICES: ${MCP_SERVICES:-world-mcp=http://world-mcp:8010,procurement-mcp=http://procurement-mcp:8011,report-mcp=http://report-mcp:8012,finance-mcp=http://finance-mcp:8013}
```

Also add `finance-mcp` (health-gated) to `services.mcp-worker.depends_on`,
and update `.env.example` / your `.env` for host-based development:

```bash
MCP_SERVICES=world-mcp=http://localhost:8010,procurement-mcp=http://localhost:8011,report-mcp=http://localhost:8012,finance-mcp=http://localhost:8013
```

> If you set `MCP_SERVICES` in your `.env`, the compose default is
> overridden — so the `.env` value must also contain the new entry.

### Step 5.6 — Grant Casbin access

Append to `policy/casbin_policy.csv` (format:
`p, subject, tenant, object, action`; `*` = every tenant):

```csv
p, role:finance-analyst, *, mcp:finance-mcp, execute
p, role:finance-analyst, *, datasource:finance-db, read
p, role:data-admin, *, mcp:finance-mcp, execute
p, role:data-admin, *, datasource:finance-db, read
```

Without the `mcp:` row, every tool call is denied with a
`tool_access_denied` audit event; without the `datasource:` row, with
`permission_access_denied`. This is the single most common onboarding
mistake.

### Step 5.7 — Verify the MCP server standalone

```bash
docker compose up -d --build finance-mcp

# 1. Card
curl -s http://localhost:8013/.well-known/mcp-card | python3 -m json.tool

# 2. Tool list (JSON-RPC)
curl -s -X POST http://localhost:8013/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# 3. A real tool call (identity headers are required for data reads)
curl -s -X POST http://localhost:8013/mcp \
  -H 'Content-Type: application/json' \
  -H 'x-tenant-id: demo-tenant' -H 'x-user-id: demo-user' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_recent_invoices","arguments":{"limit":5,"status":"overdue"}}}'
```

Expected: result 3 returns `"isError": false` with your rows in
`structuredContent`. Try a bad status value and confirm you get
`"isError": true` with your validation message (not a 500).

### Step 5.8 — Verify discovery through the orchestrator

```bash
docker compose up -d orchestrator mcp-worker
curl -s http://localhost:8001/internal/mcp | python3 -m json.tool
```

Your server must appear with its card and discovered tools. If it shows an
error instead, see Troubleshooting (section 8).

### Step 5.9 — Unit tests

Copy the patterns in `tests/test_mcp.py`:

- `McpContractTests` — drives the JSON-RPC endpoint via
  `httpx.ASGITransport(app=...)`; assert your `tools/list` names.
- `McpToolExecutionTests` — uses the `FakeDataPlane` transport to fake the
  plane and asserts (a) happy-path rows, (b) invalid arguments become
  `isError` results, (c) missing identity headers become tool errors.

Run everything:

```bash
python -m unittest discover -s tests -t . -v
```

---

## 6. Part 3 — The agent

### Step 6.1 — Create the package

```
apps/agents/finance/__init__.py     (empty file)
apps/agents/finance/main.py
```

### Step 6.2 — The functions you must implement

An agent is an `AgentDefinition` plus **two required functions and two
optional pieces**. The shared runtime (`apps/agents/runtime.py`) turns them
into a full service.

**Function 1 (required): `fallback_action`**

```python
def fallback_action(message: str) -> str:
```

Deterministic keyword → action mapping, used when the LiteLLM planner is not
configured or fails. Must return one of the action names you declare in
`AgentDefinition.actions`. Keep it dumb and predictable: `if "delete" in
text: return "approval"` is exactly the right level.

**Function 2 (required): `decide`**

```python
def decide(planned: PlannedAction, request: AgentRunRequest) -> AgentDecision:
```

The heart of the agent. Input:

- `planned.action` — the action chosen by the planner (LLM or fallback).
- `planned.arguments` — dict of LLM-planned arguments (may be empty or
  malformed — validate/default everything).
- `planned.reply` — LLM-written user-facing text, only for chat turns.
- `request` — the full run context (`message`, `tenant_id`, `user_id`,
  `allowed_permissions`, ...). Read-only; never trust it for authorization —
  the orchestrator re-checks everything.

Output: an `AgentDecision` with one of five `action` values:

| `action` | Meaning | Fields you must set |
|---|---|---|
| `"final"` | Complete the run now with a chat answer; no tool runs. | `output` (the user-facing text) |
| `"tool"` | Ask the orchestrator to execute one MCP tool. | `tool="mcp"`, `tool_input={"server", "name", "arguments"}`, `required_permission` |
| `"approval"` | Park the run for a human (`requires_approval`). | `audit_event` (e.g. `"human_approval_required"`) |
| `"async"` | Accept the run and drive it yourself in the background. | requires `run_async` on the definition |
| `"deny"` | Refuse the run. | `reason` |

Every decision also carries `workflow` (your workflow name),
`planner_action` (the action you are responding to), and optionally
`audit_event`.

**Piece 3 (recommended): `PLANNER_GUIDANCE`**

A plain string appended to the LLM planner prompt. Describe each data
action and the exact `arguments` it takes; for SQL-writing agents, list the
queryable tables. The planner is instructed to never invent argument fields
you do not mention — so mention them all.

**Piece 4 (optional): `run_async`**

```python
async def run_async(request: AgentRunRequest, broker: ToolBrokerClient) -> str:
```

Only needed for multi-step workflows. When `decide` returns
`action="async"`, the runtime schedules this in the background. Use
`await broker.run_tool("mcp", {...}, required_permission=...)` for each step
(each call is Casbin-checked by the orchestrator) and return the final
user-facing text. See `run_market_brief` in `apps/agents/world/main.py` for
the reference implementation.

### Step 6.3 — Complete agent file

`apps/agents/finance/main.py`:

```python
"""Finance analyst agent service.

Plans finance-database runs: recent-invoice lookups, overdue summaries, a
human approval gate for destructive requests, and direct chat replies for
greetings. Side effects are executed by the orchestrator after policy
enforcement; every executable decision names the `mcp` tool targeting
`finance-mcp`.
"""

from typing import Any

from apps.agents.runtime import (
    AgentDecision,
    AgentDefinition,
    AgentRunRequest,
    PlannedAction,
    create_agent_app,
)


DATA_KEYWORDS = (
    "invoice", "invoices", "payment", "payments", "amount",
    "supplier", "spend", "list", "show", "recent",
)

CHAT_FALLBACK_REPLY = (
    "Hello! I'm the finance analyst agent. Ask me about invoices and "
    "payments — for example: 'show recent invoices' or 'which suppliers "
    "have overdue payments?'."
)

PLANNER_GUIDANCE = """
Data actions for this agent:
- "invoices": recent invoice lookup. Optional arguments.limit (integer,
  1-50) and arguments.status (one of: open, paid, overdue).
- "overdue": overdue payment summary per supplier. No arguments.
""".strip()

INVOICE_STATUSES = ("open", "paid", "overdue")


def fallback_action(message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"
    if "overdue" in text or "late" in text:
        return "overdue"
    if any(keyword in text for keyword in DATA_KEYWORDS):
        return "invoices"
    return "chat"


def decide(planned: PlannedAction, request: AgentRunRequest) -> AgentDecision:
    action = planned.action

    if action == "chat":
        return AgentDecision(
            action="final",
            workflow="finance",
            planner_action=action,
            audit_event="agent_chat_answered",
            output=planned.reply or CHAT_FALLBACK_REPLY,
        )

    if action == "approval":
        return AgentDecision(
            action="approval",
            workflow="finance",
            planner_action=action,
            audit_event="human_approval_required",
        )

    if action == "overdue":
        return AgentDecision(
            action="tool",
            workflow="finance",
            planner_action=action,
            tool="mcp",
            required_permission="finance-db",
            tool_input={
                "server": "finance-mcp",
                "name": "overdue_summary",
                "arguments": {},
            },
        )

    # Default data action: recent invoices. LLM-planned arguments are
    # validated here and again inside the MCP tool.
    arguments: dict[str, Any] = {}
    limit = planned.arguments.get("limit")
    if isinstance(limit, int) and not isinstance(limit, bool) and 1 <= limit <= 50:
        arguments["limit"] = limit
    status = planned.arguments.get("status")
    if isinstance(status, str) and status in INVOICE_STATUSES:
        arguments["status"] = status

    return AgentDecision(
        action="tool",
        workflow="finance",
        planner_action="invoices",
        tool="mcp",
        required_permission="finance-db",
        tool_input={
            "server": "finance-mcp",
            "name": "list_recent_invoices",
            "arguments": arguments,
        },
    )


DEFINITION = AgentDefinition(
    agent_id="finance-agent",
    name="Finance Analyst Agent",
    description=(
        "Answers finance questions with read-only MCP lookups over the "
        "invoice database: recent invoices, payment status, and overdue "
        "supplier summaries. Destructive requests require human approval."
    ),
    version="1.0.0",
    workflow="finance",
    actions=frozenset({"chat", "invoices", "overdue", "approval"}),
    required_permissions=("finance-db",),
    tools=("mcp",),
    fallback_action=fallback_action,
    decide=decide,
    planner_guidance=PLANNER_GUIDANCE,
)

app = create_agent_app(DEFINITION)
```

`AgentDefinition` field-by-field:

| Field | What it is | Gotchas |
|---|---|---|
| `agent_id` | The id used in URLs, the registry, and Casbin (`agent:finance-agent`). | Must equal the `AGENT_SERVICES` key. |
| `name` / `description` / `version` | Card identity. | The **description feeds the supervisor router's classifier** — write it as "what questions belong to me". |
| `workflow` | Short name stamped on spans, audit events, decisions. | |
| `actions` | The closed set of planner actions. | The LLM may only pick from these; anything else falls back. |
| `required_permissions` | Advertised on the card. | Informational; enforcement uses each decision's `required_permission`. |
| `tools` | Advertised tool transports. | Always `("mcp",)` on this platform. |
| `fallback_action` / `decide` / `run_async` / `planner_guidance` | Your functions from Step 6.2. | |

`create_agent_app(DEFINITION)` gives you for free: the agent card endpoint,
`POST /runs` (LangGraph plan node + LiteLLM planner + Langfuse generation
span + fallback), `GET /health`, and background scheduling for `async` runs.

### Step 6.4 — Compose service

```yaml
  finance-agent:
    <<: *app
    command: uvicorn apps.agents.finance.main:app --host 0.0.0.0 --port 8008
    environment:
      <<: *app-env
    ports:
      - "8008:8008"
    depends_on:
      otel-collector:
        condition: service_started
    healthcheck:
      test:
        - CMD-SHELL
        - python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8008/health', timeout=5)"
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
```

Optionally add `finance-agent: {condition: service_healthy}` to
`services.orchestrator.depends_on` so compose starts it first (not strictly
required — the registry discovers lazily).

### Step 6.5 — Register the agent in `AGENT_SERVICES`

Compose default (under `services.orchestrator.environment`):

```yaml
AGENT_SERVICES: ${AGENT_SERVICES:-world-agent=http://world-agent:8004,procurement-agent=http://procurement-agent:8005,finance-agent=http://finance-agent:8008}
```

`.env.example` / your `.env` (host-based dev):

```bash
AGENT_SERVICES=world-agent=http://localhost:8004,procurement-agent=http://localhost:8005,finance-agent=http://localhost:8008
```

### Step 6.6 — Casbin + Keycloak role

Append to `policy/casbin_policy.csv` (the `mcp:`/`datasource:` rows were
added in Step 5.6):

```csv
p, role:finance-analyst, *, agent:assistant, invoke
p, role:finance-analyst, *, agent:finance-agent, invoke
p, role:data-admin, *, agent:finance-agent, invoke
```

- `agent:finance-agent invoke` — direct invocation.
- `agent:assistant invoke` — lets the role use the supervisor router.

Casbin subjects are the **role names inside the JWT** — the gateway turns
`user.roles` directly into subjects (`apps/authz.py:policy_subjects`), and
the demo realm names its roles with the prefix baked in
(`role:world-analyst`, ...). So for a new role you must either:

- **Option A (quickest):** grant the new objects to an existing role (e.g.
  `role:data-admin`, as above) and test with the `data-admin` demo user; or
- **Option B (proper):** edit `docker/keycloak/ptvn-realm.json` — add a
  realm role `role:finance-analyst` (with a description) and a demo user
  `finance-analyst` with that role, mirroring the existing entries. The
  realm imports only when the Keycloak container starts fresh, so recreate
  it: `docker compose up -d --force-recreate keycloak`.

### Step 6.7 — The supervisor router: nothing to code

The router builds its candidate list from the live agent registry: your
card's `description` becomes routing input for the LLM classifier, and the
keyword fallback derives domain words from the card. Adding the
`AGENT_SERVICES` entry (Step 6.5) and the `agent:assistant` grant
(Step 6.6) is all it takes for "which suppliers have overdue invoices?"
sent to `assistant` to land on your agent.

### Step 6.8 — End-to-end verification

```bash
# Bring everything up
docker compose up -d --build

# 1. Orchestrator sees the agent + MCP server
curl -s http://localhost:8001/internal/agents | python3 -m json.tool
curl -s http://localhost:8001/internal/mcp | python3 -m json.tool

# 2. Get a user token from Keycloak (Option A: data-admin demo user)
TOKEN=$(curl -s -X POST \
  http://localhost:8080/realms/ptvn/protocol/openid-connect/token \
  -d grant_type=password -d client_id=agent-frontend \
  -d username=data-admin -d password=data-admin-password \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

# 3. Run the agent through the gateway
curl -s -X POST http://localhost:8000/agents/finance-agent/runs \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"message": "show recent overdue invoices"}'
# -> {"request_id": "...", "run_id": "...", "status": "running", ...}

# 4. Poll the run until it settles
curl -s http://localhost:8000/runs/<run_id> -H "Authorization: Bearer $TOKEN"
# -> status "completed" with the tool result, rows included

# 5. Try the supervisor router
curl -s -X POST http://localhost:8000/agents/assistant/runs \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"message": "which suppliers have overdue payments?"}'
```

Also check the chat UI (`apps/frontend/chat.html`) and, for tracing, the
run's single trace in Grafana/Tempo (:3000) and the `agent-run` trace in
Langfuse (:3001).

### Step 6.9 — Unit tests

`decide` and `fallback_action` are pure functions — test them directly, no
HTTP needed. Follow `tests/test_agents.py::AgentDecisionTests`:

```python
import unittest

from apps.agents.finance.main import DEFINITION, decide, fallback_action
from apps.agents.runtime import AgentRunRequest, PlannedAction


def run_request(message: str) -> AgentRunRequest:
    return AgentRunRequest(
        request_id="run-1",
        tenant_id="demo-tenant",
        user_id="demo-user",
        agent_id="finance-agent",
        message=message,
        allowed_permissions=["finance-db"],
        policy_subjects=["role:finance-analyst"],
    )


class FinanceAgentDecisionTests(unittest.TestCase):
    def test_overdue_question_routes_to_mcp_tool(self) -> None:
        decision = decide(PlannedAction(action="overdue"), run_request("overdue?"))
        self.assertEqual(decision.action, "tool")
        self.assertEqual(decision.tool, "mcp")
        self.assertEqual(decision.tool_input["server"], "finance-mcp")
        self.assertEqual(decision.tool_input["name"], "overdue_summary")
        self.assertEqual(decision.required_permission, "finance-db")

    def test_invalid_planned_status_is_dropped(self) -> None:
        planned = PlannedAction(action="invoices", arguments={"status": "bogus"})
        decision = decide(planned, run_request("show invoices"))
        self.assertNotIn("status", decision.tool_input["arguments"])

    def test_destructive_message_requires_approval(self) -> None:
        self.assertEqual(fallback_action("please delete old invoices"), "approval")

    def test_every_fallback_action_is_declared(self) -> None:
        for message in ("hi", "overdue?", "show invoices", "delete it"):
            self.assertIn(fallback_action(message), DEFINITION.actions)
```

For registry/worker behaviors (discovery failures, denied tool calls) copy
`tests/test_orchestrator_broker.py` and `tests/test_mcp.py::McpWorkerTests`
patterns. Run the full suite before opening a PR:

```bash
python -m unittest discover -s tests -t . -v
```

---

## 7. Final checklist (print this)

**MCP server**

- [ ] `apps/mcp/<name>/__init__.py` + `main.py` — handlers, `McpTool`s,
      `McpServerDefinition`, `app = create_mcp_app(DEFINITION)`
- [ ] (if new DB) `docker/postgres/init/0X-create-<name>-database.sql`
- [ ] (if new DB) `apps/data_access/<name>/main.py` (`AgentDataPlane`) +
      compose services `<name>-db-init`, `<name>-db-access` + `<DOMAIN>_DATABASE_URL`
- [ ] Compose service `<name>-mcp` with health check + `<DOMAIN>_DATA_PLANE_URL`
- [ ] `MCP_SERVICES` updated in **orchestrator** and **mcp-worker** compose
      env, `.env`, `.env.example`
- [ ] mcp-worker `depends_on` includes the new server
- [ ] Casbin: `mcp:<server-id> execute` + `datasource:<perm> read` for each role
- [ ] Card + `tools/call` verified by curl; `GET /internal/mcp` shows the server
- [ ] Tests added; suite green

**Agent**

- [ ] `apps/agents/<name>/__init__.py` + `main.py` — `fallback_action`,
      `decide`, `PLANNER_GUIDANCE`, optional `run_async`, `AgentDefinition`,
      `app = create_agent_app(DEFINITION)`
- [ ] Compose service with health check
- [ ] `AGENT_SERVICES` updated in orchestrator compose env, `.env`, `.env.example`
- [ ] Casbin: `agent:<agent-id> invoke` (+ `agent:assistant invoke`) per role
- [ ] Keycloak role granted (existing role, or new realm role + user + Keycloak recreate)
- [ ] `GET /internal/agents` shows the card; end-to-end run completes via gateway
- [ ] Router reaches the agent for a domain question sent to `assistant`
- [ ] Tests added; suite green

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Gateway returns `403 User cannot access this agent` | Missing `agent:<id> invoke` for the caller's role, or the JWT lacks the role | Add the Casbin row; check the token's `roles` claim matches the subject string exactly (including the `role:` prefix) |
| Run denied with `tool_access_denied` | Missing `mcp:<server-id> execute` row | Add it for the role |
| Run denied with `permission_access_denied` | Missing `datasource:<perm> read`, or the agent's `required_permission` string doesn't match the Casbin object | Align the one permission string everywhere (Step 3 worksheet) |
| Run fails with `No MCP server registered for '<id>'` | `MCP_SERVICES` updated on the orchestrator but **not the mcp-worker** (or vice versa), or a typo between `server_id` and the registry key | Update both env blocks; restart both containers |
| `GET /internal/mcp` shows 502/504 for your server | Server not running, wrong base URL/port in `MCP_SERVICES`, or the card endpoint errors | `curl` the card directly; check container logs |
| Tool result `isError: true` — "x-tenant-id and x-user-id headers are required" | Calling `/mcp` directly without identity headers, or a caller not forwarding them | Add the headers (the worker forwards them automatically in the real flow) |
| Data plane 404 "only serves '<x>'" | Tool sends the wrong `database` value to the plane | The `database` argument of `query_data_plane` must equal the plane's `AgentDataPlane.database` |
| Data plane 403 on a valid-looking SELECT | Table not in `allowed_tables` | Add the table to the plane's allowlist (deliberate decision, not a reflex) |
| Agent always uses the fallback planner | `LITELLM_MODEL` / `LITELLM_API_KEY` unset or the LLM returned an action outside `actions` | Configure LiteLLM env; check the `agent.llm_plan` span in Langfuse for `app.planner.result` |
| New Keycloak role/user doesn't exist | Realm JSON imports only on a fresh container | `docker compose up -d --force-recreate keycloak` |
| Router never picks your agent | Card `description` too vague for the classifier; fallback keywords don't match | Rewrite the description as "what questions belong to me"; test with an explicit direct run first |

---

## 9. Quick reference

**Decision actions:** `final` (answer now), `tool` (one MCP call),
`approval` (park for human), `async` (agent drives via tool broker),
`deny` (refuse).

**`tool_input` shape for MCP decisions:**

```json
{"server": "finance-mcp", "name": "list_recent_invoices", "arguments": {"limit": 5}}
```

**Casbin objects/actions:** `agent:<id>` `invoke` · `datasource:<perm>`
`read` · `mcp:<server-id>` `execute`.

**Registries:** `AGENT_SERVICES` (orchestrator) and `MCP_SERVICES`
(orchestrator **and** mcp-worker), both `id=base-url,id=base-url`.

**Port map:** gateway 8000 · orchestrator 8001 · world-agent 8004 ·
procurement-agent 8005 · world-db-access 8006 · procurement-db-access 8007 ·
finance-agent 8008 · finance-db-access 8009 · world-mcp 8010 ·
procurement-mcp 8011 · report-mcp 8012 · finance-mcp 8013.

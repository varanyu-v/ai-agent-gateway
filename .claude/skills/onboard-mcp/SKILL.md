---
name: onboard-mcp
description: Add a new MCP tool server, add a tool to an existing one, or bring MCP code up to repo standard. Use when asked to "add an MCP server", "add a tool", "expose X as a tool", "wire up an MCP service", "give the agent a new capability", or when reviewing MCP handlers for convention drift. Covers the tool-handler contract, input validation and SQL safety, data-plane delegation, the four places MCP_SERVICES lives, Casbin rows, and verification. For the agent that calls these tools, see the onboard-agent skill.
---

# Onboarding an MCP tool server

Full tutorial: [docs/onboarding-new-agent-and-mcp.md](../../../docs/onboarding-new-agent-and-mcp.md)
§5 (MCP) and §4 (data plane) — a worked `finance` example. **`finance` is
teaching material and does not exist in the repo.** Real servers are
`world-mcp`, `procurement-mcp`, `report-mcp`. Design rationale:
[docs/mcp-services.md](../../../docs/mcp-services.md).

This skill is the operating procedure. Open the manual for full file templates.

## The two rules that shape every MCP server

1. **MCP servers hold no credentials.** Every read is delegated to the one data
   plane that owns the database, via `query_data_plane(...)`. An MCP server
   opening a DB connection is always a bug.
2. **Never trust `arguments`.** The runtime does **not** enforce `input_schema`
   — it is documentation and a client hint only. Your handler validates every
   field, every time. This is the single most important MCP convention here.

## Step 0 — Decide the scope

Ask first, don't assume:

- **New tool on an existing server?** Skip to Step 2 (write the handler), add
  the `McpTool` entry, then Step 5 (verify). No wiring, no new Casbin rows if
  the `required_permission` already exists.
- **New server?** Full path below.
- **New database too?** You also need a data plane — **use the
  `onboard-data-access` skill**. Skip it if the tools reuse `world-db-access` /
  `procurement-db-access`, or read nothing at all (`report-mcp` is the
  reference for a credential-free server).

Naming: server id is kebab-case ending `-mcp`; tool names are snake_case and
unique within the server; `server_id` **must** equal the `MCP_SERVICES` key.
Pick a free host port from `docker-compose.yml` — derive it, don't trust a
doc's port map.

> **The one-string-three-places rule.** A tool's `required_permission` must be
> identical to the agent's `required_permission` and to the Casbin object
> `datasource:<name>`. One typo = `permission_access_denied`.

## Step 1 — Package

```
apps/mcp/<name>/__init__.py     (empty)
apps/mcp/<name>/main.py
```

## Step 2 — Write one handler per tool

Exactly this signature:

```python
async def my_tool(arguments: dict[str, Any], context: McpToolContext) -> dict[str, Any]:
```

`McpToolContext` carries `tenant_id`, `user_id` (both may be `None` —
`query_data_plane` refuses without them), `request_id` (use it to mint
job-scoped references, see `report-mcp`), and `http` (a shared
`httpx.AsyncClient`).

Return a JSON-serializable dict — it becomes `structuredContent`, and the agent
reads it from `result["output"]`. Raise `McpToolError("human reason")` for bad
input or upstream refusal: the runtime converts it to an `isError: true` result
the LLM can read, **not** a 500. Any other exception is caught the same way but
yields an uglier message, so prefer `McpToolError`.

### Validation helpers (use these, don't hand-roll)

From [apps/mcp/runtime.py](../../../apps/mcp/runtime.py):

- `parse_limit_argument(arguments, default=10, maximum=50)` → bounded int;
  rejects bools and out-of-range values.
- `parse_sql_argument(arguments)` → one read-only SELECT, `MAX_SQL_LENGTH`
  4000, sqlglot-parsed, rejects INSERT/UPDATE/DELETE/DROP/ALTER/CREATE. This is
  a fail-fast courtesy check — the plane re-validates authoritatively.
- `query_data_plane(context, PLANE_URL, "<database>", sql)` → rows; raises
  `McpToolError` on timeout, unreachable plane, or a 4xx/5xx refusal (it
  forwards the plane's `detail`).

### SQL safety

Interpolate **only** values validated against a closed vocabulary — an enum
tuple (`CONTINENTS`, `INVOICE_STATUSES`), a bounded int from
`parse_limit_argument`, or a shape-checked code (`code.isalpha()`, a compiled
regex like `report-mcp`'s `REPORT_TYPE_PATTERN`). Free text is **never**
concatenated into SQL. If the LLM needs to write the query, take it through
`parse_sql_argument` and let the plane apply the final guard — that is what the
`run_sql` tools do.

Reference style: [apps/mcp/world/main.py](../../../apps/mcp/world/main.py)
(enum + shape-checked code + `run_sql`),
[apps/mcp/report/main.py](../../../apps/mcp/report/main.py) (regex-validated,
no database, no `required_permission`).

## Step 3 — Declare the server

`McpTool` fields: `name`, `description` (**written for a model** — say what it
does and when to use it), `input_schema` (JSON Schema; set
`additionalProperties: False`), `handler`, `required_permission` (omit only for
tools that read nothing).

`McpServerDefinition` fields: `server_id`, `name`, `description` (becomes the
MCP `initialize` instructions), `version`, `tools` (a **tuple**).

Then `app = create_mcp_app(DEFINITION)`, which gives you for free:
`GET /.well-known/mcp-card`, `POST /mcp` (JSON-RPC 2.0: `initialize`, `ping`,
`tools/list`, `tools/call`) with a tracing span per tool call, and
`GET /health`. Protocol revision is pinned at `MCP_PROTOCOL_VERSION`
(`2025-06-18`).

## Step 4 — Wiring (where MCP onboarding actually breaks)

**`MCP_SERVICES` lives in four places. Miss one and you get a silent gap:**

1. `DEFAULT_MCP_SERVICES` in [apps/orchestrator/mcp_registry.py](../../../apps/orchestrator/mcp_registry.py)
   — the **code fallback**, imported by both the orchestrator and the
   mcp-worker. Used whenever the env var is unset (plain host dev). The manual
   omits this one.
2. `services.orchestrator.environment` in `docker-compose.yml`
3. `services.mcp-worker.environment` in `docker-compose.yml` — the worker reads
   the registry independently to execute calls
4. `.env` **and** `.env.example`

Updating the orchestrator but not the worker (or vice versa) produces
`No MCP server registered for '<id>'`. A value in `.env` **overrides** the
compose default entirely, so the `.env` value must itself list every server.

Also:

- Add the server to `services.mcp-worker.depends_on`, health-gated.
- Compose service with the standard `<<: *app` / `<<: *app-env` anchors, a
  health check, and `<DOMAIN>_DATA_PLANE_URL` pointing at the container-internal
  plane URL.
- Casbin rows in `policy/casbin_policy.csv`
  (`p, subject, tenant, object, action`), per role:
  `p, role:<domain>-analyst, *, mcp:<server-id>, execute` and
  `p, role:<domain>-analyst, *, datasource:<perm>, read`.
  Missing the first = `tool_access_denied`; missing the second =
  `permission_access_denied`.

Agents never call the server directly — the mcp-worker routes by
`input.server` and forwards `x-request-id` / `x-tenant-id` / `x-user-id`.

## Step 5 — Verify before claiming done

```bash
docker compose up -d --build <name>-mcp

curl -s http://localhost:<port>/.well-known/mcp-card | python3 -m json.tool

curl -s -X POST http://localhost:<port>/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# identity headers are required for data reads
curl -s -X POST http://localhost:<port>/mcp -H 'Content-Type: application/json' \
  -H 'x-tenant-id: demo-tenant' -H 'x-user-id: demo-user' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"<tool>","arguments":{}}}'

curl -s http://localhost:8001/internal/mcp | python3 -m json.tool   # discovery
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests
```

> The manual's `python -m unittest discover -s tests -t . -v` **fails** on
> Python 3.11+: `tests/` has no `__init__.py`, so `-t .` reports "Start
> directory is not importable". Bare `python` also misses the deps.

Deliberately send a bad argument once and confirm you get `isError: true` with
your message — not a 500. That distinction is the contract.

Tests follow [tests/test_mcp.py](../../../tests/test_mcp.py):

- `McpContractTests` — drives `/mcp` via `httpx.ASGITransport`; assert your
  `tools/list` names, schemas, and advertised permissions.
- `McpToolExecutionTests` — uses the `FakeDataPlane` transport; assert the
  happy path, invalid arguments becoming `isError`, missing identity headers
  becoming a tool error, and a plane refusal surfacing as a tool error.
- `McpWorkerTests` / `McpRegistryTests` — only if you touched routing.

## Checklist

- [ ] `apps/mcp/<name>/__init__.py` + `main.py` — handlers, `McpTool`s, `McpServerDefinition`, `app = create_mcp_app(DEFINITION)`
- [ ] Every handler validates every argument; closed vocabulary for anything reaching SQL
- [ ] Failures raise `McpToolError`, not bare exceptions
- [ ] (new DB) init SQL + `apps/data_access/<name>/main.py` + `<name>-db-init` / `<name>-db-access` compose + `<DOMAIN>_DATABASE_URL`
- [ ] Compose `<name>-mcp` with health check + `<DOMAIN>_DATA_PLANE_URL`
- [ ] `MCP_SERVICES` in **all four** places (code default, orchestrator, mcp-worker, `.env` + `.env.example`)
- [ ] mcp-worker `depends_on` includes it, health-gated
- [ ] Casbin `mcp:<id> execute` + `datasource:<perm> read` per role
- [ ] `server_id` == `MCP_SERVICES` key; `required_permission` matches the agent's and Casbin's
- [ ] Card + `tools/call` curl-verified; appears in `/internal/mcp`
- [ ] Tests added; suite green

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `No MCP server registered for '<id>'` | `MCP_SERVICES` updated in some but not all four places, or `server_id` ≠ registry key | reconcile all four, restart orchestrator **and** worker |
| `/internal/mcp` shows 502/504 | server down, wrong URL/port, or the card endpoint errors | curl the card directly; check container logs |
| `isError`: "x-tenant-id and x-user-id headers are required" | calling `/mcp` without identity headers | add them (the worker forwards automatically in the real flow) |
| `tool_access_denied` | missing `mcp:<id> execute` | add the row for the role |
| `permission_access_denied` | missing `datasource:<perm> read`, or the permission string differs across the three places | align the one string |
| plane 404 "only serves '<x>'" | wrong `database` argument | must equal the plane's `AgentDataPlane.database` |
| plane 403 on a valid SELECT | table not in `allowed_tables` | add it deliberately, not reflexively |
| tool call 500s instead of returning `isError` | handler raised before the runtime could wrap it, or the return value isn't JSON-serializable | raise `McpToolError`; return plain dicts/lists/scalars |
| new tool on an existing server doesn't show up | the card was cached at startup; `ensure_discovered` only re-probes servers whose card is **empty**, so a healthy server never re-discovers | restart the orchestrator (and mcp-worker) after adding tools |
| a late-starting server shows no tools, then heals itself | expected — `ensure_discovered` re-probes empty-card servers on the read path | none; only genuinely-down servers stay empty |

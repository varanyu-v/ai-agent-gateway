---
name: onboard-agent
description: Add a new agent, MCP tool server, or data plane to this platform, or bring an existing/draft one up to repo standard. Use when asked to "add an agent", "create a new MCP server", "onboard a vertical", "wire up a new domain", "register an agent", or when reviewing agent/MCP code for convention drift. Covers the naming worksheet, runtime contracts, both registries, Casbin rows, compose services, and the verification gates.
---

# Onboarding a new agent / MCP server

The long-form tutorial is [docs/onboarding-new-agent-and-mcp.md](../../../docs/onboarding-new-agent-and-mcp.md)
— a complete worked example building a `finance` vertical. **`finance` does not
exist in the repo; it is teaching material.** The real verticals are `world` and
`procurement`. This skill is the operating procedure: follow it, and open the
manual for the full file templates when you need to write one.

Design rationale lives in [docs/agent-services.md](../../../docs/agent-services.md)
and [docs/mcp-services.md](../../../docs/mcp-services.md).

## The four rules everything follows from

1. **Agents decide, they never do.** An agent plans and returns an
   `AgentDecision`. No Kafka, no DB, no tool credentials, no side effects. An
   agent opening a DB connection is always a bug.
2. **The orchestrator enforces.** Every decision is Casbin-checked before any
   side effect. You onboard by *adding policy rows*, never by loosening a check.
3. **MCP is the only tool transport.** Executable decisions set `tool="mcp"`
   and name a registered server + tool. Agents never call MCP servers directly.
4. **Only data planes touch databases.** MCP servers hold no credentials; they
   delegate reads to the one plane owning that database, which applies the final
   guard (single read-only SELECT, table allowlist, row cap, tenant RLS).

## Step 0 — Fill the naming worksheet first

Decide these before writing any code; they must be identical everywhere.

| Item | Rule |
|---|---|
| Agent id | kebab-case, ends `-agent` (`finance-agent`) |
| Workflow | short lowercase word, stamped on spans/audit (`finance`) |
| MCP server id | kebab-case, ends `-mcp` (`finance-mcp`) |
| Permission | `<domain>-db` — **must** equal the Casbin `datasource:<name>` suffix |
| Data plane | `<domain>-db-access`; DB name snake_case (`finance_db`) |
| Env vars | `<DOMAIN>_DATA_PLANE_URL`, `<DOMAIN>_DATABASE_URL` |
| Role | `role:<domain>-analyst` |
| Ports | any free host port — **derive taken ports from docker-compose.yml, don't trust a doc's port map** |

> **The one-string-three-places rule.** The permission name appears in the
> agent's `required_permission`, the MCP tool's `required_permission`, and the
> Casbin object `datasource:<name>`. One typo across those three = a run denied
> with `permission_access_denied`. This is the most common onboarding failure.

Ask the user for the domain name and what the tools should read if not stated.
Don't invent a vertical.

## Step 1 — Data plane (skip unless reading a NEW database)

Skip entirely if the tools reuse `world-db-access` / `procurement-db-access`, or
read no database at all (like `report-mcp`).

Otherwise **use the `onboard-data-access` skill**, the deep reference for the
SQL guard, RLS, and compose wiring. In brief: there is **no code to implement** —
declare an `AgentDataPlane` (`database`, `service_name`, `allowed_tables`,
`url_env`, `max_rows` default 500, `title`) and `create_data_access_app` builds
`/query`, `/health`, the guard and the RLS session.

Reference: [apps/data_access/runtime.py](../../../apps/data_access/runtime.py),
[apps/data_access/world/main.py](../../../apps/data_access/world/main.py).

## Step 2 — MCP server

**Skip this step if the agent reuses existing tools** — an agent needs no new
server if its decisions target an already-registered one (`world-agent` calls
`report-mcp` this way). Reuse means no new wiring and no new Casbin
`mcp:` row; you still need the `datasource:` row if the permission is new.

Otherwise: **use the `onboard-mcp` skill**, which is the deep reference for the
tool-handler contract, argument validation, SQL safety, data planes, and the
four places `MCP_SERVICES` lives. In brief — one async handler per tool
(`(arguments, context) -> dict`), the handler validates everything because the
runtime does not enforce `input_schema`, reads go through `query_data_plane`,
and failures raise `McpToolError`.

Reference: [apps/mcp/runtime.py](../../../apps/mcp/runtime.py),
[apps/mcp/world/main.py](../../../apps/mcp/world/main.py).

## Step 3 — Agent

Two required functions plus two optional pieces:

- `fallback_action(message: str) -> str` — deterministic keyword mapping used
  when the LiteLLM planner is absent or fails. Must return an action declared in
  `AgentDefinition.actions`. Keep it dumb and predictable.
- `decide(planned: PlannedAction, request: AgentRunRequest) -> AgentDecision` —
  the heart. `planned.arguments` is LLM-produced and may be malformed: validate
  and default everything. Never trust `request` for authorization; the
  orchestrator re-checks.
- `PLANNER_GUIDANCE` (recommended) — describes each data action and its exact
  arguments. The planner is told never to invent fields you didn't mention, so
  mention them all.
- `run_async(request, broker)` (optional) — only for multi-step workflows, when
  `decide` returns `action="async"`. See `run_market_brief` in
  [apps/agents/world/main.py](../../../apps/agents/world/main.py).

`AgentDecision.action` is one of:

| action | Meaning | Required fields |
|---|---|---|
| `final` | answer now, no tool | `output` |
| `tool` | one MCP call | `tool="mcp"`, `tool_input={server,name,arguments}`, `required_permission` |
| `approval` | park for a human | `audit_event` |
| `async` | agent drives in background | `run_async` on the definition |
| `deny` | refuse | `reason` |

In `AgentDefinition`, the **`description` feeds the supervisor router's
classifier** — write it as "what questions belong to me", not as marketing.
`tools` is always `("mcp",)`.

Reference: [apps/agents/runtime.py](../../../apps/agents/runtime.py).

## Step 4 — Wiring (where onboarding actually breaks)

- `AGENT_SERVICES` lives in **three** places: `DEFAULT_AGENT_SERVICES` in
  [apps/orchestrator/agent_registry.py](../../../apps/orchestrator/agent_registry.py)
  (the code fallback used when the env var is unset — the manual omits this
  one), `services.orchestrator.environment` in compose, and `.env` +
  `.env.example`. Format is `id=base-url,id=base-url`.
- `MCP_SERVICES` lives in **four** places — the code default plus
  orchestrator **and** mcp-worker compose env, plus `.env`/`.env.example`.
  Updating one and not the others produces
  `No MCP server registered for '<id>'`. See the `onboard-mcp` skill.
- A value set in `.env` **overrides** the compose default entirely, so the
  `.env` value must itself list every entry.
- Add the new MCP server to `mcp-worker.depends_on`, health-gated.
- Casbin rows in `policy/casbin_policy.csv` (`p, subject, tenant, object, action`):
  `agent:<id> invoke`, `agent:assistant invoke` (for the router),
  `mcp:<server-id> execute`, `datasource:<perm> read` — per role.
- Casbin subjects are the **JWT role names verbatim**, including the `role:`
  prefix. For a new role either grant to an existing role (`role:data-admin` —
  quickest, testable immediately) or add it to
  `docker/keycloak/ptvn-realm.json` and recreate Keycloak (the realm imports
  only on a fresh container).
- Compose service needs the standard `<<: *app` / `<<: *app-env` anchors and a
  health check.
- The supervisor router needs **no code** — registry entry + `agent:assistant`
  grant is enough.

## Step 5 — Verify before claiming done

Run these; don't assert success from reading code alone.

```bash
curl -s http://localhost:8001/internal/mcp | python3 -m json.tool     # server + tools listed
curl -s http://localhost:8001/internal/agents | python3 -m json.tool  # card listed
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests           # full suite
```

> The manual's `python -m unittest discover -s tests -t . -v` **fails** on
> Python 3.11+: `tests/` has no `__init__.py`, so `-t .` reports "Start
> directory is not importable". Bare `python` also misses the deps.

Then an end-to-end run through the gateway with a real Keycloak token, and a
router run against `assistant` with a domain question. Manual §6.8 has the
token-fetch and run-polling commands verbatim.

Tests: `decide` and `fallback_action` are pure functions — test directly, no
HTTP (`tests/test_agents.py`). MCP servers use `httpx.ASGITransport` plus the
`FakeDataPlane` transport (`tests/test_mcp.py`); assert happy path, invalid
arguments becoming `isError`, and missing identity headers.

## Final checklist

**MCP server**
- [ ] `apps/mcp/<name>/__init__.py` + `main.py` (handlers, `McpTool`s, definition, `create_mcp_app`)
- [ ] (new DB) init SQL + `apps/data_access/<name>/main.py` + `<name>-db-init` / `<name>-db-access` compose + `<DOMAIN>_DATABASE_URL`
- [ ] Compose `<name>-mcp` with health check + `<DOMAIN>_DATA_PLANE_URL`
- [ ] `MCP_SERVICES` in **orchestrator + mcp-worker** compose, `.env`, `.env.example`
- [ ] mcp-worker `depends_on` includes it
- [ ] Casbin `mcp:<id> execute` + `datasource:<perm> read` per role
- [ ] Card + `tools/call` curl-verified; appears in `/internal/mcp`
- [ ] Tests added; suite green

**Agent**
- [ ] `apps/agents/<name>/__init__.py` + `main.py` (`fallback_action`, `decide`, `PLANNER_GUIDANCE`, optional `run_async`, definition, `create_agent_app`)
- [ ] Compose service with health check
- [ ] `AGENT_SERVICES` in orchestrator compose, `.env`, `.env.example`
- [ ] Casbin `agent:<id> invoke` + `agent:assistant invoke` per role
- [ ] Keycloak role granted (existing role, or new realm role + user + recreate)
- [ ] Appears in `/internal/agents`; end-to-end run completes via gateway
- [ ] Router reaches it from `assistant`
- [ ] Tests added; suite green

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `403 User cannot access this agent` | missing `agent:<id> invoke`, or JWT lacks the role | add the row; check the `roles` claim matches the subject exactly, `role:` prefix included |
| `tool_access_denied` | missing `mcp:<id> execute` | add it for the role |
| `permission_access_denied` | missing `datasource:<perm> read`, or the permission string differs | apply the one-string-three-places rule |
| `No MCP server registered for '<id>'` | `MCP_SERVICES` updated on only one of orchestrator/mcp-worker, or `server_id` ≠ registry key | update both, restart both |
| `/internal/mcp` shows 502/504 | server down, wrong URL/port, or the card endpoint errors | curl the card directly; check logs |
| `isError`: "x-tenant-id and x-user-id headers are required" | calling `/mcp` without identity headers | add them (the worker forwards automatically in the real flow) |
| plane 404 "only serves '<x>'" | wrong `database` argument | must equal the plane's `AgentDataPlane.database` |
| plane 403 on a valid SELECT | table not in `allowed_tables` | add it deliberately, not reflexively |
| agent always falls back | `LITELLM_MODEL`/`LITELLM_API_KEY` unset, or LLM returned an action outside `actions` | check the `agent.llm_plan` span in Langfuse |
| router never picks the agent | card `description` too vague | rewrite as "what questions belong to me"; verify a direct run works first |

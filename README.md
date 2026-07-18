# AI Agent Gateway Architecture Sample

This repo is a runnable local sample of an enterprise AI agent gateway. It shows
how a browser client can authenticate with Keycloak, call an agent gateway, route
work through an orchestrator to standalone agent services, publish tool events
through Kafka, and read data through guarded database sources.

Agents are separate services, one per agent, discovered by the orchestrator
through a standard agent card. Adding an agent to the platform is configuration
only — no orchestrator code changes. See `docs/agent-services.md` for the
multi-agent design and integration contract.

The local demo has two database sources:

- World DB from `ghusta/postgres-world-db:2.15.0`.
- A seeded `procurement_db` database created by `docker/postgres/init/02-create-procurement-database.sql`.

## Quick Start

Start the full stack:

```bash
docker compose up --build -d
docker compose ps
```

Open the test console:

```text
http://localhost:8000/ui
```

Open observability tools:

```text
http://localhost:3000    Grafana, admin/admin
http://localhost:9090    Prometheus metrics
http://localhost:3100    Loki logs API
http://localhost:3200    Tempo API, no standalone browser UI
```

Recommended first run:

1. Select `World analyst`.
2. Press `Login`.
3. Click `World Market Hotspots`.
4. Press `Run agent`.
5. Inspect `Agent input`, `Agent output`, `SQL response`, and the raw response JSON.

Watch the backend while testing:

```bash
docker compose logs -f gateway orchestrator world-agent procurement-agent sql-worker report-worker world-db-access procurement-db-access
```

## What Is Included

- `apps/gateway`: the edge policy-enforcement point. Validates Keycloak JWTs, checks agent access through Casbin, derives source permissions, and forwards trusted context headers. Adds per-user rate limiting, `Idempotency-Key` replay protection, input guards, a circuit-breaker-backed orchestrator client with clean 502/503/504 mapping, request-id correlation, and `/healthz` / `/readyz` endpoints. See `docs/gateway-design.md` for the full design.
- `apps/orchestrator`: routes runs to external agent services through an agent registry, enforces Casbin source/tool policy on every agent decision before dispatch, emits Kafka events, tracks run status, and records approvals. Agents are declared in `AGENT_SERVICES` and discovered through their agent cards. Also serves the virtual `assistant` supervisor agent (`apps/orchestrator/router.py`): general questions are answered directly by the LLM, while procurement/world questions are classified and delegated to the matching agent service under the caller's Casbin agent-invoke policy.
- `apps/agents`: standalone agent services built on a shared runtime (`apps/agents/runtime.py`). Each agent runs its own LangGraph workflow and LiteLLM planner, exposes `/.well-known/agent-card` and `POST /runs`, and returns a decision; it never touches Kafka or databases directly. `apps/agents/world` and `apps/agents/procurement` are the two current agents. See `docs/agent-services.md`.
- `apps/authz.py` and `policy/casbin_*`: shared Casbin model and policy used by the gateway and orchestrator.
- `apps/workers`: consumes `tool.requested` events and publishes `tool.completed` events. The SQL worker routes each query to the data plane that owns its database, using the `DATA_PLANES` registry. The MCP worker routes `tool="mcp"` calls to the MCP server named in the decision, using the `MCP_SERVICES` registry.
- `apps/data_access`: per-agent database access layers built on a shared runtime (`apps/data_access/runtime.py`). Each plane holds credentials for only its own database and enforces the final read-only, table-allowlisted, tenant-scoped SQL guard. `apps/data_access/world` (`world-db-access`) and `apps/data_access/procurement` (`procurement-db-access`) are the two current planes.
- `apps/mcp`: standalone MCP (Model Context Protocol) tool servers built on a shared runtime (`apps/mcp/runtime.py`). Each server exposes `/.well-known/mcp-card` for discovery and a JSON-RPC `/mcp` endpoint (`initialize`, `tools/list`, `tools/call`), holds no database credentials, and delegates reads to the owning data plane. Servers are declared in `MCP_SERVICES` and discovered by the orchestrator's `McpRegistry` (`GET /internal/mcp`), mirroring the agent registry. `apps/mcp/world` and `apps/mcp/procurement` are the two example servers. See `docs/mcp-services.md`.
- `apps/observability.py`: configures OpenTelemetry traces and metrics for the gateway, orchestrator, agent, worker, and data-plane steps.
- `apps/frontend/index.html`: local browser test console with login, role visibility, example runs, SQL response rendering, agent input/output, and human approval.
- `docker/keycloak/ptvn-realm.json`: local realm, roles, client, and seeded demo users.
- `docker/otel/collector-config.yaml`: local OTLP collector pipeline that forwards traces to Tempo and exposes metrics for Prometheus.
- `docker/prometheus/prometheus.yml`: Prometheus scrape config for app metrics exported by the collector.
- `docker/tempo/tempo.yaml`: local Tempo storage for trace search through Grafana.
- `docker/loki/loki.yaml` and `docker/promtail/promtail.yaml`: local Loki log storage and Compose container log shipping.
- `docker/grafana/provisioning`: Grafana datasource provisioning for Tempo, Prometheus, and Loki.
- `docker/postgres/init/02-create-procurement-database.sql`: idempotent procurement database and seed data initializer.
- `sql/rls_example.sql`: illustrative RLS policies for production hardening ideas.

## Architecture

This view shows the service path and the access model together. Keycloak issues
coarse persona roles, Casbin maps those role subjects to agent, tool, and
data-source objects, and each agent's own data plane (`apps/data_access`)
enforces the final guarded SQL boundary before its database is read.
OpenTelemetry spans and metrics are emitted by the existing services and
forwarded through the local collector to Tempo and Prometheus. Compose container
logs are shipped to Loki. No extra agent or tool path is added.

```mermaid
flowchart TD
    frontend["Browser Test Console"]

    subgraph identity["Identity and roles"]
        keycloak["Keycloak OIDC"]
        persona_roles["Persona roles<br/>role:world-analyst<br/>role:procurement-analyst<br/>role:source-auditor<br/>role:data-admin"]
        casbin_policy["Casbin policy<br/>policy/casbin_model.conf<br/>policy/casbin_policy.csv"]
    end

    subgraph edge["Gateway access checks"]
        gateway["Gateway API<br/>JWT validation<br/>Casbin agent invoke check<br/>rate limit + idempotency<br/>circuit breaker to orchestrator"]
        allowed_permissions["Trusted policy context<br/>x-policy-subjects<br/>x-allowed-permissions"]
    end

    subgraph orchestration["Orchestration and policy"]
        orchestrator["Orchestrator API<br/>agent registry + agent cards<br/>Casbin tool/source checks on decisions<br/>Kafka dispatch + run state<br/>Human approval state"]
        denied["Denied run<br/>permission/tool audit event"]
    end

    subgraph agents["Agent services (one service per agent)"]
        world_agent["world-agent :8004<br/>LangGraph + LiteLLM planner<br/>actions: sql, report, brief, approval<br/>requires: world-db"]
        procurement_agent["procurement-agent :8005<br/>LangGraph + LiteLLM planner<br/>actions: sql, approval<br/>requires: procurement-db"]
    end

    subgraph tools["Tool execution"]
        kafka["Kafka<br/>agent.requested<br/>tool.requested<br/>tool.completed<br/>audit.events"]
        workers["Workers<br/>SQL worker<br/>Report worker"]
    end

    subgraph db_layer["Per-agent data planes (one per agent)<br/>apps/data_access"]
        plane_router{"DATA_PLANES route<br/>by database name"}
        world_plane["world-db-access :8006<br/>SQL guard: one read-only SELECT<br/>allow: city, country,<br/>country_language, country_flag<br/>max rows: 500<br/>holds only WORLD_DATABASE_URL"]
        procurement_plane["procurement-db-access :8007<br/>SQL guard: one read-only SELECT<br/>allow: suppliers, purchase_orders,<br/>supplier_summary<br/>max rows: 500<br/>holds only PROCUREMENT_DATABASE_URL"]
        blocked_query["Blocked by data plane<br/>write SQL, multi statement,<br/>foreign DB, or disallowed table"]
    end

    subgraph sources["Database sources"]
        worlddb["Postgres World DB<br/>city<br/>country<br/>country_language<br/>country_flag"]
        procurementdb["Procurement DB<br/>suppliers<br/>purchase_orders<br/>supplier_summary"]
    end

    subgraph telemetry["OpenTelemetry monitoring"]
        collector["OTLP Collector<br/>grpc 4317<br/>http 4318"]
        tempo["Tempo<br/>localhost:3200"]
        prometheus["Prometheus<br/>localhost:9090"]
        loki["Loki<br/>localhost:3100"]
        langfuse["Langfuse<br/>localhost:3001<br/>Logical AI trace"]
        promtail["Promtail<br/>Compose container logs"]
        grafana["Grafana<br/>localhost:3000"]
    end

    frontend -->|"Login"| keycloak
    keycloak -->|"Access token"| frontend
    keycloak --> persona_roles
    persona_roles --> casbin_policy
    frontend -->|"Bearer token"| gateway
    casbin_policy -->|"authorizes selected agent"| gateway
    casbin_policy -->|"maps source/tool access"| allowed_permissions
    gateway --> allowed_permissions
    gateway -->|"Trusted context"| orchestrator
    allowed_permissions -->|"Casbin subjects + source context"| orchestrator
    orchestrator -->|"POST /runs (agent card discovery)"| world_agent
    orchestrator -->|"POST /runs (agent card discovery)"| procurement_agent
    world_agent -->|"decision: tool/approval/async"| orchestrator
    world_agent -.->|"tool-broker callbacks (async runs)"| orchestrator
    procurement_agent -->|"decision: tool/approval"| orchestrator
    orchestrator -->|"missing permission/tool"| denied
    orchestrator -->|"Tool request events"| kafka
    kafka -->|"Consume"| workers
    workers -->|"SQL tool routes to owning plane"| plane_router
    plane_router -->|"database: world → POST /query"| world_plane
    plane_router -->|"database: procurement → POST /query"| procurement_plane
    plane_router -->|"no plane for database"| blocked_query
    world_plane -->|"set app.tenant_id/app.user_id<br/>wrapped SELECT limit 500"| worlddb
    procurement_plane -->|"set app.tenant_id/app.user_id<br/>wrapped SELECT limit 500"| procurementdb
    world_plane -->|"invalid SQL, foreign or<br/>disallowed table"| blocked_query
    procurement_plane -->|"invalid SQL, foreign or<br/>disallowed table"| blocked_query
    workers -->|"Tool result events"| kafka
    kafka -->|"Resume run status"| orchestrator
    frontend -->|"Poll /runs/{run_id}"| gateway
    gateway -->|"Run status"| orchestrator
    gateway -. "traces + metrics" .-> collector
    orchestrator -. "traces + metrics" .-> collector
    world_agent -. "traces + metrics" .-> collector
    procurement_agent -. "traces + metrics" .-> collector
    orchestrator -. "agent-run root + tool spans" .-> langfuse
    world_agent -. "planner generations" .-> langfuse
    procurement_agent -. "planner generations" .-> langfuse
    workers -. "traces + metrics" .-> collector
    world_plane -. "traces + metrics" .-> collector
    procurement_plane -. "traces + metrics" .-> collector
    collector -->|"traces"| tempo
    prometheus -->|"scrapes metrics"| collector
    promtail -->|"pushes logs"| loki
    grafana --> tempo
    grafana --> prometheus
    grafana --> loki
```

| Layer | What it decides | Current examples |
| --- | --- | --- |
| Keycloak roles | Which persona subjects appear in the JWT | `role:world-analyst`, `role:procurement-analyst`, `role:source-auditor`, `role:data-admin` |
| Casbin policy | Which subjects can use which agent, tool, or source object | `agent:world-agent` `invoke`; `datasource:world-db` `read`; `tool:sql` `execute` |
| Gateway | Whether the user can invoke the requested agent, and which policy subjects are forwarded | `x-policy-subjects`, `x-allowed-permissions: world-db,procurement-db` |
| Agent service | Which workflow action and tool the run should use (a decision, no side effects) | `world-agent` can plan SQL, report, or approval; `procurement-agent` can plan SQL or approval |
| Orchestrator policy check | Whether the agent's decision is executed or denied | World data needs `datasource:world-db`; procurement data needs `datasource:procurement-db`; missing access emits an audit event |
| Agent data plane | The last-mile SQL guard against that agent's own database | `world-db-access` allows read-only SELECTs over world tables only; `procurement-db-access` the same for procurement; each holds only its own credentials |

## Local Services

| Service | URL / port | Purpose |
| --- | --- | --- |
| Keycloak | `http://localhost:8080` | Local OIDC issuer and seeded users |
| Gateway | `http://localhost:8000` | Public API and test console |
| Test console | `http://localhost:8000/ui` | Browser UI for end-to-end testing |
| Orchestrator | `http://localhost:8001` | Internal run routing, policy enforcement, and run state API |
| World agent | `http://localhost:8004` | Standalone world-agent service (`/.well-known/agent-card`) |
| Procurement agent | `http://localhost:8005` | Standalone procurement-agent service (`/.well-known/agent-card`) |
| World data plane | `http://localhost:8006` | `world-db-access`: guarded SQL over the world database only |
| Procurement data plane | `http://localhost:8007` | `procurement-db-access`: guarded SQL over the procurement database only |
| World MCP | `http://localhost:8010` | `world-mcp`: MCP tools over the world database (`/.well-known/mcp-card`) |
| Procurement MCP | `http://localhost:8011` | `procurement-mcp`: MCP tools over the procurement database (`/.well-known/mcp-card`) |
| Kafka | `localhost:29092` | Host-visible Kafka listener |
| Postgres | `localhost:5432` | World DB plus seeded `procurement_db` |
| OTLP collector | `localhost:4317`, `localhost:4318`, `localhost:9464` | Receives OpenTelemetry traces and metrics; exposes Prometheus scrape output |
| Tempo | `http://localhost:3200` | Trace storage API used by Grafana; `/` may return 404 |
| Prometheus | `http://localhost:9090` | Metrics store scraping the collector's Prometheus exporter |
| Loki | `http://localhost:3100` | Log storage API used by Grafana |
| Promtail | internal | Ships Docker Compose container logs to Loki |
| Grafana | `http://localhost:3000` | Observability UI with Tempo, Prometheus, and Loki datasources |
| Langfuse | `http://localhost:3001` | Self-hosted LLM trace UI for planner generations |
| Langfuse MinIO | `http://localhost:19090` | Local object storage endpoint used by Langfuse media/export flows |

The Postgres service uses a PG18-safe volume mount:

```text
postgres-world-pg18-data:/var/lib/postgresql
```

The `procurement-db-init` service runs on startup and creates or refreshes the
procurement schema without requiring the World DB volume to be deleted.

## Demo Users

| Username | Password | Good first test | Roles |
| --- | --- | --- | --- |
| `world-analyst` | `world-password` | World DB SQL and report | `role:world-analyst` |
| `procurement-analyst` | `procurement-password` | Procurement DB SQL | `role:procurement-analyst` |
| `source-auditor` | `auditor-password` | Permission denial | `role:source-auditor` |
| `data-admin` | `data-admin-password` | Human approval | `role:data-admin` |

All seeded users have `tenant_id=demo-tenant`. The `agent-frontend` client adds
the `agent-gateway` audience expected by the gateway.

## Run Agent Examples

The Run Agent panel provides these examples:

| Example | User | Agent | Message | Expected result |
| --- | --- | --- | --- | --- |
| World Market Hotspots | `world-analyst` | `world-agent` | `show the largest cities by population with country context` | SQL rows from World DB |
| Market Entry Report | `world-analyst` | `world-agent` | `generate a world market entry report` | Report tool completes with a sample download URL |
| Procurement Spend Radar | `procurement-analyst` | `procurement-agent` | `rank suppliers by total purchase spend and risk` | SQL rows from `procurement_db` |
| Source Permission Denial | `source-auditor` | `procurement-agent` | `rank suppliers by total purchase spend and risk` | `denied` because Casbin allows the agent but not `datasource:procurement-db` |
| Human Approval Gate | `data-admin` | `procurement-agent` | `remove blocked supplier records from the procurement source` | `requires_approval` and an approval button |

Approval currently records the approval and emits audit state. This sample does
not execute destructive follow-up actions after approval.

## Workflows And Permissions

The gateway and orchestrator enforce separate Casbin-backed checks:

- Agent access: `agent:world-agent` or `agent:procurement-agent` with the `invoke` action.
- Source permission access: `datasource:world-db` or `datasource:procurement-db` with the `read` action.
- Tool execution access: `tool:sql` or `tool:report` with the `execute` action.

The seeded realm contains only coarse persona roles. Fine-grained access is not
duplicated in Keycloak; it lives in Casbin policy.

The Casbin model lives in `policy/casbin_model.conf`, and default policies live
in `policy/casbin_policy.csv`. The gateway evaluates agent invocation and
forwards `x-policy-subjects` plus `x-allowed-permissions`; the orchestrator uses
those subjects to evaluate source and tool policy on the agent's decision
before emitting a `tool.requested` event. Agent services only propose actions —
they cannot bypass policy because all side effects run through the
orchestrator.

Workflow behavior:

- `world-agent` (own service, port 8004) can plan `sql`, `report`, `brief`, or
  `approval`. A `brief` becomes an async run: the agent drives a multi-step
  workflow (SQL, then a report) through the orchestrator's tool-broker
  callback API, with every step policy-checked (see `docs/agent-services.md`).
- `procurement-agent` (own service, port 8005) can plan `sql` or `approval`.
- Each agent service plans with LiteLLM when `LITELLM_MODEL` and `LITELLM_API_KEY` are set.
- Deterministic fallback routing is used when LiteLLM is not configured or the model call fails.

### Adding A New Agent

Agents integrate through configuration, not orchestrator code changes:

1. Define the agent in its own module with an `AgentDefinition` (identity,
   workflow, allowed actions, fallback routing, and a `decide` function) and
   build it with `create_agent_app` from `apps/agents/runtime.py` — or
   implement the same HTTP contract in any language.
2. Run it as its own service (own container, own port, own scaling).
3. Register it with the orchestrator by appending `agent-id=base-url` to
   `AGENT_SERVICES`; the orchestrator discovers its workflow and capabilities
   from `/.well-known/agent-card`.
4. Grant access in Casbin: `agent:{agent-id}` `invoke` for users, plus any
   `datasource:*` / `tool:*` rules the agent's decisions require.

The full contract (agent card, run request, decision shape, trace
propagation) is documented in `docs/agent-services.md`.

## Gateway Behaviors

`apps/gateway` is the only public entry point and applies these controls in
order (full design in `docs/gateway-design.md`):

1. Request correlation: a valid inbound `x-request-id` is honored, otherwise
   one is generated; it is echoed on every response and becomes the `run_id`.
2. JWT validation with clean failures: bad tokens return `401` with
   `WWW-Authenticate`, an unreachable JWKS endpoint returns `503`.
3. Input guards: empty messages and messages over `GATEWAY_MAX_MESSAGE_CHARS`
   return `422` before any quota or policy work.
4. Rate limiting on run creation only: a token bucket per `tenant:user`
   returns `429` with `Retry-After` when exhausted. Polling is unmetered.
5. Idempotent run creation: send an `Idempotency-Key` header to make retries
   safe. Replays return the original response with `x-idempotent-replay: true`;
   reusing a key with a different payload returns `409`.
6. Casbin agent invoke check, then trusted context forwarding (unchanged).
7. Resilient upstream calls: one pooled HTTP client plus a circuit breaker.
   While the orchestrator is down the gateway fails fast with `503` and
   `Retry-After`; timeouts map to `504`, unreachable/5xx to `502`.
8. Health endpoints: `/healthz` (liveness) and `/readyz` (checks the
   orchestrator through the breaker).

Retry a run safely:

```bash
curl -sS -X POST http://localhost:8000/agents/world-agent/runs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-1" \
  -d '{"message":"show the largest cities by population with country context"}'
```

Gateway tunables (defaults shown in `.env.example`): `GATEWAY_RATE_LIMIT_ENABLED`,
`GATEWAY_RUN_RATE_PER_MINUTE`, `GATEWAY_RUN_RATE_BURST`,
`GATEWAY_MAX_MESSAGE_CHARS`, `GATEWAY_IDEMPOTENCY_TTL_SECONDS`,
`GATEWAY_UPSTREAM_CONNECT_TIMEOUT_SECONDS`, `GATEWAY_UPSTREAM_READ_TIMEOUT_SECONDS`,
`GATEWAY_BREAKER_FAILURE_THRESHOLD`, `GATEWAY_BREAKER_RESET_SECONDS`,
`GATEWAY_JWT_LEEWAY_SECONDS`.

## Observability Monitoring

The app emits distributed traces and metrics for the existing runtime path only.
It does not add new agents, workflows, or tools.

Compose sends spans and metrics to the local collector. The collector exports
traces to Tempo and exposes app metrics for Prometheus on port `9464`:

```bash
OTEL_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
OTEL_SERVICE_NAMESPACE=ai-agent-gateway
```

For local processes outside Compose, use the host collector endpoint:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

Langfuse LLM tracking uses a dedicated OpenTelemetry tracer. This Compose stack
runs a self-hosted Langfuse UI at `http://localhost:3001`. The orchestrator
owns the logical `agent-run` root span and the selected tool execution with its
input, result, and final run output; each agent service emits its
`agent.llm_plan` planner generation under that same root by joining the
Langfuse trace context propagated in the `x-langfuse-traceparent` header. The
Langfuse trace carries the same trace ID as Tempo plus filterable request,
tenant, agent, and workflow metadata.
Gateway HTTP, JWT, polling, worker internals, database spans, and other
infrastructure details remain only in Tempo, avoiding complete-trace
duplication while preserving model, input, output, and usage analytics in
Langfuse:

```bash
LANGFUSE_ENABLED=true
LANGFUSE_PUBLIC_KEY=pk-lf-local-ai-agent-gateway
LANGFUSE_SECRET_KEY=sk-lf-local-ai-agent-gateway
LANGFUSE_BASE_URL=http://langfuse-web:3000
LANGFUSE_PUBLIC_URL=http://localhost:3001
LANGFUSE_CAPTURE_CONTENT=true
```

The local Langfuse project is initialized with the same public/secret keys and
an admin login from `.env.example` (`admin@example.com` / `admin-password`).
These local defaults are not production secrets. Set `LANGFUSE_CAPTURE_CONTENT=false`
to send prompt/response lengths instead of the raw planner messages. If you run
the app processes outside Compose but keep Langfuse in Docker, override
`LANGFUSE_BASE_URL=http://localhost:3001`. For Langfuse Cloud, set
`LANGFUSE_BASE_URL` to your Cloud region URL and use keys from that project
instead of the local defaults.

After running an agent, open `http://localhost:3000` for Grafana
(`admin` / `admin`). Grafana starts with `Tempo`, `Prometheus`, and `Loki`
datasources already provisioned.
Search services such as
`gateway`, `orchestrator`, `world-agent`, `procurement-agent`, `sql-worker`,
`report-worker`, `world-db-access`, and `procurement-db-access`.
Tempo is an API-backed trace store in this stack, so the browser UI for Tempo
traces is Grafana Explore, not `http://localhost:3200/`.

For a Tempo panel or Explore query, select the `Tempo` datasource (`uid: tempo`).
For metrics, use the `Prometheus` datasource (`uid: prometheus`). For logs, use
the `Loki` datasource (`uid: loki`) and filter by labels such as
`{service="gateway"}` or `{service="orchestrator"}`.

Useful spans include:

- `gateway.agent_call`: user request, agent authorization, orchestrator response.
- `orchestrator.agent_run`: trusted request handling and decision execution.
- `orchestrator.agent_invoke`: the HTTP call to the selected agent service.
- `agent.plan.world` / `agent.plan.procurement`: the agent service's planning step.
- `agent.choose_plan_action`: LiteLLM or fallback routing decision inside the agent.
- `kafka.publish`: `agent.requested`, `tool.requested`, `tool.completed`, or `audit.events` emission.
- `worker.sql_tool` and `worker.report_tool`: existing tool execution steps.
- `data_access.query`, `data_access.validate_sql`, and `data_access.execute_sql`: the owning data plane's SQL guard and database read.
- `gateway.run_status_response`: the user-facing poll that returns the final run result.

Trace context is carried across HTTP automatically and across Kafka in the event
payload's `trace_context` field. The initial agent-call trace continues through
the async worker completion path. Status polling and approvals are separate HTTP
calls, and they include the same `run_id` / `request_id` attributes for search
and correlation in Grafana and Tempo. Container logs are shipped by Promtail to
Loki with Compose labels, including the `service` label.

## Database Access Layer

Each agent owns a dedicated data plane — a small FastAPI service built on
`apps/data_access/runtime.py` that holds credentials for **only** its own
database. `world-db-access` (:8006) can reach the world database and
`procurement-db-access` (:8007) the procurement database; neither can touch the
other's data. This is the least-privilege, credential-isolated version of a
data-access layer.

In the full agent flow, the gateway first checks `agent:{agent_id}` `invoke`
through Casbin, the orchestrator checks the run's data-source and tool policy,
and only then does the SQL worker route `POST /query` to the plane that owns the
database (via the `DATA_PLANES` registry).

Each plane's job is the database boundary:

1. Refuse any request whose `database` is not the one this plane serves (`404`).
2. Parse SQL with `sqlglot`.
3. Allow exactly one read-only `SELECT` statement.
4. Reject table names outside that plane's allowlist.
5. Set `app.tenant_id` and `app.user_id` in the Postgres session.
6. Wrap the query with a max row limit before returning rows.

Request shape:

```json
{
  "database": "world",
  "sql": "select name, population from city order by population desc limit 3"
}
```

Required trusted headers:

```text
x-tenant-id: demo-tenant
x-user-id: demo-user
```

Response shape:

```json
{
  "rows": []
}
```

Compose gives each plane only its own database URL, and the SQL worker learns
where to route each query from `DATA_PLANES`:

```bash
# world-db-access service only
WORLD_DATABASE_URL=postgresql://world:world123@postgres:5432/world-db
# procurement-db-access service only
PROCUREMENT_DATABASE_URL=postgresql://world:world123@postgres:5432/procurement_db
# sql-worker only (database=base-url routing)
DATA_PLANES=world=http://world-db-access:8006,procurement=http://procurement-db-access:8007
```

| Plane | Source permission | Owns database | Env var | Allowed tables | Max rows |
| --- | --- | --- | --- | --- | --- |
| `world-db-access` | `world-db` | `world` | `WORLD_DATABASE_URL` or `DATABASE_URL` | `city`, `country`, `country_language`, `country_flag` | 500 |
| `procurement-db-access` | `procurement-db` | `procurement` | `PROCUREMENT_DATABASE_URL` | `suppliers`, `purchase_orders`, `supplier_summary` | 500 |

A request for a database the plane does not own returns `404`. A missing
database URL returns `503`. Invalid SQL returns `400`, and disallowed tables
return `403`. If no plane is registered for a database, the SQL worker fails the
run rather than routing the query elsewhere.

## API Smoke Tests

Get a World DB token:

```bash
TOKEN="$(
  curl -sS -X POST http://localhost:8080/realms/ptvn/protocol/openid-connect/token \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "client_id=agent-frontend" \
    -d "grant_type=password" \
    -d "username=world-analyst" \
    -d "password=world-password" \
  | python -c "import json, sys; print(json.load(sys.stdin)['access_token'])"
)"
```

Run the World DB SQL example:

```bash
curl -sS -X POST http://localhost:8000/agents/world-agent/runs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message":"show the largest cities by population with country context"}'
```

Poll a run:

```bash
RUN_ID=<run_id from the previous response>

curl -sS http://localhost:8000/runs/$RUN_ID \
  -H "Authorization: Bearer $TOKEN"
```

Query World DB directly:

```bash
curl -sS -X POST http://localhost:8003/query \
  -H "Content-Type: application/json" \
  -H "x-tenant-id: demo-tenant" \
  -H "x-user-id: demo-user" \
  -d '{"database":"world","sql":"select city.name as city, country.name as country, country.continent, city.population from city join country on country.code = city.country_code order by city.population desc limit 3"}'
```

Query Procurement DB directly:

```bash
curl -sS -X POST http://localhost:8003/query \
  -H "Content-Type: application/json" \
  -H "x-tenant-id: demo-tenant" \
  -H "x-user-id: demo-user" \
  -d '{"database":"procurement","sql":"select supplier_name, category, country, total_spend, order_count, risk_level from supplier_summary order by total_spend desc limit 3"}'
```

Trigger an approval request:

```bash
ADMIN_TOKEN="$(
  curl -sS -X POST http://localhost:8080/realms/ptvn/protocol/openid-connect/token \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "client_id=agent-frontend" \
    -d "grant_type=password" \
    -d "username=data-admin" \
    -d "password=data-admin-password" \
  | python -c "import json, sys; print(json.load(sys.stdin)['access_token'])"
)"

curl -sS -X POST http://localhost:8000/agents/procurement-agent/runs \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message":"remove blocked supplier records from the procurement source"}'
```

Approve that run:

```bash
RUN_ID=<requires_approval run_id>

curl -sS -X POST http://localhost:8000/runs/$RUN_ID/approve \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

Test source permission denial:

```bash
AUDITOR_TOKEN="$(
  curl -sS -X POST http://localhost:8080/realms/ptvn/protocol/openid-connect/token \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "client_id=agent-frontend" \
    -d "grant_type=password" \
    -d "username=source-auditor" \
    -d "password=auditor-password" \
  | python -c "import json, sys; print(json.load(sys.stdin)['access_token'])"
)"

curl -sS -X POST http://localhost:8000/agents/procurement-agent/runs \
  -H "Authorization: Bearer $AUDITOR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message":"rank suppliers by total purchase spend and risk"}'
```

Expected denial:

```json
{
  "status": "denied",
  "denied_reason": "User cannot use data source permission: procurement-db"
}
```

## LiteLLM Planning

Each agent service calls an OpenAI-compatible LiteLLM chat completions endpoint
for request planning when these variables are configured:

```bash
LITELLM_BASE_URL=http://localhost:4000/v1
LITELLM_MODEL=your-litellm-model-name
LITELLM_API_KEY=your-litellm-secret-key
LITELLM_TIMEOUT_SECONDS=30
LANGFUSE_PUBLIC_KEY=pk-lf-local-ai-agent-gateway
LANGFUSE_SECRET_KEY=sk-lf-local-ai-agent-gateway
LANGFUSE_BASE_URL=http://langfuse-web:3000
```

For Docker Compose on macOS or Windows, use a host-reachable URL such as:

```bash
LITELLM_BASE_URL=http://host.docker.internal:4000/v1
```

## Local Development Without Compose

Install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Run services in separate terminals after exporting environment variables from
`.env.example`:

```bash
uvicorn apps.gateway.main:app --host 0.0.0.0 --port 8000
uvicorn apps.orchestrator.main:app --host 0.0.0.0 --port 8001
uvicorn apps.agents.world.main:app --host 0.0.0.0 --port 8004
uvicorn apps.agents.procurement.main:app --host 0.0.0.0 --port 8005
uvicorn apps.data_access.world.main:app --host 0.0.0.0 --port 8006
uvicorn apps.data_access.procurement.main:app --host 0.0.0.0 --port 8007
uvicorn apps.mcp.world.main:app --host 0.0.0.0 --port 8010
uvicorn apps.mcp.procurement.main:app --host 0.0.0.0 --port 8011
python -m apps.workers.sql_worker
python -m apps.workers.report_worker
python -m apps.workers.mcp_worker
```

Kafka, Keycloak, and Postgres are still required for the full flow.

## Common Operations

Rebuild app containers after code changes:

```bash
docker compose up --build -d gateway orchestrator world-agent procurement-agent world-db-access procurement-db-access sql-worker report-worker
```

Restart everything:

```bash
docker compose up --build -d
```

Open the self-hosted Langfuse UI:

```bash
open http://localhost:3001
```

Inspect World DB directly:

```bash
docker compose exec -T postgres psql -U world -d world-db -c "select count(*) from city"
```

Inspect Procurement DB directly:

```bash
docker compose exec -T postgres psql -U world -d procurement_db -c "select * from supplier_summary order by total_spend desc"
```

Re-run only the procurement seed:

```bash
docker compose up --force-recreate procurement-db-init
```

Follow useful logs:

```bash
docker compose logs -f gateway orchestrator world-agent procurement-agent sql-worker report-worker world-db-access procurement-db-access langfuse-web langfuse-worker postgres procurement-db-init
```

If you edit `docker/keycloak/ptvn-realm.json` after Keycloak has already imported
the realm, recreate the Keycloak container before retesting realm changes.

## Production Notes

This is a local architecture sample. Before production use:

- Replace the in-memory LangGraph checkpointer/store with durable persistence.
- Replace sample report URLs with a real report artifact store.
- Replace permissive RLS examples with tenant-scoped policies on owned tables or views.
- Use real secrets management for Keycloak, LiteLLM, Kafka, and database credentials.
- Add TLS, structured audit retention, observability, and deployment-specific network policy.

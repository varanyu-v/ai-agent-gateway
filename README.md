# AI Agent Gateway Architecture Sample

This repo is a runnable local sample of an enterprise AI agent gateway. It shows
how a browser client can authenticate with Keycloak, call an agent gateway, route
work through an orchestrator, publish tool events through Kafka, and read data
through guarded database sources.

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
docker compose logs -f gateway orchestrator sql-worker report-worker db-access
```

## What Is Included

- `apps/gateway`: validates Keycloak JWTs, checks agent access through Casbin, derives source permissions, and forwards trusted context headers.
- `apps/orchestrator`: runs LangGraph workflows, enforces Casbin source/tool policy before dispatch, plans actions with LiteLLM when configured, emits Kafka events, tracks run status, and records approvals.
- `apps/authz.py` and `policy/casbin_*`: shared Casbin model and policy used by the gateway and orchestrator.
- `apps/workers`: consumes `tool.requested` events and publishes `tool.completed` events.
- `apps/db_access`: validates read-only SQL, allowlists tables per database source, sets tenant/user session context, and limits returned rows.
- `apps/observability.py`: configures OpenTelemetry traces and metrics for the existing gateway, orchestrator, worker, and DB-access steps.
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
data-source objects, and `apps/db_access` enforces the final guarded SQL
boundary before either database is read.
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
        gateway["Gateway API<br/>JWT validation<br/>Casbin agent invoke check"]
        allowed_permissions["Trusted policy context<br/>x-policy-subjects<br/>x-allowed-permissions"]
    end

    subgraph agents["Orchestrator agents"]
        orchestrator["Orchestrator API<br/>LangGraph workflows<br/>Casbin tool/source checks<br/>LiteLLM planning<br/>Human approval state"]
        world_agent["world-agent<br/>tools: sql, report, approval<br/>requires: world-db"]
        procurement_agent["procurement-agent<br/>tools: sql, approval<br/>requires: procurement-db"]
        denied["Denied run<br/>permission/tool audit event"]
    end

    subgraph tools["Tool execution"]
        kafka["Kafka<br/>agent.requested<br/>tool.requested<br/>tool.completed<br/>audit.events"]
        workers["Workers<br/>SQL worker<br/>Report worker"]
    end

    subgraph db_layer["Database access layer<br/>apps/db_access"]
        db_query["POST /query<br/>body: database + sql<br/>headers: x-tenant-id, x-user-id"]
        sql_guard["SQL guard<br/>parse with sqlglot<br/>one read-only SELECT only"]
        source_router{"Logical database<br/>world or procurement"}
        world_policy["World source policy<br/>allow: city, country,<br/>country_language, country_flag<br/>max rows: 500"]
        procurement_policy["Procurement source policy<br/>allow: suppliers, purchase_orders,<br/>supplier_summary<br/>max rows: 500"]
        blocked_query["Blocked by DB access<br/>write SQL, multi statement,<br/>unknown DB, or disallowed table"]
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
    orchestrator --> world_agent
    orchestrator --> procurement_agent
    world_agent -->|"allowed: world-db"| kafka
    procurement_agent -->|"allowed: procurement-db"| kafka
    world_agent -->|"missing permission"| denied
    procurement_agent -->|"missing permission"| denied
    orchestrator -->|"Tool request events"| kafka
    kafka -->|"Consume"| workers
    workers -->|"SQL tool calls /query"| db_query
    db_query --> sql_guard
    sql_guard -->|"valid SELECT"| source_router
    sql_guard -->|"invalid SQL"| blocked_query
    source_router -->|"database: world"| world_policy
    source_router -->|"database: procurement"| procurement_policy
    source_router -->|"unknown database"| blocked_query
    world_policy -->|"set app.tenant_id/app.user_id<br/>wrapped SELECT limit 500"| worlddb
    procurement_policy -->|"set app.tenant_id/app.user_id<br/>wrapped SELECT limit 500"| procurementdb
    world_policy -->|"table not allowlisted"| blocked_query
    procurement_policy -->|"table not allowlisted"| blocked_query
    workers -->|"Tool result events"| kafka
    kafka -->|"Resume run status"| orchestrator
    frontend -->|"Poll /runs/{run_id}"| gateway
    gateway -->|"Run status"| orchestrator
    gateway -. "traces + metrics" .-> collector
    orchestrator -. "traces + metrics" .-> collector
    orchestrator -. "agent + generation + tool" .-> langfuse
    workers -. "traces + metrics" .-> collector
    db_query -. "traces + metrics" .-> collector
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
| Orchestrator agent | Which workflow and tool path runs | `world-agent` can use SQL, report, or approval; `procurement-agent` can use SQL or approval |
| Source/tool policy check | Whether a tool request is emitted or denied | World data needs `datasource:world-db`; procurement data needs `datasource:procurement-db`; missing access emits an audit event |

## Local Services

| Service | URL / port | Purpose |
| --- | --- | --- |
| Keycloak | `http://localhost:8080` | Local OIDC issuer and seeded users |
| Gateway | `http://localhost:8000` | Public API and test console |
| Test console | `http://localhost:8000/ui` | Browser UI for end-to-end testing |
| Orchestrator | `http://localhost:8001` | Internal workflow API |
| DB access | `http://localhost:8003` | Guarded SQL proxy |
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
those subjects to evaluate source and tool policy before emitting a
`tool.requested` event.

Workflow behavior:

- `world-agent` can route to `sql`, `report`, or `approval`.
- `procurement-agent` can route to `sql` or `approval`.
- LiteLLM planning is used when `LITELLM_MODEL` and `LITELLM_API_KEY` are set.
- Deterministic fallback routing is used when LiteLLM is not configured or the model call fails.

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
sends a logical AI trace containing the `agent-run` root, the
`orchestrator.llm_plan` generation, and the selected tool execution with its
input, result, and final run output. The Langfuse trace carries the same trace
ID as Tempo plus filterable request, tenant, agent, and workflow metadata.
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
`gateway`, `orchestrator`, `sql-worker`, `report-worker`, and `db-access`.
Tempo is an API-backed trace store in this stack, so the browser UI for Tempo
traces is Grafana Explore, not `http://localhost:3200/`.

For a Tempo panel or Explore query, select the `Tempo` datasource (`uid: tempo`).
For metrics, use the `Prometheus` datasource (`uid: prometheus`). For logs, use
the `Loki` datasource (`uid: loki`) and filter by labels such as
`{service="gateway"}` or `{service="orchestrator"}`.

Useful spans include:

- `gateway.agent_call`: user request, agent authorization, orchestrator response.
- `orchestrator.agent_run`: trusted request handling and LangGraph invocation.
- `orchestrator.choose_plan_action`: LiteLLM or fallback routing decision.
- `kafka.publish`: `agent.requested`, `tool.requested`, `tool.completed`, or `audit.events` emission.
- `worker.sql_tool` and `worker.report_tool`: existing tool execution steps.
- `db_access.validate_sql` and `db_access.execute_sql`: SQL guard and database read.
- `gateway.run_status_response`: the user-facing poll that returns the final run result.

Trace context is carried across HTTP automatically and across Kafka in the event
payload's `trace_context` field. The initial agent-call trace continues through
the async worker completion path. Status polling and approvals are separate HTTP
calls, and they include the same `run_id` / `request_id` attributes for search
and correlation in Grafana and Tempo. Container logs are shipped by Promtail to
Loki with Compose labels, including the `service` label.

## Database Access Layer

`apps/db_access` is a small FastAPI service that sits behind the SQL worker. In
the full agent flow, the gateway first checks `agent:{agent_id}` `invoke`
through Casbin, the orchestrator checks the run's data-source and tool policy,
and only then does the SQL worker call `POST /query` on `db-access`.

The access layer's job is the database boundary:

1. Pick the logical database from the request body.
2. Parse SQL with `sqlglot`.
3. Allow exactly one read-only `SELECT` statement.
4. Reject table names outside that source's allowlist.
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

Compose points the two logical sources at different databases:

```bash
DATABASE_URL=postgresql://world:world123@postgres:5432/world-db
WORLD_DATABASE_URL=postgresql://world:world123@postgres:5432/world-db
PROCUREMENT_DATABASE_URL=postgresql://world:world123@postgres:5432/procurement_db
```

| Logical database | Source permission | Env var | Allowed tables | Max rows |
| --- | --- | --- | --- | --- |
| `world` | `world-db` | `WORLD_DATABASE_URL` or `DATABASE_URL` | `city`, `country`, `country_language`, `country_flag` | 500 |
| `procurement` | `procurement-db` | `PROCUREMENT_DATABASE_URL` | `suppliers`, `purchase_orders`, `supplier_summary` | 500 |

Unknown logical database names return `404`. Missing configured database URLs
return `503`. Invalid SQL returns `400`, and disallowed tables return `403`.

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

The orchestrator calls an OpenAI-compatible LiteLLM chat completions endpoint for
request planning when these variables are configured:

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
uvicorn apps.db_access.main:app --host 0.0.0.0 --port 8003
python -m apps.workers.sql_worker
python -m apps.workers.report_worker
```

Kafka, Keycloak, and Postgres are still required for the full flow.

## Common Operations

Rebuild app containers after code changes:

```bash
docker compose up --build -d gateway orchestrator db-access sql-worker report-worker
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
docker compose logs -f gateway orchestrator sql-worker report-worker db-access langfuse-web langfuse-worker postgres procurement-db-init
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

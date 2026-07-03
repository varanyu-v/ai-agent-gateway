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

- `apps/gateway`: validates Keycloak JWTs, checks agent roles, maps source permissions, and forwards trusted context headers.
- `apps/orchestrator`: runs LangGraph workflows, plans actions with LiteLLM when configured, emits Kafka events, tracks run status, and records approvals.
- `apps/workers`: consumes `tool.requested` events and publishes `tool.completed` events.
- `apps/db_access`: validates read-only SQL, allowlists tables per database source, sets tenant/user session context, and limits returned rows.
- `apps/frontend/index.html`: local browser test console with login, role visibility, example runs, SQL response rendering, agent input/output, and human approval.
- `docker/keycloak/ptvn-realm.json`: local realm, roles, client, and seeded demo users.
- `docker/postgres/init/02-create-procurement-database.sql`: idempotent procurement database and seed data initializer.
- `sql/rls_example.sql`: illustrative RLS policies for production hardening ideas.

## Architecture

```mermaid
flowchart TD
    frontend["Browser Test Console"]
    keycloak["Keycloak OIDC"]
    gateway["Gateway API<br/>JWT validation<br/>Agent access<br/>Source permission mapping"]
    orchestrator["Orchestrator API<br/>LangGraph workflows<br/>LiteLLM planning<br/>Human approval state"]
    kafka["Kafka<br/>agent.requested<br/>tool.requested<br/>tool.completed<br/>audit.events"]
    workers["Workers<br/>SQL worker<br/>Report worker"]
    db_access["DB Access API<br/>SQL validation<br/>Per-source allowlist<br/>Tenant context"]
    worlddb["Postgres World DB<br/>city<br/>country<br/>country_language<br/>country_flag"]
    procurementdb["Procurement DB<br/>suppliers<br/>purchase_orders<br/>supplier_summary"]

    frontend -->|"Login"| keycloak
    keycloak -->|"Access token"| frontend
    frontend -->|"Bearer token"| gateway
    gateway -->|"Trusted headers"| orchestrator
    orchestrator -->|"Tool request events"| kafka
    kafka -->|"Consume"| workers
    workers -->|"Authorized query"| db_access
    db_access -->|"World read-only SQL"| worlddb
    db_access -->|"Procurement read-only SQL"| procurementdb
    workers -->|"Tool result events"| kafka
    kafka -->|"Resume run status"| orchestrator
    frontend -->|"Poll /runs/{run_id}"| gateway
    gateway -->|"Run status"| orchestrator
```

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

The Postgres service uses a PG18-safe volume mount:

```text
postgres-world-pg18-data:/var/lib/postgresql
```

The `procurement-db-init` service runs on startup and creates or refreshes the
procurement schema without requiring the World DB volume to be deleted.

## Demo Users

| Username | Password | Good first test | Roles |
| --- | --- | --- | --- |
| `world-analyst` | `world-password` | World DB SQL and report | `agent:world-agent:invoke`, `permission:world-db:read` |
| `procurement-analyst` | `procurement-password` | Procurement DB SQL | `agent:procurement-agent:invoke`, `permission:procurement-db:read` |
| `source-auditor` | `auditor-password` | Permission denial | `agent:world-agent:invoke`, `agent:procurement-agent:invoke`, `permission:world-db:read` |
| `data-admin` | `data-admin-password` | Human approval | `agent:world-agent:invoke`, `agent:procurement-agent:invoke`, `permission:world-db:read`, `permission:procurement-db:read` |

All seeded users have `tenant_id=demo-tenant`. The `agent-frontend` client adds
the `agent-gateway` audience expected by the gateway.

## Run Agent Examples

The Run Agent panel provides these examples:

| Example | User | Agent | Message | Expected result |
| --- | --- | --- | --- | --- |
| World Market Hotspots | `world-analyst` | `world-agent` | `show the largest cities by population with country context` | SQL rows from World DB |
| Market Entry Report | `world-analyst` | `world-agent` | `generate a world market entry report` | Report tool completes with a sample download URL |
| Procurement Spend Radar | `procurement-analyst` | `procurement-agent` | `rank suppliers by total purchase spend and risk` | SQL rows from `procurement_db` |
| Source Permission Denial | `source-auditor` | `procurement-agent` | `rank suppliers by total purchase spend and risk` | `denied` because `permission:procurement-db:read` is missing |
| Human Approval Gate | `data-admin` | `procurement-agent` | `remove blocked supplier records from the procurement source` | `requires_approval` and an approval button |

Approval currently records the approval and emits audit state. This sample does
not execute destructive follow-up actions after approval.

## Workflows And Permissions

The gateway and orchestrator enforce two separate checks:

- Agent access: `agent:world-agent:invoke` or `agent:procurement-agent:invoke`.
- Source permission access: `permission:world-db:read` or `permission:procurement-db:read`.

The seeded realm contains exactly two agent roles and two permission roles.

The gateway converts Keycloak realm roles into `x-allowed-permissions`. The
orchestrator checks that list before emitting a `tool.requested` event.

Workflow behavior:

- `world-agent` can route to `sql`, `report`, or `approval`.
- `procurement-agent` can route to `sql` or `approval`.
- LiteLLM planning is used when `LITELLM_MODEL` and `LITELLM_API_KEY` are set.
- Deterministic fallback routing is used when LiteLLM is not configured or the model call fails.

## Database

Compose points the two logical sources at different databases:

```bash
DATABASE_URL=postgresql://world:world123@localhost:5432/world-db
WORLD_DATABASE_URL=postgresql://world:world123@localhost:5432/world-db
PROCUREMENT_DATABASE_URL=postgresql://world:world123@localhost:5432/procurement_db
```

World DB allowlist:

- `city`
- `country`
- `country_language`
- `country_flag`

Procurement DB allowlist:

- `suppliers`
- `purchase_orders`
- `supplier_summary`

The DB access layer rejects write statements, multiple statements, unknown
logical database names, and tables outside each source allowlist. Every query is
wrapped with a max row limit.

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
docker compose logs -f gateway orchestrator sql-worker report-worker db-access postgres procurement-db-init
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

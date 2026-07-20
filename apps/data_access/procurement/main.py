"""Procurement agent's database access layer.

Holds credentials for the procurement database only. Runs as its own service
(`procurement-db-access`) so a fault or compromise here cannot reach world
data.
"""

from apps.data_access.runtime import AgentDataPlane, create_data_access_app


PROCUREMENT_DB_TABLES = frozenset({"suppliers", "purchase_orders", "supplier_summary"})

DEFINITION = AgentDataPlane(
    database="procurement",
    service_name="procurement-db-access",
    allowed_tables=PROCUREMENT_DB_TABLES,
    url_env=("PROCUREMENT_DATABASE_URL",),
    title="Procurement Database Access Layer",
)

app = create_data_access_app(DEFINITION)

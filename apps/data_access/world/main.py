"""World agent's database access layer.

Holds credentials for the world database only. Runs as its own service
(`world-db-access`) so a fault or compromise here cannot reach procurement
data.
"""

from apps.data_access.runtime import AgentDataPlane, create_data_access_app


WORLD_DB_TABLES = frozenset({"city", "country", "country_language", "country_flag"})

DEFINITION = AgentDataPlane(
    database="world",
    service_name="world-db-access",
    allowed_tables=WORLD_DB_TABLES,
    url_env=("WORLD_DATABASE_URL", "DATABASE_URL"),
    title="World Database Access Layer",
)

app = create_data_access_app(DEFINITION)

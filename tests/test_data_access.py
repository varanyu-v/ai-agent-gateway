import unittest

import httpx

from apps.data_access.runtime import DATA_PLANE_PROTOCOL, parse_data_planes
from apps.data_access.procurement.main import app as procurement_app
from apps.data_access.world.main import app as world_app


# ASGITransport does not run FastAPI's lifespan, so no connection pool is
# created. Guard checks (foreign database, SQL validation) run before the pool
# is touched; a *valid* query therefore reaches the pool guard and returns 503,
# which is exactly how these tests prove validation passed without needing a
# live database.
HEADERS = {"x-tenant-id": "demo-tenant", "x-user-id": "demo-user"}


def data_plane_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://data-plane-under-test",
    )


class DataPlaneRegistryTests(unittest.TestCase):
    def test_parse_data_planes(self) -> None:
        planes = parse_data_planes(
            "world=http://world-db-access:8006/, procurement=http://procurement-db-access:8007",
        )
        self.assertEqual(
            planes,
            {
                "world": "http://world-db-access:8006",
                "procurement": "http://procurement-db-access:8007",
            },
        )

    def test_parse_data_planes_skips_malformed_entries(self) -> None:
        self.assertEqual(parse_data_planes("bad-entry,,=http://x,world="), {})


class WorldDataPlaneTests(unittest.IsolatedAsyncioTestCase):
    async def query(self, sql: str, database: str = "world") -> httpx.Response:
        async with data_plane_client(world_app) as client:
            return await client.post(
                "/query",
                json={"database": database, "sql": sql},
                headers=HEADERS,
            )

    async def test_health_reports_service_and_database(self) -> None:
        async with data_plane_client(world_app) as client:
            response = await client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["protocol"], DATA_PLANE_PROTOCOL)
        self.assertEqual(payload["service"], "world-db-access")
        self.assertEqual(payload["database"], "world")

    async def test_refuses_foreign_database(self) -> None:
        response = await self.query("select 1 from suppliers", database="procurement")
        self.assertEqual(response.status_code, 404)

    async def test_rejects_non_select(self) -> None:
        response = await self.query("delete from city")
        self.assertEqual(response.status_code, 400)

    async def test_rejects_multiple_statements(self) -> None:
        response = await self.query("select * from city; select * from country")
        self.assertEqual(response.status_code, 400)

    async def test_rejects_disallowed_table(self) -> None:
        # `suppliers` belongs to procurement; the world plane must not read it.
        response = await self.query("select * from suppliers")
        self.assertEqual(response.status_code, 403)

    async def test_valid_query_passes_validation_and_reaches_pool_guard(self) -> None:
        response = await self.query("select name from city")
        self.assertEqual(response.status_code, 503)


class ProcurementDataPlaneTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_reports_procurement_identity(self) -> None:
        async with data_plane_client(procurement_app) as client:
            response = await client.get("/health")

        self.assertEqual(response.json()["service"], "procurement-db-access")
        self.assertEqual(response.json()["database"], "procurement")

    async def test_refuses_world_database(self) -> None:
        async with data_plane_client(procurement_app) as client:
            response = await client.post(
                "/query",
                json={"database": "world", "sql": "select * from city"},
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 404)

    async def test_rejects_disallowed_table(self) -> None:
        async with data_plane_client(procurement_app) as client:
            response = await client.post(
                "/query",
                json={"database": "procurement", "sql": "select * from city"},
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()

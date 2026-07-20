import unittest

from apps import authz


def user(user_id: str, roles: list[str]) -> dict[str, object]:
    return {
        "user_id": user_id,
        "tenant_id": "demo-tenant",
        "roles": roles,
    }


class AuthzPolicyTests(unittest.TestCase):
    def test_world_analyst_can_invoke_world_and_use_world_source(self) -> None:
        current_user = user(
            "world-analyst",
            ["role:world-analyst"],
        )

        self.assertTrue(authz.can_invoke_agent(current_user, "world-agent"))
        self.assertFalse(authz.can_invoke_agent(current_user, "procurement-agent"))
        self.assertEqual(authz.allowed_data_sources(current_user), ["world-db"])

    def test_legacy_world_roles_map_to_world_analyst(self) -> None:
        current_user = user(
            "world-analyst",
            ["agent:world-agent:invoke", "permission:world-db:read"],
        )

        self.assertIn("role:world-analyst", authz.policy_subjects(current_user))
        self.assertTrue(authz.can_invoke_agent(current_user, "world-agent"))
        self.assertFalse(authz.can_invoke_agent(current_user, "procurement-agent"))
        self.assertEqual(authz.allowed_data_sources(current_user), ["world-db"])

    def test_procurement_analyst_can_execute_procurement_mcp(self) -> None:
        current_user = user(
            "procurement-analyst",
            ["role:procurement-analyst"],
        )
        subjects = authz.policy_subjects(current_user)

        self.assertTrue(authz.can_invoke_agent(current_user, "procurement-agent"))
        self.assertTrue(
            authz.can_read_data_source_subjects(
                subjects,
                "demo-tenant",
                "procurement-db",
            ),
        )
        self.assertTrue(
            authz.can_execute_mcp_server(subjects, "demo-tenant", "procurement-mcp"),
        )
        self.assertFalse(
            authz.can_execute_mcp_server(subjects, "demo-tenant", "report-mcp"),
        )

    def test_legacy_procurement_roles_map_to_procurement_analyst(self) -> None:
        current_user = user(
            "procurement-analyst",
            ["agent:procurement-agent:invoke", "permission:procurement-db:read"],
        )
        subjects = authz.policy_subjects(current_user)

        self.assertIn("role:procurement-analyst", subjects)
        self.assertTrue(authz.can_invoke_agent(current_user, "procurement-agent"))
        self.assertTrue(
            authz.can_read_data_source_subjects(
                subjects,
                "demo-tenant",
                "procurement-db",
            ),
        )
        self.assertTrue(
            authz.can_execute_mcp_server(subjects, "demo-tenant", "procurement-mcp"),
        )
        self.assertFalse(
            authz.can_execute_mcp_server(subjects, "demo-tenant", "report-mcp"),
        )

    def test_source_auditor_lacks_procurement_source(self) -> None:
        current_user = user(
            "source-auditor",
            ["role:source-auditor"],
        )
        subjects = authz.policy_subjects(current_user)

        self.assertTrue(authz.can_invoke_agent(current_user, "procurement-agent"))
        self.assertFalse(
            authz.can_read_data_source_subjects(
                subjects,
                "demo-tenant",
                "procurement-db",
            ),
        )
        self.assertEqual(authz.allowed_data_sources(current_user), ["world-db"])

    def test_legacy_source_auditor_roles_map_to_source_auditor(self) -> None:
        current_user = user(
            "source-auditor",
            [
                "agent:world-agent:invoke",
                "agent:procurement-agent:invoke",
                "permission:world-db:read",
            ],
        )
        subjects = authz.policy_subjects(current_user)

        self.assertIn("role:source-auditor", subjects)
        self.assertTrue(authz.can_invoke_agent(current_user, "world-agent"))
        self.assertTrue(authz.can_invoke_agent(current_user, "procurement-agent"))
        self.assertFalse(
            authz.can_read_data_source_subjects(
                subjects,
                "demo-tenant",
                "procurement-db",
            ),
        )
        self.assertEqual(authz.allowed_data_sources(current_user), ["world-db"])

    def test_allowed_resources_use_resolved_policy_subjects(self) -> None:
        current_user = user(
            "source-auditor",
            [
                "agent:world-agent:invoke",
                "agent:procurement-agent:invoke",
                "permission:world-db:read",
            ],
        )

        self.assertEqual(
            authz.allowed_agents(current_user),
            ["assistant", "procurement-agent", "world-agent"],
        )
        self.assertEqual(authz.allowed_data_sources(current_user), ["world-db"])
        self.assertEqual(
            authz.allowed_mcp_servers(current_user),
            ["report-mcp", "world-mcp"],
        )

    def test_legacy_data_admin_roles_map_to_data_admin(self) -> None:
        current_user = user(
            "data-admin",
            [
                "agent:world-agent:invoke",
                "agent:procurement-agent:invoke",
                "permission:world-db:read",
                "permission:procurement-db:read",
            ],
        )
        subjects = authz.policy_subjects(current_user)

        self.assertIn("role:data-admin", subjects)
        self.assertTrue(authz.can_invoke_agent(current_user, "world-agent"))
        self.assertTrue(authz.can_invoke_agent(current_user, "procurement-agent"))
        self.assertEqual(
            authz.allowed_data_sources(current_user),
            ["procurement-db", "world-db"],
        )
        self.assertTrue(
            authz.can_execute_mcp_server(subjects, "demo-tenant", "report-mcp"),
        )

    def test_policy_subjects_are_forwarded_without_recreating_permissions(self) -> None:
        self.assertEqual(
            authz.parse_policy_subjects(""),
            [],
        )
        self.assertEqual(
            authz.parse_policy_subjects("user:alice, role:world-analyst"),
            ["user:alice", "role:world-analyst"],
        )

    def test_access_catalog_lists_denied_objects_without_their_roles(self) -> None:
        current_user = user("world-analyst", ["role:world-analyst"])

        catalog = authz.access_catalog(authz.AGENT_PREFIX, authz.INVOKE, current_user)
        entries = {entry["id"]: entry for entry in catalog}

        self.assertEqual(
            sorted(entries),
            ["assistant", "procurement-agent", "world-agent"],
        )
        self.assertTrue(entries["world-agent"]["allowed"])
        self.assertFalse(entries["procurement-agent"]["allowed"])
        self.assertEqual(entries["world-agent"]["policyObject"], "agent:world-agent")
        for entry in catalog:
            self.assertNotIn("roles", entry)

    def test_access_catalog_includes_roles_for_admins_only(self) -> None:
        admin = user("data-admin", ["role:data-admin"])
        analyst = user("world-analyst", ["role:world-analyst"])

        self.assertTrue(authz.is_policy_admin(admin))
        self.assertFalse(authz.is_policy_admin(analyst))

        catalog = authz.access_catalog(
            authz.AGENT_PREFIX,
            authz.INVOKE,
            admin,
            include_roles=True,
        )
        entries = {entry["id"]: entry for entry in catalog}
        self.assertEqual(
            entries["world-agent"]["roles"],
            ["role:data-admin", "role:source-auditor", "role:world-analyst"],
        )

    def test_access_catalog_surfaces_ungoverned_registry_entries(self) -> None:
        current_user = user("data-admin", ["role:data-admin"])

        catalog = authz.access_catalog(
            authz.AGENT_PREFIX,
            authz.INVOKE,
            current_user,
            known_ids=["shadow-agent"],
        )
        entries = {entry["id"]: entry for entry in catalog}

        self.assertIn("shadow-agent", entries)
        self.assertFalse(entries["shadow-agent"]["governed"])
        # Deny-by-default: even an admin has no grant for an unlisted object.
        self.assertFalse(entries["shadow-agent"]["allowed"])
        self.assertTrue(entries["world-agent"]["governed"])

    def test_admin_policy_metadata_comes_from_casbin_policy(self) -> None:
        admin = user("data-admin", ["role:data-admin"])

        agents = {
            entry["id"]: entry
            for entry in authz.access_catalog(
                authz.AGENT_PREFIX,
                authz.INVOKE,
                admin,
                include_roles=True,
            )
        }
        self.assertEqual(
            {agent_id: agents[agent_id]["roles"] for agent_id in agents},
            {
                "assistant": [
                    "role:data-admin",
                    "role:procurement-analyst",
                    "role:source-auditor",
                    "role:world-analyst",
                ],
                "procurement-agent": [
                    "role:data-admin",
                    "role:procurement-analyst",
                    "role:source-auditor",
                ],
                "world-agent": [
                    "role:data-admin",
                    "role:source-auditor",
                    "role:world-analyst",
                ],
            },
        )

        servers = {
            entry["id"]: entry
            for entry in authz.access_catalog(
                authz.MCP_SERVER_PREFIX,
                authz.EXECUTE,
                admin,
                include_roles=True,
            )
        }
        self.assertEqual(
            {server_id: servers[server_id]["roles"] for server_id in servers},
            {
                "procurement-mcp": [
                    "role:data-admin",
                    "role:procurement-analyst",
                ],
                "report-mcp": [
                    "role:data-admin",
                    "role:source-auditor",
                    "role:world-analyst",
                ],
                "world-mcp": [
                    "role:data-admin",
                    "role:source-auditor",
                    "role:world-analyst",
                ],
            },
        )
        self.assertEqual(
            servers["world-mcp"]["policyObject"],
            "mcp:world-mcp",
        )


if __name__ == "__main__":
    unittest.main()

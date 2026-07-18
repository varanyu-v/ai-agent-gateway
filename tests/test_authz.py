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

    def test_procurement_analyst_can_execute_sql_for_procurement_source(self) -> None:
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
        self.assertTrue(authz.can_execute_tool(subjects, "demo-tenant", "sql"))
        self.assertFalse(authz.can_execute_tool(subjects, "demo-tenant", "report"))

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
        self.assertTrue(authz.can_execute_tool(subjects, "demo-tenant", "sql"))
        self.assertFalse(authz.can_execute_tool(subjects, "demo-tenant", "report"))

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
        self.assertEqual(authz.allowed_tools(current_user), ["mcp", "report", "sql"])

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
        self.assertTrue(authz.can_execute_tool(subjects, "demo-tenant", "report"))

    def test_policy_subjects_are_forwarded_without_recreating_permissions(self) -> None:
        self.assertEqual(
            authz.parse_policy_subjects(""),
            [],
        )
        self.assertEqual(
            authz.parse_policy_subjects("user:alice, role:world-analyst"),
            ["user:alice", "role:world-analyst"],
        )

    def test_ui_policy_metadata_comes_from_casbin_policy(self) -> None:
        self.assertEqual(
            authz.agent_access_rules(),
            [
                {
                    "id": "assistant",
                    "role": "role:data-admin",
                    "roles": [
                        "role:data-admin",
                        "role:procurement-analyst",
                        "role:source-auditor",
                        "role:world-analyst",
                    ],
                    "policyObject": "agent:assistant",
                    "policyAction": "invoke",
                },
                {
                    "id": "procurement-agent",
                    "role": "role:data-admin",
                    "roles": [
                        "role:data-admin",
                        "role:procurement-analyst",
                        "role:source-auditor",
                    ],
                    "policyObject": "agent:procurement-agent",
                    "policyAction": "invoke",
                },
                {
                    "id": "world-agent",
                    "role": "role:data-admin",
                    "roles": [
                        "role:data-admin",
                        "role:source-auditor",
                        "role:world-analyst",
                    ],
                    "policyObject": "agent:world-agent",
                    "policyAction": "invoke",
                },
            ],
        )
        self.assertEqual(
            authz.tool_access_rules(),
            [
                {
                    "id": "mcp",
                    "role": "role:data-admin",
                    "roles": [
                        "role:data-admin",
                        "role:procurement-analyst",
                        "role:source-auditor",
                        "role:world-analyst",
                    ],
                    "policyObject": "tool:mcp",
                    "policyAction": "execute",
                },
                {
                    "id": "report",
                    "role": "role:data-admin",
                    "roles": [
                        "role:data-admin",
                        "role:source-auditor",
                        "role:world-analyst",
                    ],
                    "policyObject": "tool:report",
                    "policyAction": "execute",
                },
                {
                    "id": "sql",
                    "role": "role:data-admin",
                    "roles": [
                        "role:data-admin",
                        "role:procurement-analyst",
                        "role:source-auditor",
                        "role:world-analyst",
                    ],
                    "policyObject": "tool:sql",
                    "policyAction": "execute",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()

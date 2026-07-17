import csv
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import casbin


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL_PATH = REPO_ROOT / "policy" / "casbin_model.conf"
DEFAULT_POLICY_PATH = REPO_ROOT / "policy" / "casbin_policy.csv"

AGENT_PREFIX = "agent:"
DATA_SOURCE_PREFIX = "datasource:"
TOOL_PREFIX = "tool:"
INVOKE = "invoke"
READ = "read"
EXECUTE = "execute"
LEGACY_WORLD_AGENT_ROLE = "agent:world-agent:invoke"
LEGACY_PROCUREMENT_AGENT_ROLE = "agent:procurement-agent:invoke"
LEGACY_WORLD_DB_ROLE = "permission:world-db:read"
LEGACY_PROCUREMENT_DB_ROLE = "permission:procurement-db:read"


@dataclass(frozen=True)
class PolicyRule:
    subject: str
    domain: str
    object: str
    action: str


def model_path() -> Path:
    return Path(os.getenv("CASBIN_MODEL_PATH", DEFAULT_MODEL_PATH))


def policy_path() -> Path:
    return Path(os.getenv("CASBIN_POLICY_PATH", DEFAULT_POLICY_PATH))


@lru_cache(maxsize=1)
def enforcer() -> casbin.Enforcer:
    return casbin.Enforcer(str(model_path()), str(policy_path()))


@lru_cache(maxsize=1)
def policy_rules() -> tuple[PolicyRule, ...]:
    rules: list[PolicyRule] = []
    with policy_path().open(newline="") as policy_file:
        for row in csv.reader(policy_file):
            if not row or row[0].strip().startswith("#"):
                continue
            values = [value.strip() for value in row]
            if len(values) != 5 or values[0] != "p":
                continue
            rules.append(
                PolicyRule(
                    subject=values[1],
                    domain=values[2],
                    object=values[3],
                    action=values[4],
                ),
            )
    return tuple(rules)


def policy_subjects(user: dict[str, Any]) -> list[str]:
    roles = [str(role) for role in user.get("roles", [])]
    subjects = [f"user:{user['user_id']}"]
    subjects.extend(roles)
    subjects.extend(legacy_persona_subjects(roles))
    return list(dict.fromkeys(subjects))


def legacy_persona_subjects(roles: Iterable[str]) -> list[str]:
    role_set = set(roles)
    has_world_agent = LEGACY_WORLD_AGENT_ROLE in role_set
    has_procurement_agent = LEGACY_PROCUREMENT_AGENT_ROLE in role_set
    has_world_source = LEGACY_WORLD_DB_ROLE in role_set
    has_procurement_source = LEGACY_PROCUREMENT_DB_ROLE in role_set

    subjects: list[str] = []
    if has_world_agent and has_procurement_agent and has_world_source and has_procurement_source:
        subjects.append("role:data-admin")
    elif has_world_agent and has_procurement_agent and has_world_source:
        subjects.append("role:source-auditor")
    elif has_world_agent and has_world_source:
        subjects.append("role:world-analyst")
    elif has_procurement_agent and has_procurement_source:
        subjects.append("role:procurement-analyst")
    return subjects


def parse_policy_subjects(header_value: str) -> list[str]:
    subjects = [subject.strip() for subject in header_value.split(",") if subject.strip()]
    return list(dict.fromkeys(subjects))


def is_allowed_subjects(
    subjects: Iterable[str],
    tenant_id: str,
    object_id: str,
    action: str,
) -> bool:
    policy_enforcer = enforcer()
    return any(
        policy_enforcer.enforce(subject, tenant_id, object_id, action)
        for subject in subjects
    )


def is_allowed_user(user: dict[str, Any], object_id: str, action: str) -> bool:
    return is_allowed_subjects(
        policy_subjects(user),
        user["tenant_id"],
        object_id,
        action,
    )


def can_invoke_agent(user: dict[str, Any], agent_id: str) -> bool:
    return is_allowed_user(user, f"{AGENT_PREFIX}{agent_id}", INVOKE)


def can_read_data_source(user: dict[str, Any], source_id: str) -> bool:
    return is_allowed_user(user, f"{DATA_SOURCE_PREFIX}{source_id}", READ)


def can_execute_tool(subjects: Iterable[str], tenant_id: str, tool_id: str) -> bool:
    return is_allowed_subjects(subjects, tenant_id, f"{TOOL_PREFIX}{tool_id}", EXECUTE)


def can_read_data_source_subjects(
    subjects: Iterable[str],
    tenant_id: str,
    source_id: str,
) -> bool:
    return is_allowed_subjects(subjects, tenant_id, f"{DATA_SOURCE_PREFIX}{source_id}", READ)


def can_invoke_agent_subjects(
    subjects: Iterable[str],
    tenant_id: str,
    agent_id: str,
) -> bool:
    return is_allowed_subjects(subjects, tenant_id, f"{AGENT_PREFIX}{agent_id}", INVOKE)


def allowed_agents(user: dict[str, Any]) -> list[str]:
    return sorted(
        agent_id
        for agent_id in configured_agents()
        if can_invoke_agent(user, agent_id)
    )


def allowed_data_sources(user: dict[str, Any]) -> list[str]:
    return sorted(
        source_id
        for source_id in configured_data_sources()
        if can_read_data_source(user, source_id)
    )


def allowed_tools(user: dict[str, Any]) -> list[str]:
    subjects = policy_subjects(user)
    return sorted(
        tool_id
        for tool_id in configured_tools()
        if can_execute_tool(subjects, user["tenant_id"], tool_id)
    )


def configured_agents() -> tuple[str, ...]:
    return _configured_ids(AGENT_PREFIX, INVOKE)


def configured_data_sources() -> tuple[str, ...]:
    return _configured_ids(DATA_SOURCE_PREFIX, READ)


def configured_tools() -> tuple[str, ...]:
    return _configured_ids(TOOL_PREFIX, EXECUTE)


def _configured_ids(prefix: str, action: str) -> tuple[str, ...]:
    ids = {
        rule.object.removeprefix(prefix)
        for rule in policy_rules()
        if rule.object.startswith(prefix) and rule.action == action
    }
    return tuple(sorted(ids))


def agent_access_rules() -> list[dict[str, Any]]:
    return _access_rules(AGENT_PREFIX, INVOKE)


def data_source_access_rules() -> list[dict[str, Any]]:
    return _access_rules(DATA_SOURCE_PREFIX, READ)


def tool_access_rules() -> list[dict[str, Any]]:
    return _access_rules(TOOL_PREFIX, EXECUTE)


def _access_rules(prefix: str, action: str) -> list[dict[str, Any]]:
    grouped: dict[str, set[str]] = {}
    for rule in policy_rules():
        if rule.object.startswith(prefix) and rule.action == action:
            object_id = rule.object.removeprefix(prefix)
            grouped.setdefault(object_id, set()).add(rule.subject)

    access_rules = []
    for object_id, subjects in sorted(grouped.items()):
        roles = sorted(subjects)
        access_rules.append(
            {
                "id": object_id,
                "role": roles[0],
                "roles": roles,
                "policyObject": f"{prefix}{object_id}",
                "policyAction": action,
            },
        )
    return access_rules

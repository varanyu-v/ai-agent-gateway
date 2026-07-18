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
MCP_SERVER_PREFIX = "mcp:"
INVOKE = "invoke"
READ = "read"
EXECUTE = "execute"
DEFAULT_ADMIN_SUBJECTS = "role:data-admin"
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


def can_execute_mcp_server(
    subjects: Iterable[str],
    tenant_id: str,
    server_id: str,
) -> bool:
    return is_allowed_subjects(
        subjects,
        tenant_id,
        f"{MCP_SERVER_PREFIX}{server_id}",
        EXECUTE,
    )


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


def allowed_mcp_servers(user: dict[str, Any]) -> list[str]:
    subjects = policy_subjects(user)
    return sorted(
        server_id
        for server_id in configured_mcp_servers()
        if can_execute_mcp_server(subjects, user["tenant_id"], server_id)
    )


def configured_agents() -> tuple[str, ...]:
    return _configured_ids(AGENT_PREFIX, INVOKE)


def configured_data_sources() -> tuple[str, ...]:
    return _configured_ids(DATA_SOURCE_PREFIX, READ)


def configured_mcp_servers() -> tuple[str, ...]:
    return _configured_ids(MCP_SERVER_PREFIX, EXECUTE)


def _configured_ids(prefix: str, action: str) -> tuple[str, ...]:
    return tuple(sorted(_subjects_by_object(prefix, action)))


def _subjects_by_object(prefix: str, action: str) -> dict[str, set[str]]:
    """Map each governed object id to the policy subjects granted `action` on it."""
    grouped: dict[str, set[str]] = {}
    for rule in policy_rules():
        if rule.object.startswith(prefix) and rule.action == action:
            object_id = rule.object.removeprefix(prefix)
            grouped.setdefault(object_id, set()).add(rule.subject)
    return grouped


def admin_subjects() -> tuple[str, ...]:
    raw = os.getenv("POLICY_ADMIN_SUBJECTS", DEFAULT_ADMIN_SUBJECTS)
    return tuple(subject.strip() for subject in raw.split(",") if subject.strip())


def is_policy_admin(user: dict[str, Any]) -> bool:
    admins = set(admin_subjects())
    return any(subject in admins for subject in policy_subjects(user))


def access_catalog(
    prefix: str,
    action: str,
    user: dict[str, Any],
    known_ids: Iterable[str] = (),
    include_roles: bool = False,
) -> list[dict[str, Any]]:
    """List every object of this kind with whether `user` may act on it.

    Objects come from two independent sources that can disagree: the policy
    file (what is governed) and the caller-supplied registry ids (what is
    actually running). Listing the union surfaces that drift instead of
    hiding it — a running service with no policy row is reported as
    ungoverned and, since casbin is deny-by-default, not allowed.

    `roles` names the subjects that would grant access, so it is only
    included for policy admins.
    """
    grouped = _subjects_by_object(prefix, action)
    subjects = policy_subjects(user)
    tenant_id = user["tenant_id"]

    entries: list[dict[str, Any]] = []
    for object_id in sorted(set(grouped) | set(known_ids)):
        entry: dict[str, Any] = {
            "id": object_id,
            "allowed": is_allowed_subjects(
                subjects,
                tenant_id,
                f"{prefix}{object_id}",
                action,
            ),
            "governed": object_id in grouped,
            "policyObject": f"{prefix}{object_id}",
            "policyAction": action,
        }
        if include_roles:
            entry["roles"] = sorted(grouped.get(object_id, set()))
        entries.append(entry)
    return entries

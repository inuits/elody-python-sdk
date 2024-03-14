import re as regex

from elody.policies.permission_handler import (
    get_permissions,
    get_mask_protected_content_post_request_hook,
)
from elody.util import get_item_metadata_value
from flask import Request  # pyright: ignore
from inuits_policy_based_auth import BaseAuthorizationPolicy  # pyright: ignore
from inuits_policy_based_auth.contexts.policy_context import (  # pyright: ignore
    PolicyContext,
)
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)


class GenericObjectRequestPolicy(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not user_context.auth_objects.get("token") or not regex.match(
            "^/[^/]+$|^/ngsi-ld/v1/entities$", request.path
        ):
            return policy_context

        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [PostRequestRules, GetRequestRules]
            access_verdict = None
            for rule in rules:
                access_verdict = rule().apply(user_context, request, permissions)
                if access_verdict != None:
                    policy_context.access_verdict = access_verdict
                    if not policy_context.access_verdict:
                        return policy_context

            if policy_context.access_verdict:
                return policy_context

        return policy_context


class PostRequestRules:
    def apply(self, _, request: Request, permissions) -> bool | None:
        if request.method != "POST":
            return None

        item = request.json or {}
        if item["type"] in permissions["create"].keys():
            restrictions = permissions["create"][item["type"]].get("restrictions", {})
            for metadata in restrictions.get("metadata", []):
                value = get_item_metadata_value(item, metadata["key"])
                if isinstance(value, str):
                    if value not in metadata["value"]:
                        return None
                elif isinstance(value, list):
                    for expected_value in metadata["value"]:
                        if expected_value not in value:
                            return None
            return True

        return None


class GetRequestRules:
    def apply(
        self, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "GET":
            return None

        type_query_parameter = request.args.get("type")
        allowed_item_types = list(permissions["read"].keys())
        filters = []

        if type_query_parameter:
            if type_query_parameter in allowed_item_types:
                restrictions = permissions["read"][type_query_parameter].get(
                    "restrictions", {}
                )
                for parent_key in restrictions.keys():
                    all_matches = []
                    if parent_key == "metadata":
                        for metadata in restrictions[parent_key]:
                            all_matches.append(
                                {
                                    "$elemMatch": {
                                        "key": metadata["key"],
                                        "value": {"$in": metadata["value"]},
                                    }
                                }
                            )
                        filters.append({parent_key: {"$all": all_matches}})
                    elif parent_key == "relations":
                        for relation in restrictions[parent_key]:
                            all_matches.append(
                                {
                                    "$elemMatch": {
                                        "type": relation["key"],
                                        "key": {"$in": relation["value"]},
                                    }
                                }
                            )
                        filters.append({parent_key: {"$all": all_matches}})
                    elif parent_key == "root":
                        for restriction in restrictions[parent_key]:
                            filters.append(
                                {restriction["key"]: {"$in": restriction["value"]}}
                            )
            else:
                return None
        else:
            filters = [
                {"type": {"$in": allowed_item_types}},
                {
                    "relations": {
                        "$elemMatch": {
                            "key": user_context.bag.get(
                                "tenant_defining_entity_id", user_context.x_tenant.id
                            ),
                            "type": user_context.bag["tenant_relation_type"],
                        }
                    }
                },
            ]

        user_context.access_restrictions.filters = filters
        user_context.access_restrictions.post_request_hook = (
            get_mask_protected_content_post_request_hook(user_context, permissions)
        )
        return True

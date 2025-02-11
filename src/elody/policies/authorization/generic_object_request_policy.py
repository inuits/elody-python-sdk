import re as regex

from elody.policies.helpers import get_content
from configuration import get_object_configuration_mapper  # pyright: ignore
from elody.policies.permission_handler import (
    get_permissions,
    handle_single_item_request,
    mask_protected_content_post_request_hook,
)
from elody.util import interpret_flat_key
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
        if not regex.match("^(/[^/]+/v[0-9]+)?/[^/]+$", request.path):
            return policy_context

        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [PostRequestRules, GetRequestRules]
            access_verdict = None
            for rule in rules:
                access_verdict = rule().apply(user_context, request, permissions)
                if access_verdict is not None:
                    policy_context.access_verdict = access_verdict
                    if not policy_context.access_verdict:
                        break

            if policy_context.access_verdict:
                return policy_context

        return policy_context


class PostRequestRules:
    def apply(
        self, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "POST":
            return None
        if request.args.get("dry_run", False):
            return True
        if regex.match(r"^/batch?$", request.path):
            return True

        content = get_content(request.json, request, request.json)
        return handle_single_item_request(
            user_context, request.json, permissions, "create", content
        )


class GetRequestRules:
    def apply(
        self, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "GET":
            return None
        type_query_parameter = (
            "mediafile"
            if regex.match(r"^/mediafiles(?:\?(.*))?$", request.path)
            else request.args.get("type")
        )
        allowed_item_types = list(permissions["read"].keys())
        filters = []

        if type_query_parameter:
            if type_query_parameter in allowed_item_types:
                config = get_object_configuration_mapper().get(type_query_parameter)
                object_lists = config.document_info()["object_lists"]

                restrictions = permissions["read"][type_query_parameter].get(
                    "object_restrictions", {}
                )
                for key, value in restrictions.items():
                    keys_info = interpret_flat_key(key, object_lists)
                    filters.append(
                        _build_nested_matcher(object_lists, keys_info, value)
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
                            "type": [
                                user_context.bag["tenant_relation_type"],
                                "belongsTo",
                            ],
                        }
                    }
                },
            ]

        user_context.access_restrictions.filters = filters
        user_context.access_restrictions.post_request_hook = (
            mask_protected_content_post_request_hook(user_context, permissions)
        )
        return True


def _build_nested_matcher(object_lists, keys_info, value, index=0):
    info = keys_info[index]

    if info["object_list"]:
        nested_matcher = _build_nested_matcher(
            object_lists, keys_info, value, index + 1
        )
        elem_match = {
            "$elemMatch": {
                object_lists[info["key"]]: info["object_key"],
                keys_info[index + 1]["key"]: nested_matcher,
            }
        }
        return elem_match if index > 0 else {info["key"]: elem_match}

    if isinstance(value, list):
        value = {"$in": value}
    return value if index > 0 else {info["key"]: value}

import re as regex

from copy import deepcopy
from elody.policies.helpers import generate_filter_key_and_lookup_from_restricted_key
from elody.policies.permission_handler import (
    get_permissions,
    mask_protected_content_post_request_hook,
)
from flask import g, Request  # pyright: ignore
from inuits_policy_based_auth import BaseAuthorizationPolicy  # pyright: ignore
from inuits_policy_based_auth.contexts.policy_context import (  # pyright: ignore
    PolicyContext,
)
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)
from werkzeug.exceptions import BadRequest


class FilterGenericObjectsPolicyV2(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not regex.match("^(/[^/]+/v[0-9]+)?/[^/]+/filter$", request.path):
            return policy_context

        if not isinstance(user_context.access_restrictions.filters, list):
            user_context.access_restrictions.filters = []
        type_filter, filters = self.__split_type_filter(
            deepcopy(g.get("content") or request.json or [])
        )

        policy_context.access_verdict = False
        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [PostRequestRules]
            access_verdict = None
            for rule in rules:
                access_verdict = rule().apply(
                    deepcopy(type_filter["value"]),
                    filters,
                    user_context,
                    request,
                    permissions,
                )
                if access_verdict is not None:
                    policy_context.access_verdict = access_verdict
                    if not policy_context.access_verdict:
                        break

            if policy_context.access_verdict:
                return policy_context

        return policy_context

    def __split_type_filter(self, request_body: list):
        type_filter = None
        for filter in request_body:
            if filter["type"] == "type":
                type_filter = filter
                break
            elif filter["type"] == "selection" and filter["key"] == "type":
                type_filter = filter
                break
            elif item_types := filter.get("item_types"):
                type_filter = {
                    "type": "selection",
                    "key": "type",
                    "value": item_types,
                    "match_exact": True,
                }
                break

        if not type_filter:
            raise BadRequest(
                "Filter with type 'type', or a filter with type 'selection' and 'key' equal to 'type' is required"
            )

        try:
            request_body.remove(type_filter)
        except ValueError:
            pass
        return type_filter, request_body


class PostRequestRules:
    def apply(
        self,
        type_filter_values: str | list[str],
        filters: list[dict],
        user_context: UserContext,
        request: Request,
        permissions,
    ) -> bool | None:
        if request.method != "POST":
            return None

        if isinstance(type_filter_values, str):
            type_filter_values = [type_filter_values]

        type_filter_values_copy = deepcopy(type_filter_values)
        for type_filter_value in type_filter_values_copy:
            if type_filter_value not in permissions["read"].keys():
                type_filter_values.remove(type_filter_value)
                continue

            restrictions_grouped_by_index = {}
            schemas = permissions["read"][type_filter_value]
            for schema in schemas.keys():
                restrictions = schemas[schema].get("object_restrictions", {})
                for restricted_key, restricting_value in restrictions.items():
                    index, restricted_key = restricted_key.split(":")
                    restricted_key, lookup = (
                        generate_filter_key_and_lookup_from_restricted_key(
                            restricted_key
                        )
                    )
                    key = f"{schema}|{restricted_key}"
                    if group := restrictions_grouped_by_index.get(index):
                        group["key"].append(key)
                    else:
                        restrictions_grouped_by_index.update(
                            {
                                index: {
                                    "lookup": lookup,
                                    "key": [key],
                                    "value": restricting_value,
                                }
                            }
                        )

            # support false soft call read responses
            for filter in filters:
                key = filter.get("key", "")
                if isinstance(key, list):
                    key = ",".join(key)
                for restriction in restrictions_grouped_by_index.values():
                    if key not in ",".join(restriction["key"]):
                        continue
                    values = (
                        filter["value"]
                        if isinstance(filter["value"], list)
                        else [filter["value"]]
                    )
                    for value in values:
                        if value not in restriction["value"] and value not in ["", "*"]:
                            return False

            for restriction in restrictions_grouped_by_index.values():
                user_context.access_restrictions.filters.append(  # pyright: ignore
                    {
                        "lookup": restriction["lookup"],
                        "type": "selection",
                        "key": restriction["key"],
                        "value": restriction["value"],
                        "match_exact": True,
                    }
                )

        if len(type_filter_values) == 0:
            return False
        user_context.access_restrictions.post_request_hook = (
            mask_protected_content_post_request_hook(user_context, permissions)
        )
        return True

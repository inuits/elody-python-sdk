import re as regex

from copy import deepcopy
from elody.policies.permission_handler import (
    get_permissions,
    get_mask_protected_content_post_request_hook,
)
from flask import Request  # pyright: ignore
from inuits_policy_based_auth import BaseAuthorizationPolicy  # pyright: ignore
from inuits_policy_based_auth.contexts.policy_context import (  # pyright: ignore
    PolicyContext,
)
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)


class FilterGenericObjectsPolicy(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not user_context.auth_objects.get("token") or not regex.match(
            "^/[^/]+/filter$", request.path
        ):
            return policy_context

        if not isinstance(user_context.access_restrictions.filters, list):
            user_context.access_restrictions.filters = []
        type_filter = self.__get_type_filter(user_context, request.json or [])
        if not type_filter:
            policy_context.access_verdict = True
            return policy_context

        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [PostRequestRules]
            access_verdict = None
            for rule in rules:
                access_verdict = rule().apply(
                    type_filter["value"], user_context, request, permissions
                )
                if access_verdict != None:
                    policy_context.access_verdict = access_verdict
                    if not policy_context.access_verdict:
                        return policy_context

            if policy_context.access_verdict:
                return policy_context

        return policy_context

    def __get_type_filter(self, user_context: UserContext, request_body: list):
        type_filter = None
        for filter in request_body:
            if filter["type"] == "type":
                type_filter = filter
            elif (
                filter["type"] == "selection"
                and filter["parent_key"] == ""
                and filter["key"] == "type"
            ):
                type_filter = filter

        if not type_filter:
            user_context.access_restrictions.filters.append(  # pyright: ignore
                {
                    "type": "selection",
                    "parent_key": "relations",
                    "key": user_context.bag["tenant_relation_type"],
                    "value": [
                        user_context.bag.get(
                            "tenant_defining_entity_id", user_context.x_tenant.id
                        )
                    ],
                    "match_exact": True,
                }
            )
            return None
        return type_filter


class PostRequestRules:
    def apply(
        self,
        type_filter_values: str | list[str],
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

            restrictions = permissions["read"][type_filter_value].get(
                "restrictions", {}
            )
            for parent_key in restrictions.keys():
                for restriction in restrictions[parent_key]:
                    user_context.access_restrictions.filters.append(  # pyright: ignore
                        {
                            "type": "selection",
                            "parent_key": parent_key if parent_key != "root" else "",
                            "key": restriction["key"],
                            "value": restriction["value"],
                            "match_exact": True,
                        }
                    )

        if len(type_filter_values) == 0:
            return None
        user_context.access_restrictions.post_request_hook = (
            get_mask_protected_content_post_request_hook(user_context, permissions)
        )
        return True

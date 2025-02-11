import re as regex

from copy import deepcopy
from elody.policies.permission_handler import (
    get_permissions,
    mask_protected_content_post_request_hook,
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
        if not regex.match("^(/[^/]+/v[0-9]+)?/[^/]+/filter$", request.path):
            return policy_context

        if not isinstance(user_context.access_restrictions.filters, list):
            user_context.access_restrictions.filters = []
        type_filter = self.__get_type_filter(user_context, request.json or [])
        if not type_filter:
            policy_context.access_verdict = True
            return policy_context

        policy_context.access_verdict = False
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
                if access_verdict is not None:
                    policy_context.access_verdict = access_verdict
                    if not policy_context.access_verdict:
                        break

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
                and filter.get("parent_key", "") == ""
                and filter["key"] == "type"
            ):
                type_filter = filter

        if not type_filter:
            user_context.access_restrictions.filters.append(  # pyright: ignore
                {
                    "type": "selection",
                    "key": "type",
                    # TODO refactor this in a more generic way
                    "value": [
                        "language",
                        "type",
                        "collectionForm",
                        "institution",
                        "tag",
                        "triple",
                        "person",
                        "externalRecord",
                        "verzameling",
                        "arches_record",
                        "photographer",
                        "creator",
                        "assetPart",
                        "set",
                        "license",
                        "mediafile",
                        "share_link",
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

            restrictions_grouped_by_index = {}
            schemas = permissions["read"][type_filter_value]
            for schema in schemas.keys():
                restrictions = schemas[schema].get("object_restrictions", {})
                for restricted_key, restricting_value in restrictions.items():
                    index, restricted_key = restricted_key.split(":")
                    prefix = ""
                    if restricted_key[0] == "!":
                        restricted_key = restricted_key[1:]
                        prefix += "!"
                    if restricted_key[0] == "?":
                        restricted_key = restricted_key[1:]
                        prefix += "?"
                    key = f"{schema}|{restricted_key}"

                    if group := restrictions_grouped_by_index.get(index):
                        group["key"].append(key)
                    else:
                        restrictions_grouped_by_index.update(
                            {
                                index: {
                                    "key": [key],
                                    "value": restricting_value,
                                    "prefix": prefix,
                                }
                            }
                        )

            for restriction in restrictions_grouped_by_index.values():
                user_context.access_restrictions.filters.append(  # pyright: ignore
                    {
                        "type": "selection",
                        "key": restriction["key"],
                        "value": restriction["value"],
                        "match_exact": True,
                        "operator": "or" if restriction["prefix"] == "?" else "and",
                    }
                )
                if restriction["prefix"] == "?":
                    user_context.access_restrictions.filters.append(  # pyright: ignore
                        {
                            "type": "text",
                            "key": restriction["key"],
                            "value": "",
                            "operator": "or",
                        }
                    )

        if len(type_filter_values) == 0:
            return False
        user_context.access_restrictions.post_request_hook = (
            mask_protected_content_post_request_hook(user_context, permissions)
        )
        return True

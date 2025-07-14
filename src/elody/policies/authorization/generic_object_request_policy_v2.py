import re as regex

from configuration import get_object_configuration_mapper  # pyright: ignore
from elody.policies.helpers import (
    generate_filter_key_and_lookup_from_restricted_key,
    get_content,
)
from elody.policies.permission_handler import (
    get_permissions,
    handle_single_item_request,
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


class GenericObjectRequestPolicyV2(BaseAuthorizationPolicy):
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

        content = g.get("content") or request.json
        content = get_content(content, request, content)
        schema_type = get_object_configuration_mapper().get(content["type"]).SCHEMA_TYPE
        item = {**content, "schema": {"type": schema_type}}
        return handle_single_item_request(
            user_context, item, permissions, "create", content
        )


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
            if type_query_parameter not in allowed_item_types:
                return None
            type_permissions = permissions["read"][type_query_parameter]
            schemas = list(type_permissions.keys())
            if len(schemas) > 0:
                number_of_object_restrictions = len(
                    type_permissions[schemas[0]].get("object_restrictions", {}).keys()
                )
                for i in range(number_of_object_restrictions):
                    lookup, keys, values = {}, [], []
                    for schema in schemas:
                        object_restrictions = type_permissions[schema].get(
                            "object_restrictions", {}
                        )
                        key = [
                            key
                            for key in object_restrictions.keys()
                            if key.startswith(f"{i}:")
                        ][0]
                        values = object_restrictions[key]
                        key, lookup = (
                            generate_filter_key_and_lookup_from_restricted_key(
                                key.split(":")[1]
                            )
                        )
                        keys.append(f"{schema}|{key}")
                    filters.append(
                        {
                            "lookup": lookup,
                            "type": "selection",
                            "key": keys,
                            "value": values,
                            "match_exact": True,
                        }
                    )
            filters.insert(0, {"type": "type", "value": type_query_parameter})
        else:
            """
            Allowing no type_query_parameter will expose following security risks:
                - object_restrictions not being applied
                - inability to determine collection to execute filters
            => if no type_query_parameter is a business requirement, override this policy for that specific client and consider the security risks
            """
            raise BadRequest("Query parameter 'type' is required")

        user_context.access_restrictions.filters = filters
        user_context.access_restrictions.post_request_hook = (
            mask_protected_content_post_request_hook(user_context, permissions)
        )
        return True

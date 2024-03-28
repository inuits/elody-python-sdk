import re as regex

from elody.policies.permission_handler import get_permissions
from flask import Request  # pyright: ignore
from inuits_policy_based_auth import BaseAuthorizationPolicy  # pyright: ignore


class TenantRequestPolicy(BaseAuthorizationPolicy):
    def authorize(self, policy_context, user_context, request_context):
        request: Request = request_context.http_request
        if not regex.match("^(/[^/]+/v[0-9]+)?/tenants$", request.path):
            return policy_context

        set_restricting_filter = True
        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if "tenant" in permissions.get("read", {}).keys():
                set_restricting_filter = False
                break

        if set_restricting_filter:
            user_context.access_restrictions.filters = {
                "relations": {
                    "$elemMatch": {
                        "key": {"$in": user_context.bag["user_ids"]},
                        "type": "isTenantFor",
                    }
                }
            }

        policy_context.access_verdict = True
        return policy_context

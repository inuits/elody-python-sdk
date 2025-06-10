import re as regex

from elody.policies.helpers import get_item
from elody.policies.permission_handler import (
    get_permissions,
    handle_single_item_request,
)
from flask import Request  # pyright: ignore
from inuits_policy_based_auth import BaseAuthorizationPolicy  # pyright: ignore
from inuits_policy_based_auth.contexts.policy_context import (  # pyright: ignore
    PolicyContext,
)
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)
from storage.storagemanager import StorageManager  # pyright: ignore


class MediafileDownloadPolicy(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not regex.match(
            "^(/elody/v[0-9]+)?/mediafiles/[^/]+/download$", request.path
        ):
            return policy_context

        item = get_item(StorageManager(), user_context.bag, request.view_args)
        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [GetRequestRules]
            access_verdict = None
            for rule in rules:
                access_verdict = rule().apply(item, user_context, request, permissions)
                if access_verdict is not None:
                    policy_context.access_verdict = access_verdict
                    if not policy_context.access_verdict:
                        break

            if policy_context.access_verdict:
                return policy_context

        return policy_context


class GetRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "GET":
            return None

        return handle_single_item_request(user_context, item, permissions, "read")

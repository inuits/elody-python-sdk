import re as regex

from elody.policies.permission_handler import (
    get_mask_protected_content_post_request_hook,
    get_permissions,
    handle_single_item_request,
)
from flask import Request  # pyright: ignore
from flask_restful import abort  # pyright: ignore
from inuits_policy_based_auth import BaseAuthorizationPolicy  # pyright: ignore
from inuits_policy_based_auth.contexts.policy_context import (  # pyright: ignore
    PolicyContext,
)
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)
from storage.storagemanager import StorageManager  # pyright: ignore


class GenericObjectMetadataPolicy(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not user_context.auth_objects.get("token") or not regex.match(
            "^/[^/]+/[^/]+/metadata$", request.path
        ):
            return policy_context

        view_args = request.view_args or {}
        collection = view_args.get("collection", request.path.split("/")[1])
        id = view_args.get("id")
        item = (
            StorageManager()
            .get_db_engine()
            .get_item_from_collection_by_id(collection, id)
        )
        if not item:
            abort(
                404,
                message=f"Item with id {id} doesn't exist in collection {collection}",
            )

        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [GetRequestRules, PutRequestRules, PatchRequestRules]
            access_verdict = None
            for rule in rules:
                access_verdict = rule().apply(item, user_context, request, permissions)
                if access_verdict != None:
                    policy_context.access_verdict = access_verdict
                    if not policy_context.access_verdict:
                        return policy_context

            if policy_context.access_verdict:
                return policy_context

        return policy_context


class GetRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "GET":
            return None

        user_context.access_restrictions.post_request_hook = (
            get_mask_protected_content_post_request_hook(
                user_context, permissions, item["type"]
            )
        )
        return handle_single_item_request(user_context, item, permissions, "read")


class PutRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "PUT":
            return None

        return handle_single_item_request(
            user_context, item, permissions, "update", {"metadata": request.json}
        )


class PatchRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "PATCH":
            return None

        return handle_single_item_request(
            user_context, item, permissions, "update", {"metadata": request.json}
        )

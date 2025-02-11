import re as regex

from elody.error_codes import ErrorCode, get_error_code, get_read, get_write
from elody.policies.permission_handler import (
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


class GenericObjectMediafilesPolicy(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not regex.match("^(/[^/]+/v[0-9]+)?/[^/]+/[^/]+/mediafiles$", request.path):
            return policy_context

        view_args = request.view_args or {}
        collection = view_args.get("collection", request.path.split("/")[-3])
        id = view_args.get("id")
        item = (
            StorageManager()
            .get_db_engine()
            .get_item_from_collection_by_id(collection, id)
        )
        if not item:
            abort(
                404,
                message=f"{get_error_code(ErrorCode.ITEM_NOT_FOUND_IN_COLLECTION, get_read())} | id:{id} | collection:{collection} - Item with id {id} doesn't exist in collection {collection}",
            )

        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [
                PostRequestRules,
                GetRequestRules,
            ]
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


class PostRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "POST":
            return None

        return handle_single_item_request(user_context, item, permissions, "create")


class GetRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "GET":
            return None

        return handle_single_item_request(user_context, item, permissions, "read")

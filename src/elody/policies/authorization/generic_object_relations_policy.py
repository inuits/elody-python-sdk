import app  # pyright: ignore
import re as regex

from elody.policies.helpers import get_item
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


class GenericObjectRelationsPolicy(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not regex.match("^(/elody/v[0-9]+)?/[^/]+/[^/]+/relations$", request.path):
            return policy_context

        item = get_item(StorageManager(), user_context.bag, request.view_args)
        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            rules = [
                PostRequestRules,
                GetRequestRules,
                PutRequestRules,
                PatchRequestRules,
                DeleteRequestRules,
            ]
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


class PostRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "POST":
            return None

        content = app.serialize(
            {"relations": request.json},
            type=item.get("type"),
            from_format="elody",
            to_format=app.object_configuration_mapper.get(item["type"]).SCHEMA_TYPE,
        )
        return handle_single_item_request(
            user_context, item, permissions, "create", content
        )


class GetRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "GET":
            return None

        return handle_single_item_request(user_context, item, permissions, "read")


class PutRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "PUT":
            return None

        content = app.serialize(
            {"relations": request.json},
            type=item.get("type"),
            from_format="elody",
            to_format=app.object_configuration_mapper.get(item["type"]).SCHEMA_TYPE,
        )
        return handle_single_item_request(
            user_context, item, permissions, "update", content
        )


class PatchRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "PATCH":
            return None

        content = app.serialize(
            {"relations": request.json},
            type=item.get("type"),
            from_format="elody",
            to_format=app.object_configuration_mapper.get(item["type"]).SCHEMA_TYPE,
        )
        return handle_single_item_request(
            user_context, item, permissions, "update", content
        )


class DeleteRequestRules:
    def apply(
        self, item, user_context: UserContext, request: Request, permissions
    ) -> bool | None:
        if request.method != "DELETE":
            return None

        content = app.serialize(
            {"relations": request.json},
            type=item.get("type"),
            from_format="elody",
            to_format=app.object_configuration_mapper.get(item["type"]).SCHEMA_TYPE,
        )
        return handle_single_item_request(
            user_context, item, permissions, "delete", content
        )

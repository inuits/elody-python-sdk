import re as regex

from abc import ABC, abstractmethod
from configuration import get_object_configuration_mapper  # pyright: ignore
from copy import deepcopy
from elody.util import get_raw_id
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)
from inuits_policy_based_auth.helpers.tenant import Tenant  # pyright: ignore
from storage.storagemanager import StorageManager  # pyright: ignore
from werkzeug.exceptions import Forbidden  # pyright: ignore


class BaseUserTenantValidationPolicy(ABC):
    def __init__(self) -> None:
        self.storage = StorageManager().get_db_engine()
        self.super_tenant_id = "tenant:super"
        self.user = {}

    @abstractmethod
    def get_user(self, id: str, user_context: UserContext) -> dict:
        user_context.bag["roles_from_idp"] = deepcopy(user_context.x_tenant.roles)

    @abstractmethod
    def build_user_context_for_authenticated_user(
        self, request, user_context: UserContext, user: dict
    ):
        self.user = user
        user_context.x_tenant = Tenant()
        user_context.x_tenant.id = self._determine_tenant_id(request, user)
        user_context.x_tenant.roles = self.__get_tenant_roles(
            user_context.x_tenant.id, request
        )
        user_context.x_tenant.raw = self.__get_x_tenant_raw(user_context.x_tenant.id)
        user_context.tenants = [user_context.x_tenant]
        user_context.bag["x_tenant_id"] = user_context.x_tenant.id
        user_context.bag["tenant_defining_entity_id"] = user_context.x_tenant.id
        user_context.bag["tenant_relation_type"] = "isIn"
        user_context.bag["user_ids"] = self.user["identifiers"]
        user_context.bag["http_method"] = request.method
        user_context.bag["requested_endpoint"] = request.endpoint
        user_context.bag["full_path"] = request.full_path

    @abstractmethod
    def build_user_context_for_anonymous_user(
        self, user_context: UserContext, user: dict
    ):
        self.user = user
        user_context.id = get_raw_id(user)
        user_context.x_tenant = Tenant()
        user_context.x_tenant.id = self.super_tenant_id
        user_context.x_tenant.roles = ["anonymous"]
        user_context.x_tenant.raw = self.__get_x_tenant_raw(user_context.x_tenant.id)
        user_context.tenants = [user_context.x_tenant]
        user_context.bag["x_tenant_id"] = user_context.x_tenant.id
        user_context.bag["tenant_defining_entity_id"] = user_context.x_tenant.id
        user_context.bag["tenant_relation_type"] = "isIn"
        user_context.bag["user_ids"] = self.user["identifiers"]

    @abstractmethod
    def _determine_tenant_id(self, request, user) -> str:
        pass

    def __get_tenant_roles(self, x_tenant_id: str, request) -> list[str]:
        roles = self.__get_user_tenant_relation(self.super_tenant_id).get("roles", [])
        if x_tenant_id != self.super_tenant_id:
            try:
                user_tenant_relation = self.__get_user_tenant_relation(x_tenant_id)
            except Forbidden as error:
                user_tenant_relation = {}
                if len(roles) == 0:
                    raise Forbidden(error.description)
            roles.extend(user_tenant_relation.get("roles", []))

        if len(roles) == 0 and not regex.match(
            "(/[^/]+/v[0-9]+)?/tenants$", request.path
        ):
            raise Forbidden("User has no global roles, switch to a specific tenant.")
        return roles

    def __get_user_tenant_relation(self, x_tenant_id: str) -> dict:
        user_tenant_relation = None
        for relation in self.user.get("relations", []):
            if relation["key"] == x_tenant_id and relation["type"] == "hasTenant":
                user_tenant_relation = relation
                break

        if not user_tenant_relation:
            if x_tenant_id != self.super_tenant_id:
                raise Forbidden(f"User is not a member of tenant {x_tenant_id}.")
            else:
                return {}

        return user_tenant_relation

    def __get_x_tenant_raw(self, x_tenant_id: str) -> dict:
        collection = (
            get_object_configuration_mapper().get("tenant").crud()["collection"]
        )
        x_tenant_raw = (
            self.storage.get_item_from_collection_by_id(collection, x_tenant_id) or {}
        )
        if x_tenant_raw.get("type") != "tenant":
            raise Forbidden(f"No tenant {x_tenant_id} exists.")

        return x_tenant_raw

    def _get_tenant_defining_entity_id(
        self, x_tenant_id: str, x_tenant_raw: dict
    ) -> str:
        if x_tenant_id == self.super_tenant_id:
            return x_tenant_id.removeprefix("tenant:")

        tenant_defining_entity_id = None
        for relation in x_tenant_raw.get("relations", []):
            if relation["type"] == "definedBy":
                tenant_defining_entity_id = relation["key"]
                break
        if not tenant_defining_entity_id:
            raise Forbidden(
                f"{x_tenant_raw['_id']} has no relation with a tenant defining entity."
            )

        return tenant_defining_entity_id

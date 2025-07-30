import re as regex

from abc import ABC, abstractmethod
from configuration import get_object_configuration_mapper  # pyright: ignore
from copy import deepcopy
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)
from inuits_policy_based_auth.helpers.tenant import Tenant  # pyright: ignore
from werkzeug.exceptions import Forbidden  # pyright: ignore


class BaseUserTenantValidationPolicy(ABC):
    @abstractmethod
    def get_user(
        self,
        id: str,
        user_context: UserContext,
        storage,
        *,
        user_metadata_key_for_global_roles="roles",
        user_tenant_relation_type="hasTenant",
    ) -> dict:
        config = get_object_configuration_mapper().get("user")
        collection = config.crud()["collection"]
        self.serialize = config.serialization(config.SCHEMA_TYPE, "elody")

        user = storage.get_item_from_collection_by_id(collection, id) or {}
        self.user = self.serialize(user)
        user_context.bag["roles_from_idp"] = deepcopy(user_context.x_tenant.roles)
        user_context.bag["user_metadata_key_for_global_roles"] = (
            user_metadata_key_for_global_roles
        )
        user_context.bag["user_tenant_relation_type"] = user_tenant_relation_type

        return self.user

    @abstractmethod
    def build_user_context_for_anonymous_user(
        self, request, user_context: UserContext
    ) -> UserContext:
        user_context = self.__build_user_context(request, user_context)
        user_context.id = "anonymous"
        user_context.x_tenant = Tenant()
        user_context.x_tenant.id = ""
        user_context.x_tenant.roles = ["anonymous"]
        return user_context

    @abstractmethod
    def build_user_context_for_authenticated_user(
        self, request, user_context: UserContext, user: dict
    ) -> UserContext:
        self.user = self.serialize(user)
        user_context = self.__build_user_context(request, user_context)
        user_context.x_tenant = Tenant()
        user_context.x_tenant.id = self._determine_tenant_id(request, user_context)
        user_context.x_tenant.roles = self.__get_tenant_roles(request, user_context)
        return user_context

    @abstractmethod
    def _determine_tenant_id(self, request, user_context: UserContext) -> str:
        pass

    def __build_user_context(self, request, user_context: UserContext):
        user_context.bag["http_method"] = request.method
        user_context.bag["requested_endpoint"] = request.endpoint
        user_context.bag["full_path"] = request.full_path
        user_context.bag["collection_resolver"] = (
            self._resolve_collections  # pyright: ignore
        )
        return user_context

    def __get_tenant_roles(self, request, user_context: UserContext) -> list[str]:
        """
        Gathering multiple tenants are supported, but there are some important notes:
            - all roles are stored in a combined way in user_context.x_tenant.roles
            - those combination of different tenant roles will work, as long as:
                - the roles are the same
                - the object restrictions allow all tenants that are linked with the user
                => to make it fully work, see #143185
            - with combined roles, when combaring rol_a vs rol_b
                - if rol_a allow and role_b does not allow access: the role allowing access will always win
                - if both roles allow, but just have different restrictions: the first allowing role will win
                    - this is completely random
                    => currently no logic in policies to determine which allowing role to apply
        """

        roles = []
        for metadata in self.user.get("metadata", []):
            if (
                metadata["key"]
                == user_context.bag["user_metadata_key_for_global_roles"]
            ):
                roles.extend(metadata["value"])

        if user_context.x_tenant.id:
            tenant_ids = user_context.x_tenant.id.split(",")
            for tenant_id in tenant_ids:
                try:
                    user_tenant_relation = self.__get_user_tenant_relation(
                        tenant_id, user_context.bag["user_tenant_relation_type"]
                    )
                except Forbidden as error:
                    user_tenant_relation = {}
                    if len(roles) == 0:
                        raise Forbidden(error.description)
                roles.extend(user_tenant_relation.get("roles", []))

        if len(roles) == 0 and not regex.match(
            "(/[^/]+/v[0-9]+)?/tenants$", request.path
        ):
            raise Forbidden("User has no global roles, switch to a specific tenant.")

        return list(set(roles))

    def __get_user_tenant_relation(
        self, tenant_id: str, user_tenant_relation_type: str
    ) -> dict:
        user_tenant_relation = None
        for relation in self.user.get("relations", []):
            if (
                relation["key"] == tenant_id
                and relation["type"] == user_tenant_relation_type
            ):
                user_tenant_relation = relation
                break

        if not user_tenant_relation:
            if tenant_id:
                raise Forbidden(f"User is not a member of tenant {tenant_id}.")
            return {}

        return user_tenant_relation

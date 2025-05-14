from hashlib import sha256
from inuits_policy_based_auth import BaseAuthenticationPolicy, RequestContext
from inuits_policy_based_auth.contexts import UserContext
from storage.storagemanager import StorageManager
from werkzeug.exceptions import Forbidden


class MultiTenantPolicy(BaseAuthenticationPolicy):
    """
    An authentication policy that gets or creates (when applicable) a
    tenant, using the details from the configured tenant defining
    header (default 'X-tenant-id').

    Parameters:
    -----------
    defining_header : str
        Which header receives an identifier to get or create a
        tenant. (default 'X-tenant-id')
    defining_types : list
        List of types that defines a tenant.
    auto_create_tenants : bool
        Configures the auto creation of tenants.
    """

    def __init__(self, defining_header, defining_types, auto_create_tenants):
        self._defining_header = defining_header
        self._defining_types = defining_types
        self._auto_create_tenants = auto_create_tenants

    def __allowed_url_rule_with_api_key_hash(self, url_rule):
        return str(url_rule) in [
            "/download/<string:app_id>/<string:mediafile_id_prefix>/<string:mediafile_id>/<string:filename>",
            "/media/<string:app_id>/<string:mediafile_id_prefix>/<string:mediafile_id>/<string:filename>",
            "/mediafiles/<string:id>",
            "/mediafiles/<string:id>/copyright",
            "/tickets/<string:id>",
        ]

    def __get_tenant_id_from_hashed_api_key(self, api_key_hash):
        storage = StorageManager().get_db_engine()
        tenant = None
        if not (
            tenant := storage.get_item_from_collection_by_id("entities", api_key_hash)
        ):
            tenants = storage.get_entities(0, 0, 1, {"type": "tenant"})
            for possible_tenant in tenants.get("results", list()):
                for id in possible_tenant.get("identifiers", list()):
                    if api_key_hash == sha256(id.encode()).hexdigest():
                        return possible_tenant
        return tenant

    def authenticate(self, user_context: UserContext, request_context: RequestContext):
        """
        Get tenant from tenant defining header and set x_tenant accordingly.

        Parameters:
        -----------
        user_context : UserContext
            The context of the user requesting authentication.
        request_context : RequestContext
            The context of the request.

        Returns:
        --------
        UserContext
            The user context with x_tenant set.

        Raises:
        -------
        Unauthorized
            If the authentication fails.
        """

        if self._defining_types:
            return user_context
        auth_header = self._defining_header
        if not (tenant_id := request_context.http_request.headers.get(auth_header)):
            if (
                not (
                    tenant := self.__get_tenant_id_from_hashed_api_key(
                        request_context.http_request.args.get("api_key_hash")
                    )
                )
                or request_context.http_request.method != "GET"
                or not self.__allowed_url_rule_with_api_key_hash(
                    request_context.http_request.url_rule
                )
            ):
                raise Forbidden(description=f"{auth_header} header not present")
        else:
            storage = StorageManager().get_db_engine()
            tenant = storage.get_item_from_collection_by_id("entities", tenant_id)
        if not tenant:
            if not self._auto_create_tenants:
                raise Forbidden(
                    description=f"Tenant with identifier {tenant_id} not found"
                )
            tenant = storage.save_item_to_collection(
                "entities", {"type": "tenant", "identifiers": [tenant_id]}
            )
        user_context.x_tenant.id = tenant["_id"]
        user_context.x_tenant.raw = tenant
        return user_context

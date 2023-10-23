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
            raise Forbidden(description=f"{auth_header} header not present")
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

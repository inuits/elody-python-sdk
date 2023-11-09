from inuits_policy_based_auth import BaseAuthorizationPolicy, RequestContext
from inuits_policy_based_auth.contexts import UserContext, PolicyContext
from storage.storagemanager import StorageManager


class MultiTenantPolicy(BaseAuthorizationPolicy):
    """
    An authorization policy that checks if you have access to the requested
    item or operation through your tenant.
    """

    def authorize(
        self,
        policy_context: PolicyContext,
        user_context: UserContext,
        request_context: RequestContext,
    ) -> PolicyContext:
        """
        Authorizes a user.

        Parameters
        ----------
        policy_context : PolicyContext
            An object containing data about the context of an applied
            authorization policy.
        user_context : UserContext
            An object containing data about the authenticated user.
        request_context : RequestContext
            An object containing data about the context of a request.

        Returns
        -------
        PolicyContext
            An object containing data about the context of an applied
            authorization policy.
        """
        request = request_context.http_request
        view_args = request.view_args or {}
        item_id = view_args.get("id")
        policy_context.access_verdict = True
        if item_id:
            storage = StorageManager().get_db_engine()
            item_relations = storage.get_collection_item_relations(
                request.path.split("/")[1], item_id
            )
            if not any(
                x
                for x in item_relations
                if x["type"] == "isIn" and x["key"] == user_context.x_tenant.raw["_id"]
            ):
                policy_context.access_verdict = False
        elif "/filter" in request.path:
            user_context.access_restrictions.filters = [
                {
                    "type": "selection",
                    "parent_key": "relations",
                    "key": "isIn",
                    "value": [user_context.x_tenant.raw["_id"]],
                    "match_exact": True,
                }
            ]
        else:
            user_context.access_restrictions.filters = {
                "relations.type": "isIn",
                "relations.key": user_context.x_tenant.raw["_id"],
            }
        return policy_context

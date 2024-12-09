from elody.error_codes import ErrorCode, get_error_code, get_read, get_write
from configuration import get_collection_mapper
from flask_restful import abort
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
            request_name = request.path.split("/")[1]
            collection = get_collection_mapper().get(request_name, request_name)
            item = storage.get_item_from_collection_by_id(collection, item_id)
            if not item:
                abort(
                    404,
                    message=f"{get_error_code(ErrorCode.ITEM_NOT_FOUND_IN_COLLECTION, get_read())} | id:{id} | collection:{collection} - Item with id {id} doesn't exist in collection {collection}",
                )
            item_relations = storage.get_collection_item_relations(collection, item_id)
            if (
                item.get("type") != "ticket"
                and not any(
                    x
                    for x in item_relations
                    if x["type"] == "isIn"
                    and x["key"] == user_context.x_tenant.raw["_id"]
                )
                and collection != "mediafiles"
            ):
                policy_context.access_verdict = False
        if "/filter" in request.path:
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

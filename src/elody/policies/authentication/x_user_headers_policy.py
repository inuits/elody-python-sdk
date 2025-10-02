from configuration import get_object_configuration_mapper
from flask import Request
from inuits_policy_based_auth.authentication.base_authentication_policy import (
    BaseAuthenticationPolicy,
)
from os import getenv
from storage.storagemanager import StorageManager


class XUserHeadersPolicy(BaseAuthenticationPolicy):
    def authenticate(self, user_context, request_context):
        request: Request = request_context.http_request

        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header.removeprefix("Bearer ")
            static_jwt = getenv("STATIC_JWT")

            if static_jwt and token == static_jwt:
                user_email = request.headers.get("X-User-Email")
                if user_email:
                    config = get_object_configuration_mapper().get("user")
                    storage_manager = StorageManager()
                    user = (
                        storage_manager.get_db_engine().get_item_from_collection_by_id(
                            config.crud()["collection"], user_email
                        )
                    )

                    if user:
                        user_context.id = user.get("_id", user.get("id"))
                        user_context.email = user_email

        return user_context

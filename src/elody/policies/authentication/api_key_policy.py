from inuits_policy_based_auth import BaseAuthenticationPolicy
from werkzeug.exceptions import Unauthorized


class ApiKeyPolicy(BaseAuthenticationPolicy):
    def __init__(self, tenant_app_id_mapping, static_api_key):
        self.__tenant_app_id_mapping = tenant_app_id_mapping
        self.__static_api_key = static_api_key

    def authenticate(self, user_context, request_context):
        if not (api_key := request_context.http_request.headers.get("x-api-key")):
            raise Unauthorized("X-API-Key header not present")
        if (
            api_key != self.__static_api_key
            and api_key not in self.__tenant_app_id_mapping
        ):
            raise Unauthorized("Your API key is not registered")

        user_context.auth_objects.add_key_value_pair("api_key", api_key)
        return user_context

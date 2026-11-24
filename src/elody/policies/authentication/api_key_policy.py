from inuits_policy_based_auth import BaseAuthenticationPolicy
from werkzeug.exceptions import Unauthorized


class ApiKeyPolicy(BaseAuthenticationPolicy):
    def __init__(self, api_key_mapping, static_api_key):
        if not api_key_mapping:
            raise ValueError("API_KEY_MAPPING is not set or empty")

        self.__api_key_mapping = api_key_mapping
        self.__static_api_key = static_api_key

    def authenticate(self, user_context, request_context):
        if not (api_key := request_context.http_request.headers.get("x-api-key")):
            raise Unauthorized("X-API-Key header not present")
        if api_key != self.__static_api_key and api_key not in self.__api_key_mapping:
            raise Unauthorized("Your API key is not registered")

        user_context.auth_objects.add_key_value_pair("api_key", api_key)
        return user_context

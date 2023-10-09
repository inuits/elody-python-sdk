import elody.util as util
import json
import os

from importlib import import_module
from inuits_policy_based_auth.exceptions import (
    PolicyFactoryException,
)


def load_apps(flask_app, logger):
    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST"), logger)
    for app in apps:
        for resource in apps[app].get("resources", []):
            api_bp = import_module(f"apps.{app}.resources.{resource}").api_bp
            flask_app.register_blueprint(api_bp)


def load_policies(policy_factory, logger):
    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST"), logger)
    for app in apps:
        try:
            auth_type = "authentication"
            for policy_module_name in apps[app]["policies"].get(auth_type):
                policy = __get_class(app, auth_type, policy_module_name)
                policy = __instantiate_authentication_policy(
                    policy_module_name, policy, logger
                )
                policy_factory.register_authentication_policy(f"apps.{app}", policy)
            auth_type = "authorization"
            for policy_module_name in apps[app]["policies"].get(auth_type):
                policy = __get_class(app, auth_type, policy_module_name)
                policy_factory.register_authorization_policy(f"apps.{app}", policy())
            # FIXME: don't always set last app as fallback
            policy_factory.set_fallback_key_for_policy_mapping(f"apps.{app}")
        except Exception as error:
            raise PolicyFactoryException(
                f"Policy factory was not configured correctly: {str(error)}"
            ).with_traceback(error.__traceback__)


def load_queues(logger):
    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST"), logger)
    for app in apps:
        try:
            import_module(f"apps.{app}.resources.queues")
        except ModuleNotFoundError:
            pass


def __get_class(app, auth_type, policy_module_name):
    try:
        module = import_module(f"apps.{app}.policies.{auth_type}.{policy_module_name}")
    except:
        module = import_module(
            f"inuits_policy_based_auth.{auth_type}.policies.{policy_module_name}"
        )
    policy_class_name = module.__name__.split(".")[-1].title().replace("_", "")
    policy = getattr(module, policy_class_name)
    return policy


def __instantiate_authentication_policy(policy_module_name, policy, logger):
    if policy_module_name == "token_based_policies.authlib_flask_oauth2_policy":
        token_schema = __load_token_schema()
        allowed_issuers = os.getenv("ALLOWED_ISSUERS")
        allow_anonymous_users = (
            True
            if os.getenv("ALLOW_ANONYMOUS_USERS", "false").lower() == "true"
            else False
        )
        return policy(
            logger,
            token_schema,
            os.getenv("STATIC_ISSUER"),
            os.getenv("STATIC_PUBLIC_KEY"),
            allowed_issuers.split(",") if allowed_issuers else None,
            allow_anonymous_users,
        )
    if policy_module_name == "token_based_policies.default_tenant_policy":
        token_schema = __load_token_schema()
        return policy(
            token_schema, os.getenv("ROLE_SCOPE_MAPPING", "role_scope_mapping.json")
        )

    return policy()


def __load_token_schema() -> dict:
    token_schema_path = os.getenv("TOKEN_SCHEMA", "token_schema.json")
    with open(token_schema_path, "r") as token_schema:
        return json.load(token_schema)

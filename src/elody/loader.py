import elody.util as util
import json
import os

from apscheduler.triggers.cron import CronTrigger
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


def load_jobs(scheduler, logger):
    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST"), logger)
    for app in apps:
        for job, job_properties in apps[app].get("jobs", {}).items():
            module_paths = [f"apps.{app}.cron_jobs.{job}", f"cron_jobs.{job}"]
            module = None
            for path in module_paths:
                try:
                    module = import_module(path)
                    break
                except ModuleNotFoundError:
                    pass
            if module:
                job_class = __get_class_from_module(module)
                scheduler.add_job(
                    job_class(),
                    CronTrigger.from_crontab(
                        job_properties.get("expression", "0 0 * * *")
                    ),
                )


def load_policies(
    policy_factory, logger, permissions: dict = {}, placeholders: list[str] = []
):
    if permissions:
        from elody.policies.permission_handler import set_permissions

        set_permissions(permissions, placeholders)

    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST", ""), logger)
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
    import_module("resources.queues")
    apps = util.read_json_as_dict(os.getenv("APPS_MANIFEST"), logger)
    for app in apps:
        try:
            import_module(f"apps.{app}.resources.queues")
        except ModuleNotFoundError:
            pass


def __get_class(app, auth_type, policy_module_name):
    locations = [
        policy_module_name,
        f"apps.{app}.policies.{auth_type}.{policy_module_name}",
        f"elody.policies.{auth_type}.{policy_module_name}",
        f"inuits_policy_based_auth.{auth_type}.policies.{policy_module_name}",
    ]
    for location in locations:
        try:
            module = import_module(location)
            break
        except ModuleNotFoundError:
            pass
    else:
        raise ModuleNotFoundError(f"Policy {policy_module_name} not found")
    policy = __get_class_from_module(module)
    return policy


def __get_class_from_module(module):
    class_name = module.__name__.split(".")[-1].title().replace("_", "")
    return getattr(module, class_name)


def __instantiate_authentication_policy(policy_module_name, policy, logger):
    allow_anonymous_users = os.getenv("ALLOW_ANONYMOUS_USERS", False) in [
        "True",
        "true",
        True,
    ]
    if policy_module_name == "token_based_policies.authlib_flask_oauth2_policy":
        token_schema = __load_token_schema()
        allowed_issuers = os.getenv("ALLOWED_ISSUERS")
        return policy(
            logger,
            token_schema,
            os.getenv("STATIC_ISSUER"),
            os.getenv("STATIC_PUBLIC_KEY"),
            allowed_issuers.split(",") if allowed_issuers else None,
            allow_anonymous_users,
        )
    if policy_module_name == "token_based_policies.tenant_token_roles_policy":
        token_schema = __load_token_schema()
        return policy(
            token_schema,
            os.getenv("ROLE_SCOPE_MAPPING", "role_scope_mapping.json"),
            allow_anonymous_users,
        )
    if policy_module_name == "elody.policies.authentication.multi_tenant_policy":
        tenant_defining_types = os.getenv("TENANT_DEFINING_TYPES")
        tenant_defining_types = (
            tenant_defining_types.split(",") if tenant_defining_types else []
        )
        return policy(
            os.getenv("TENANT_DEFINING_HEADER", "X-tenant-id"),
            tenant_defining_types,
            os.getenv("AUTO_CREATE_TENANTS", False) in ["True", "true", True],
        )
    return policy()


def __load_token_schema() -> dict:
    token_schema_path = os.getenv("TOKEN_SCHEMA", "token_schema.json")
    with open(token_schema_path, "r") as token_schema:
        return json.load(token_schema)

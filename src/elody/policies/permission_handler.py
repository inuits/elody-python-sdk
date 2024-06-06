import re as regex

from configuration import get_object_configuration_mapper  # pyright: ignore
from copy import deepcopy
from elody.util import flatten_dict, interpret_flat_key
from inuits_policy_based_auth.contexts.user_context import UserContext
from logging_elody.log import log  # pyright: ignore


_permissions = {}


def set_permissions(permissions: dict):
    global _permissions
    _permissions = permissions


def get_permissions(role: str, user_context: UserContext):
    permissions = deepcopy(_permissions)
    placeholders = ["X_TENANT_ID", "TENANT_DEFINING_ENTITY_ID"]

    for placeholder in placeholders:
        permissions = __replace_permission_placeholders(
            permissions, placeholder, user_context.bag[placeholder.lower()]
        )
    return permissions.get(role, {})  # pyright: ignore


def __replace_permission_placeholders(data, placeholder_key, placeholder_value):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = __replace_permission_placeholders(
                value, placeholder_key, placeholder_value
            )
    elif isinstance(data, list):
        data = [
            __replace_permission_placeholders(item, placeholder_key, placeholder_value)
            for item in data
        ]
    elif isinstance(data, str):
        data = data.replace(placeholder_key, placeholder_value)
    return data


def handle_single_item_request(
    user_context: UserContext, item, permissions, crud, request_body: dict = {}
):
    try:
        item_in_storage_format, flat_item, object_lists, restrictions_schema = (
            __prepare_item_for_permission_check(item, permissions, crud)
        )

        is_allowed_to_crud_item = (
            __is_allowed_to_crud_item(flat_item, restrictions_schema)
            if flat_item
            else None
        )
        if not is_allowed_to_crud_item:
            return is_allowed_to_crud_item

        return __is_allowed_to_crud_item_keys(
            user_context,
            item_in_storage_format,
            flat_item,
            restrictions_schema,
            crud,
            object_lists,
            flatten_dict(object_lists, request_body),
        )
    except Exception as exception:
        log.debug(
            f"{exception.__class__.__name__}: {str(exception)}",
            item.get("storage_format", item),
        )
        if crud != "read":
            log.debug(f"Request body: {request_body}", {})
        raise exception


def mask_protected_content_post_request_hook(user_context: UserContext, permissions):
    def __post_request_hook(response):
        items = response["results"]
        for item in items:
            try:
                (
                    item_in_storage_format,
                    flat_item,
                    object_lists,
                    restrictions_schema,
                ) = __prepare_item_for_permission_check(item, permissions, "read")
                if not flat_item:
                    continue

                __is_allowed_to_crud_item_keys(
                    user_context,
                    item_in_storage_format,
                    flat_item,
                    restrictions_schema,
                    "read",
                    object_lists,
                )
            except Exception as exception:
                log.debug(
                    f"{exception.__class__.__name__}: {str(exception)}",
                    item.get("storage_format", item),
                )
                raise exception

        return response

    return __post_request_hook


def __prepare_item_for_permission_check(item, permissions, crud):
    item = deepcopy(item.get("storage_format", item))
    if item["type"] not in permissions[crud].keys():
        return item, None, None, None

    config = get_object_configuration_mapper().get(item["type"])
    object_lists = config.document_info()["object_lists"]
    flat_item = flatten_dict(object_lists, item)

    return (
        item,
        flat_item,
        object_lists,
        __get_restrictions_schema(flat_item, permissions, crud),
    )


def __get_restrictions_schema(flat_item, permissions, crud):
    schema_type = flat_item.get("schema.type", "elody")
    schema_version = flat_item.get("schema.version", "1")
    schema = f"{schema_type}:{schema_version}"

    schemas = permissions[crud][flat_item["type"]]
    if restrictions_schema := schemas.get(schema):
        return restrictions_schema

    for schema in reversed(schemas.keys()):
        if regex.match(f"^{schema_type}:[0-9]{1,3}?$", schema):
            break
        schema = None
    return schemas[schema] if schemas and schema else {}


def __is_allowed_to_crud_item(flat_item, restrictions_schema):
    restrictions = restrictions_schema.get("object_restrictions", {})

    for restricted_key, restricting_values in restrictions.items():
        restricted_key = restricted_key.split(":")[1]
        item_value_in_restricting_values = __item_value_in_values(
            flat_item, restricted_key, restricting_values
        )
        if not item_value_in_restricting_values:
            return None

    return True


def __is_allowed_to_crud_item_keys(
    user_context: UserContext,
    item_in_storage_format,
    flat_item,
    restrictions_schema,
    crud,
    object_lists,
    flat_request_body: dict = {},
):
    user_context.bag["soft_call_response_body"] = []
    restrictions = restrictions_schema.get("key_restrictions", {})

    for restricted_key, restricting_conditions in restrictions.items():
        restricted_key = restricted_key.split(":")[1]
        condition_match = True
        for condition_key, condition_values in restricting_conditions.items():
            condition_match = __item_value_in_values(
                flat_item, condition_key, condition_values, flat_request_body
            )
            if not condition_match:
                break

        if condition_match:
            if crud == "read":
                keys_info = interpret_flat_key(restricted_key, object_lists)
                element = item_in_storage_format
                for info in keys_info:
                    if info["is_object_list"]:
                        element = __get_element_from_object_list_of_item(
                            element,
                            info["key"],
                            info["object_key"],
                            object_lists,
                        )
                element[info["key"]] = "[protected content]"  # pyright: ignore
            else:
                if flat_request_body.get(restricted_key):
                    user_context.bag["soft_call_response_body"].append(restricted_key)

    user_context.bag["requested_item"] = item_in_storage_format
    return len(user_context.bag["soft_call_response_body"]) == 0


def __item_value_in_values(flat_item, key, values: list, flat_request_body: dict = {}):
    negate_condition = False
    if key[0] == "!":
        key = key[1:]
        negate_condition = True

    item_value = flat_request_body.get(key, flat_item[key])
    expected_values = []
    for value in values:
        if flat_item_key_value := flat_item.get(value):
            value = flat_item_key_value
        if isinstance(value, list):
            expected_values.extend(value)
        else:
            expected_values.append(value)

    if isinstance(item_value, (str, int, float, bool)):
        if negate_condition:
            return item_value not in expected_values
        else:
            return item_value in expected_values
    elif isinstance(item_value, list):
        for expected_value in expected_values:
            if expected_value in item_value:
                return True != negate_condition
        return False != negate_condition

    raise Exception(f"Invalid item_value: {item_value}")


def __get_element_from_object_list_of_item(
    item: dict, object_list: str, key: str, object_lists: dict
):
    for element in item[object_list]:
        if element[object_lists[object_list]] == key:
            return element
    return {}

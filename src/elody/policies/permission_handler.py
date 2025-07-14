import re as regex

from copy import deepcopy
from elody.error_codes import ErrorCode, get_error_code, get_read
from elody.policies.helpers import get_flat_item_and_object_lists, get_item
from elody.util import flatten_dict, interpret_flat_key
from inuits_policy_based_auth.contexts.user_context import UserContext
from logging_elody.log import log  # pyright: ignore
from storage.storagemanager import StorageManager  # pyright: ignore


_permissions = {}
_placeholders = ["X_TENANT_ID", "TENANT_DEFINING_ENTITY_ID"]


def set_permissions(permissions: dict, placeholders: list[str] = []):
    global _permissions
    _permissions = permissions
    _placeholders.extend(placeholders)


def get_permissions(role: str, user_context: UserContext):
    permissions = deepcopy(_permissions)

    for placeholder_key in _placeholders:
        placeholder_value = user_context.bag.get(placeholder_key.lower())
        permissions = __replace_permission_placeholders(
            permissions, placeholder_key, placeholder_value
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
        if isinstance(placeholder_value, str):
            data = data.replace(placeholder_key, placeholder_value)
        elif isinstance(placeholder_value, list) and data == placeholder_key:
            data = placeholder_value
    return data


def handle_single_item_request(
    user_context: UserContext,
    item,
    permissions,
    crud,
    request_body: dict = {},
    *,
    key_to_check=None,
):
    try:
        item, flat_item, object_lists, restrictions_schema = (
            __prepare_item_for_permission_check(item, permissions, crud)
        )

        is_allowed_to_crud_item = (
            __is_allowed_to_crud_item(flat_item, restrictions_schema, user_context)
            if flat_item
            else None
        )
        if not is_allowed_to_crud_item:
            return is_allowed_to_crud_item

        return __is_allowed_to_crud_item_keys(
            user_context,
            item,
            flat_item,
            restrictions_schema,
            crud,
            object_lists,
            flatten_dict(object_lists, request_body),
            key_to_check=key_to_check,
        )
    except Exception as exception:
        log.debug(
            f"{exception.__class__.__name__}: {str(exception)}",
            item,
        )
        if crud != "read":
            log.debug(f"Request body: {request_body}", {})
        raise exception


def mask_protected_content_post_request_hook(user_context: UserContext, permissions):
    def __post_request_hook(response):
        items = []
        for item in response["results"]:
            try:
                (
                    item,
                    flat_item,
                    object_lists,
                    restrictions_schema,
                ) = __prepare_item_for_permission_check(item, permissions, "read")
                if not flat_item:
                    continue

                __is_allowed_to_crud_item_keys(
                    user_context,
                    item,
                    flat_item,
                    restrictions_schema,
                    "read",
                    object_lists,
                )
                items.append(user_context.bag["requested_item"])
            except Exception as exception:
                log.debug(
                    f"{exception.__class__.__name__}: {str(exception)}",
                    item,
                )
                raise exception

        response["results"] = items
        return response

    return __post_request_hook


def __prepare_item_for_permission_check(item, permissions, crud):
    if item.get("type", "") not in permissions[crud].keys():
        return item, None, None, None

    flat_item, object_lists = get_flat_item_and_object_lists(item)
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


def __is_allowed_to_crud_item(
    flat_item, restrictions_schema, user_context: UserContext
):
    restrictions = restrictions_schema.get("object_restrictions", {})

    for restricted_key, restricting_values in restrictions.items():
        restricted_key = restricted_key.split(":")[1]
        item_value_in_restricting_values = __item_value_in_values(
            flat_item, restricted_key, restricting_values, {}, user_context
        )
        if not item_value_in_restricting_values:
            return None

    return True


def __is_allowed_to_crud_item_keys(
    user_context: UserContext,
    item,
    flat_item,
    restrictions_schema,
    crud,
    object_lists,
    flat_request_body: dict = {},
    *,
    key_to_check=None,
):
    user_context.bag["restricted_keys"] = []
    restrictions = restrictions_schema.get("key_restrictions", {})

    for restricted_key, restricting_conditions in restrictions.items():
        restricted_key = restricted_key.split(":")[1]
        condition_match = True
        for condition_key, condition_values in restricting_conditions.items():
            condition_match = __item_value_in_values(
                flat_item,
                condition_key,
                condition_values,
                flat_request_body,
                user_context,
            )
            if not condition_match:
                break

        if condition_match:
            if crud == "read":
                keys_info = interpret_flat_key(restricted_key, object_lists)
                for info in keys_info:
                    if info["object_list"]:
                        element = __get_element_from_object_list_of_item(
                            item,
                            info["key"],
                            info["object_key"],
                            object_lists,
                        )
                        if element:
                            item[info["key"]].remove(element)
                        break
                else:
                    try:
                        del item[keys_info[0]["key"]][keys_info[1]["key"]]
                    except KeyError:
                        pass
                if key_to_check and key_to_check == restricted_key:
                    user_context.bag["restricted_keys"].append(restricted_key)
            else:
                if flat_request_body.get(restricted_key):
                    user_context.bag["restricted_keys"].append(restricted_key)

    user_context.bag["requested_item"] = item
    return len(user_context.bag["restricted_keys"]) == 0


def __item_value_in_values(
    flat_item, key, values: list, flat_request_body, user_context: UserContext
):
    negate_condition = False
    is_optional = False

    if key[0] == "!":
        key = key[1:]
        negate_condition = True
    if key[0] == "?":
        key = key[1:]
        is_optional = True

    key_of_relation = None
    if (keys := key.split("@", 1)) and len(keys) == 2:
        key = keys[0]
        key_of_relation = keys[1].split("-", 1)[1]
    try:
        item_value = flat_request_body.get(key, flat_item[key])
        if is_optional:
            negate_condition = False
    except KeyError:
        if not is_optional:
            raise Exception(
                f"{get_error_code(ErrorCode.METADATA_KEY_UNDEFINED, get_read())} | key:{key} | document:{flat_item.get('_id', flat_item["type"])} - Key {key} not found in document {flat_item.get('_id', flat_item["type"])}. Either prefix the key with '?' in your permission configuration to make it an optional restriction, or patch the document to include the key. '?' will allow access if key does not exist, '!?' will deny access if key does not exist."
            )
        return not negate_condition
    else:
        if key_of_relation:
            if isinstance(item_value, list):
                item_value = item_value[0]
            item = get_item(StorageManager(), user_context.bag, {"id": item_value})
            flat_item, _ = get_flat_item_and_object_lists(item)
            return __item_value_in_values(
                flat_item, key_of_relation, values, flat_request_body, user_context
            )

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

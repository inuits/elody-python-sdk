from copy import deepcopy
from elody.util import get_item_metadata_value
from inuits_policy_based_auth.contexts.user_context import UserContext


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


def handle_single_item_request(
    user_context: UserContext, item, permissions, crud, request_body={}
):
    is_allowed_to_crud_item = __is_allowed_to_crud_item(item, permissions, crud)
    if not is_allowed_to_crud_item:
        return is_allowed_to_crud_item

    return __is_allowed_to_crud_item_keys(
        user_context, item, permissions, crud, request_body
    )


def get_mask_protected_content_post_request_hook(
    user_context: UserContext, permissions, item_type=None
):
    def __post_request_hook(
        response, *, is_single_item_response=False, single_item_sub_route_key=""
    ):
        if is_single_item_response:
            if single_item_sub_route_key in ["metadata", "relations"]:
                items = [{single_item_sub_route_key: response[0], "type": item_type}]
            else:
                items = [response[0]]
        else:
            items = response[0]["results"]

        for item in items:
            __is_allowed_to_crud_item_keys(user_context, item, permissions, "read")

        return response

    return __post_request_hook


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


def __is_allowed_to_crud_item(item, permissions, crud):
    if item["type"] not in permissions[crud].keys():
        return None

    restrictions = permissions[crud][item["type"]].get("restrictions", {})

    for metadata in restrictions.get("metadata", []):
        value = get_item_metadata_value(item, metadata["key"])
        if isinstance(value, str):
            if value not in metadata["value"]:
                return None
        elif isinstance(value, list):
            for expected_value in metadata["value"]:
                if expected_value in value:
                    return True
            return None

    for relation in restrictions.get("relations", []):
        keys = _get_relation_keys(item, relation["key"])
        for expected_value in relation["value"]:
            if expected_value in keys:
                return True
        return None

    return True


def _get_relation_keys(item: dict, relation_type: str):
    return [
        relation["key"]
        for relation in item["relations"]
        if relation["type"] == relation_type
    ]


def __is_allowed_to_crud_item_keys(
    user_context: UserContext, item, permissions, crud, request_body={}
):
    user_context.bag["soft_call_response_body"] = []
    keys_permissions, negate_condition = __get_keys_permissions(
        permissions[crud][item["type"]]
    )

    if keys_permissions:
        initial_item = deepcopy(item)
        for key in item.keys():
            data_key = ""
            if key == "metadata":
                data_key = "key"
                (
                    permission_key_data_map,
                    data_value_key,
                ) = __determine_data_per_permission_key(item, key, data_key, "value")
            elif key == "relations":
                data_key = "type"
                (
                    permission_key_data_map,
                    data_value_key,
                ) = __determine_data_per_permission_key(item, key, data_key, "key")
            else:
                (
                    permission_key_data_map,
                    data_value_key,
                ) = __determine_data_per_permission_key(item, key)

            for permission_key, data in permission_key_data_map.items():
                if __is_not_valid_request_on_key(
                    initial_item,
                    keys_permissions,
                    negate_condition,
                    key,
                    permission_key,
                    data_value_key,
                ):
                    if crud == "read":
                        data[data_value_key] = "[protected content]"
                    else:
                        if key == data_value_key:
                            if request_body.get(key):
                                user_context.bag["soft_call_response_body"].append(key)
                        else:
                            for element in request_body.get(key, []):
                                if f"{key}.{element[data_key]}" == permission_key:
                                    user_context.bag["soft_call_response_body"].append(
                                        permission_key
                                    )

    return len(user_context.bag["soft_call_response_body"]) == 0


def __get_keys_permissions(item_permissions):
    if item_permissions.get("keys"):
        negate_condition = False
        keys_permissions = item_permissions["keys"].get("allowed_only", {})
        if not keys_permissions:
            negate_condition = True
            keys_permissions = item_permissions["keys"].get("disallowed_only", {})

        return keys_permissions, negate_condition

    return None, None


def __determine_data_per_permission_key(item, root_key, data_key="", data_value_key=""):
    key_data_map = {}

    if data_key and data_value_key:
        for data in item[root_key]:
            key_data_map.update({f"{root_key}.{data[data_key]}": data})
        value_key = data_value_key
    else:
        key_data_map.update({root_key: item})
        value_key = root_key

    return key_data_map, value_key


def __is_not_valid_request_on_key(
    initial_item,
    keys_permissions,
    negate_condition,
    root_key,
    key,
    data_value_key,
):
    if root_key == data_value_key:
        is_in_keys_permissions = lambda: key in keys_permissions.keys()
    else:
        is_in_keys_permissions = (
            lambda: key in keys_permissions.keys()
            or f"{root_key}.*" in keys_permissions.keys()
        )

    if is_in_keys_permissions():
        if __check_key_conditions_disallow_request(
            initial_item,
            keys_permissions.get(key, keys_permissions.get(f"{root_key}.*")),
            negate_condition,
        ):
            return True
    elif not negate_condition:
        return True

    return False


def __check_key_conditions_disallow_request(
    initial_item, key_conditions, negate_condition
):
    multiple_conditions = len(key_conditions) > 1
    return_value = False if negate_condition and len(key_conditions) == 0 else True
    switch_return_value = False

    for key_condition in key_conditions:
        metadata_key, expected_value = key_condition.split("==")
        actual_value = get_item_metadata_value(initial_item, metadata_key)
        if isinstance(actual_value, list):
            is_condition_met = lambda: expected_value in actual_value
        else:
            is_condition_met = (
                lambda: f"{expected_value}".lower() == f"{actual_value}".lower()
            )

        if is_condition_met():
            if negate_condition:
                if not multiple_conditions:
                    return return_value
                elif not switch_return_value:
                    return_value = not return_value
                    switch_return_value = True
        elif not negate_condition:
            return return_value
        elif switch_return_value:
            return False

    return not return_value

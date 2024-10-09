from abc import ABC, abstractmethod
from copy import deepcopy
from elody.migration.base_object_migrator import BaseObjectMigrator


class BaseObjectConfiguration(ABC):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    @abstractmethod
    def crud(self):
        return {
            "collection": "entities",
            "collection_history": "history",
            "creator": lambda post_body, **kwargs: post_body,
            "nested_matcher_builder": lambda object_lists, keys_info, value: self.__build_nested_matcher(
                object_lists, keys_info, value
            ),
            "post_crud_hook": lambda **kwargs: None,
            "pre_crud_hook": lambda **kwargs: None,
            "storage_type": "db",
        }

    @abstractmethod
    def document_info(self):
        return {"object_lists": {"metadata": "key", "relations": "type"}}

    @abstractmethod
    def logging(self, flat_document, **kwargs):
        info_labels = {
            "uuid": flat_document.get("_id"),
            "type": flat_document.get("type"),
            "schema": f"{flat_document.get('schema.type')}:{flat_document.get('schema.version')}",
        }
        try:
            user_context = kwargs.get("get_user_context")()  # pyright: ignore
            info_labels["http_method"] = user_context.bag.get("http_method")
            info_labels["requested_endpoint"] = user_context.bag.get(
                "requested_endpoint"
            )
            info_labels["full_path"] = user_context.bag.get("full_path")
            info_labels["preferred_username"] = user_context.preferred_username
            info_labels["email"] = user_context.email
            info_labels["user_roles"] = ", ".join(user_context.x_tenant.roles)
            info_labels["x_tenant"] = user_context.x_tenant.id
        except Exception:
            pass
        return {"info_labels": info_labels, "loki_indexed_info_labels": {}}

    @abstractmethod
    def migration(self):
        return BaseObjectMigrator(status="disabled")

    @abstractmethod
    def serialization(self, from_format, to_format):
        def serializer(document, **_):
            return document

        return serializer

    @abstractmethod
    def validation(self):
        def validator(http_method, content, **_):
            pass

        return "function", validator

    def _get_merged_post_body(self, post_body, document_defaults, object_list_name):
        key = self.document_info()["object_lists"][object_list_name]
        post_body[object_list_name] = self.__merge_object_lists(
            document_defaults.get(object_list_name, []),
            post_body.get(object_list_name, []),
            key,
        )
        return post_body

    def _sanitize_document(self, document, object_list_name, value_field_name):
        object_list = deepcopy(document[object_list_name])
        for element in object_list:
            if not element[value_field_name]:
                document[object_list_name].remove(element)

    def _sort_document_keys(self, document):
        def sort_keys(data):
            if isinstance(data, dict):
                sorted_items = {key: data.pop(key) for key in sorted(data.keys())}
                for key, value in sorted_items.items():
                    data[key] = sort_keys(value)
                return data
            elif isinstance(data, list):
                if all(isinstance(i, str) for i in data):
                    data.sort()
                    return data
                else:
                    for index, item in enumerate(data):
                        data[index] = sort_keys(item)
                    return data
            else:
                return data

        for key, value in self.document_info()["object_lists"].items():
            document[key] = sorted(document[key], key=lambda property: property[value])
        sort_keys(document)

    def __build_nested_matcher(self, object_lists, keys_info, value, index=0):
        if index == 0 and not any(info["is_object_list"] for info in keys_info):
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                value = {"$exists": value == "ANY_MATCH"}
            return {".".join(info["key"] for info in keys_info): value}

        info = keys_info[index]

        if info["is_object_list"]:
            nested_matcher = self.__build_nested_matcher(
                object_lists, keys_info, value, index + 1
            )
            elem_match = {
                "$elemMatch": {
                    object_lists[info["key"]]: info["object_key"],
                    keys_info[index + 1]["key"]: nested_matcher,
                }
            }
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                del elem_match["$elemMatch"][keys_info[index + 1]["key"]]
                if value == "NONE_MATCH":
                    return {"NOR_MATCHER": {info["key"]: {"$all": [elem_match]}}}
            return elem_match if index > 0 else {info["key"]: {"$all": [elem_match]}}

        return value

    def __merge_object_lists(self, source, target, key):
        for target_item in target:
            for source_item in source:
                if source_item[key] == target_item[key]:
                    source.remove(source_item)
        return [*source, *target]

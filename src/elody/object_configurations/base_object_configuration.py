from abc import ABC, abstractmethod
from copy import deepcopy
from elody.migration.base_object_migrator import BaseObjectMigrator


class BaseObjectConfiguration(ABC):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    @abstractmethod
    def crud(self):
        return {
            "creator": lambda post_body, **kwargs: post_body,
            "document_content_patcher": lambda *, document, content, overwrite=False, **kwargs: self._document_content_patcher(
                document=document,
                content=content,
                overwrite=overwrite,
                **kwargs,
            ),
            "nested_matcher_builder": lambda object_lists, keys_info, value, **kwargs: self.__build_nested_matcher(
                object_lists, keys_info, value, **kwargs
            ),
            "post_crud_hook": lambda **kwargs: None,
            "pre_crud_hook": lambda *, document, **kwargs: document,
            "storage_type": "db",
        }

    @abstractmethod
    def document_info(self):
        return {}

    @abstractmethod
    def logging(self, flat_document, **kwargs):
        info_labels = {
            "uuid": flat_document.get("_id"),
            "type": flat_document.get("type"),
            "schema": f"{flat_document.get('schema.type')}:{flat_document.get('schema.version')}",
        }
        try:
            from policy_factory import get_user_context  # pyright: ignore

            user_context = get_user_context()
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
        def validator(http_method, content, item, **_):
            pass

        return "function", validator

    def _document_content_patcher(
        self, *, document, content, overwrite=False, **kwargs
    ):
        raise NotImplementedError(
            "Provide concrete implementation in child object configuration"
        )

    def _merge_object_lists(self, source, target, object_list_key):
        for target_item in target:
            for source_item in source:
                if source_item[object_list_key] == target_item[object_list_key]:
                    source.remove(source_item)
        return [*source, *target]

    def _get_user_context_id(self):
        try:
            from policy_factory import get_user_context  # pyright: ignore

            return get_user_context().id
        except Exception:
            return None

    def _sanitize_document(self, *, document, **kwargs):
        sanitized_document = {}
        document_deepcopy = deepcopy(document)
        for key, value in document_deepcopy.items():
            if isinstance(value, dict):
                sanitized_value = BaseObjectConfiguration._sanitize_document(
                    self, document=value
                )
                if sanitized_value:
                    sanitized_document[key] = sanitized_value
            elif isinstance(value, list):
                sanitized_document[key] = [
                    element.strip() if isinstance(element, str) else element
                    for element in value
                    if element
                ]
                if all(isinstance(element, str) for element in sanitized_document[key]):
                    sanitized_document[key] = list(set(sanitized_document[key]))
            elif isinstance(value, str):
                lines = value.splitlines()
                value = "\n".join(line.strip() for line in lines).strip()
                sanitized_document[key] = value.strip()
            elif value:
                sanitized_document[key] = value
        return sanitized_document

    def _should_create_history_object(self):
        try:
            from policy_factory import get_user_context  # pyright: ignore

            get_user_context()
            return bool(self.crud().get("collection_history"))
        except Exception:
            return False

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

        for key, value in self.document_info().get("object_lists", {}).items():
            if document.get(key):
                document[key] = sorted(
                    document[key], key=lambda property: property[value]
                )
        sort_keys(document)
        return document

    def __build_nested_matcher(
        self, object_lists, keys_info, value, *, index=0, **kwargs
    ):
        if index == 0 and not any(info["object_list"] for info in keys_info):
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                value = {"$exists": value == "ANY_MATCH"}
            matcher = {".".join(info["key"] for info in keys_info): value}
            if inner_exact_matches := kwargs.get("inner_exact_matches"):
                matcher.update(inner_exact_matches)
            return matcher

        info = keys_info[index]

        if info["object_list"]:
            elem_match = {
                "$elemMatch": {
                    object_lists[info["object_list"]]: info["object_key"],
                    **self.__build_nested_matcher(
                        object_lists, keys_info[index + 1 :], value, index=0, **kwargs
                    ),
                }
            }
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                elem_match_with_exists_operator = deepcopy(elem_match)
                del elem_match["$elemMatch"][keys_info[index + 1]["key"]]
                if value == "NONE_MATCH":
                    return {
                        "NOR_MATCHER": [
                            {info["key"]: {"$all": [elem_match]}},
                            {info["key"]: {"$all": [elem_match_with_exists_operator]}},
                        ]
                    }
            return elem_match if index > 0 else {info["key"]: {"$all": [elem_match]}}

        raise Exception(f"Unable to build nested matcher. See keys_info: {keys_info}")

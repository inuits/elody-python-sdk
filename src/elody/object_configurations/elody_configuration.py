from datetime import datetime, timezone
from elody.object_configurations.base_object_configuration import (
    BaseObjectConfiguration,
)
from elody.util import flatten_dict
from uuid import uuid4


class ElodyConfiguration(BaseObjectConfiguration):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {
            "collection": "entities",
            "collection_history": "history",
            "creator": lambda post_body, **kwargs: self._creator(post_body, **kwargs),
            "post_crud_hook": lambda **kwargs: self._post_crud_hook(**kwargs),
            "pre_crud_hook": lambda **kwargs: self._pre_crud_hook(**kwargs),
            "sorting": lambda key_order_map, **kwargs: self._sorting(
                key_order_map, **kwargs
            ),
        }
        return {**super().crud(), **crud}

    def document_info(self):
        return {"object_lists": {"metadata": "key", "relations": "type"}}

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return super().serialization(from_format, to_format)

    def validation(self):
        return super().validation()

    def _creator(
        self,
        post_body,
        *,
        flat_post_body={},
        document_defaults={},
    ):
        flat_post_body = flat_post_body or flatten_dict(
            self.document_info()["object_lists"],
            post_body if isinstance(post_body, dict) else {},
        )
        _id = document_defaults.get("_id", str(uuid4()))
        timestamp = datetime.now(timezone.utc)

        identifiers = (
            post_body.pop("identifiers", []) if isinstance(post_body, dict) else []
        )
        for property in self.document_info().get("identifier_properties", []):
            if identifier := flat_post_body.get(f"metadata.{property}.value"):
                identifiers.append(identifier)

        template = {
            "_id": _id,
            "identifiers": list(
                set(
                    [
                        _id,
                        *identifiers,
                        *document_defaults.pop("identifiers", []),
                    ]
                )
            ),
            "metadata": [],
            "relations": [],
            "schema": {"type": self.SCHEMA_TYPE, "version": self.SCHEMA_VERSION},
        }

        if isinstance(post_body, dict):
            for key, object_list_key in self.document_info()["object_lists"].items():
                if not key.startswith("lookup.virtual_relations"):
                    post_body[key] = self._merge_object_lists(
                        document_defaults.get(key, []),
                        post_body.get(key, []),
                        object_list_key,
                    )
            document = {**template, **document_defaults, **post_body}
        else:
            document = {**template, **document_defaults}
        document = self._pre_crud_hook(
            crud="create", timestamp=timestamp, document=document
        )
        return document

    def _document_content_patcher(self, *, document, content, overwrite=False, **_):
        object_lists = self.document_info().get("object_lists", {})
        if overwrite:
            document = content
        else:
            for key, value in content.items():
                if key in object_lists:
                    if key != "relations":
                        for value_element in value:
                            for item_element in document[key]:
                                if (
                                    item_element[object_lists[key]]
                                    == value_element[object_lists[key]]
                                ):
                                    document[key].remove(item_element)
                                    break
                    else:
                        for value_element in value:
                            for item_element in document[key]:
                                if (
                                    item_element[object_lists[key]]
                                    == value_element[object_lists[key]]
                                    and item_element["key"] == value_element["key"]
                                ):
                                    document[key].remove(item_element)
                                    break
                    if not document.get(key):
                        document[key] = []
                    document[key].extend(value)
                else:
                    document[key] = value

        return document

    def _post_crud_hook(self, **kwargs):
        pass

    def _pre_crud_hook(self, *, crud, timestamp, document={}, **kwargs):
        if document:
            document = self.__patch_document_unique_value(document)
            document = self.__patch_document_audit_info(crud, document, timestamp)
            document = self._sanitize_document(
                document=document,
                object_list_name="metadata",
                object_list_value_field_name="value",
            )
            document = self._sanitize_document(
                document=document,
                object_list_name="relations",
                object_list_value_field_name="key",
            )
            document = self._sort_document_keys(document)
        return document

    def _sanitize_document(
        self, *, document, object_list_name, object_list_value_field_name, **kwargs
    ):
        sanitized_document = super()._sanitize_document(document=document, **kwargs)
        for element in document[object_list_name]:
            if not element.get(object_list_value_field_name):
                sanitized_document[object_list_name].remove(element)
        for element in sanitized_document[object_list_name]:
            value = element[object_list_value_field_name]
            if isinstance(value, str):
                lines = value.splitlines()
                value = "\n".join(line.strip() for line in lines).strip()
                element[object_list_value_field_name] = value.strip()
        return sanitized_document

    def __patch_document_audit_info(self, crud, document, timestamp):
        document.update({f"date_{crud}d": timestamp})
        if email := self._get_user_context_id():
            label = f"{crud}d_by" if crud == "create" else "last_editor"
            document.update({label: email})
        return document

    def __patch_document_unique_value(self, document):
        if unique_field := self.document_info().get("unique_field"):
            flat_document = flatten_dict(
                self.document_info().get("object_lists", {}), document
            )
            if value := flat_document.get(unique_field):
                document["unique_field"] = f"{document['type']}:{value}"
        return document

    def _sorting(self, key_order_map, **_):
        addFields, sort = {}, {}
        for key, order in key_order_map.items():
            if key == "order":
                addFields.update(
                    {
                        key: {
                            "$arrayElemAt": [
                                {
                                    "$map": {
                                        "input": {
                                            "$filter": {
                                                "input": "$relations",
                                                "as": "relation",
                                                "cond": {
                                                    "$eq": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$$relation.metadata.key",
                                                                0,
                                                            ]
                                                        },
                                                        key,
                                                    ]
                                                },
                                            }
                                        },
                                        "as": "relation",
                                        "in": {
                                            "$arrayElemAt": [
                                                "$$relation.metadata.value",
                                                0,
                                            ]
                                        },
                                    }
                                },
                                0,
                            ]
                        }
                    }
                )
            elif key not in ["date_created", "date_updated", "last_editor"]:
                addFields.update(
                    {
                        key: {
                            "$arrayElemAt": [
                                {
                                    "$map": {
                                        "input": {
                                            "$filter": {
                                                "input": "$metadata",
                                                "as": "metadata",
                                                "cond": {
                                                    "$eq": ["$$metadata.key", key]
                                                },
                                            }
                                        },
                                        "as": "metadata",
                                        "in": {"$toLower": "$$metadata.value"},
                                    }
                                },
                                0,
                            ]
                        }
                    }
                )
            sort.update({key: order})
        pipeline = []
        if addFields:
            pipeline.append({"$addFields": addFields})
        pipeline.append({"$sort": sort})
        return pipeline

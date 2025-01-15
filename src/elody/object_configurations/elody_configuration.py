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
        if not flat_post_body:
            flat_post_body = flatten_dict(
                self.document_info()["object_lists"], post_body
            )
        _id = document_defaults.get("_id", str(uuid4()))

        identifiers = []
        for property in self.document_info().get("identifier_properties", []):
            if identifier := flat_post_body.get(f"metadata.{property}.value"):
                identifiers.append(identifier)

        template = {
            "_id": _id,
            "computed_values": {
                "created_at": datetime.now(timezone.utc),
                "event": "create",
            },
            "identifiers": list(
                set([_id, *identifiers, *document_defaults.pop("identifiers", [])])
            ),
            "metadata": [],
            "relations": [],
            "schema": {"type": self.SCHEMA_TYPE, "version": self.SCHEMA_VERSION},
        }
        if user_context_id := self._get_user_context_id():
            template["computed_values"]["created_by"] = user_context_id

        for key, object_list_key in self.document_info()["object_lists"].items():
            if not key.startswith("lookup.virtual_relations"):
                post_body[key] = self._merge_object_lists(
                    document_defaults.get(key, []),
                    post_body.get(key, []),
                    object_list_key,
                )
        document = {**template, **document_defaults, **post_body}

        document = self._sanitize_document(
            document=document,
            object_list_name="metadata",
            object_list_value_field_name="value",
        )
        document = self._sort_document_keys(document)
        return document

    def _document_content_patcher(
        self, *, document, content, overwrite=False, **kwargs
    ):
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
                    if not document.get(key):
                        document[key] = []
                    document[key].extend(value)
                else:
                    document[key] = value

        return document

    def _post_crud_hook(self, **kwargs):
        pass

    def _pre_crud_hook(self, *, crud, document={}, **kwargs):
        if document:
            document = self._sanitize_document(
                document=document,
                object_list_name="metadata",
                object_list_value_field_name="value",
            )
            document = self.__patch_document_computed_values(crud, document)
            document = self._sort_document_keys(document)
        return document

    def _sanitize_document(
        self, *, document, object_list_name, object_list_value_field_name, **kwargs
    ):
        sanitized_document = super()._sanitize_document(document=document)
        object_list = document[object_list_name]
        for element in object_list:
            if not element[object_list_value_field_name]:
                sanitized_document[object_list_name].remove(element)
        return sanitized_document

    def __patch_document_computed_values(self, crud, document):
        if not document.get("computed_values"):
            document["computed_values"] = {}
        document["computed_values"].update({"event": crud})
        document["computed_values"].update({"modified_at": datetime.now(timezone.utc)})
        if email := self._get_user_context_id():
            document["computed_values"].update({"modified_by": email})
        return document

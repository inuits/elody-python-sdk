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
        get_user_context,
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
        if email := self.__get_email(get_user_context):
            template["computed_values"]["created_by"] = email

        for key in self.document_info()["object_lists"].keys():
            post_body = self._get_merged_post_body(post_body, document_defaults, key)
        document = {**template, **document_defaults, **post_body}

        self._sanitize_document(document, "metadata", "value")
        self._sort_document_keys(document)
        return document

    def _post_crud_hook(self, **_):
        pass

    def _pre_crud_hook(self, *, crud, document={}, get_user_context=None, **_):
        if document:
            self._sanitize_document(document, "metadata", "value")
            self.__patch_document_computed_values(
                crud, document, get_user_context=get_user_context
            )
            self._sort_document_keys(document)

    def __get_email(self, get_user_context):
        try:
            return get_user_context().email
        except Exception:
            return None

    def __patch_document_computed_values(self, crud, document, **kwargs):
        document["computed_values"].update({"event": crud})
        document["computed_values"].update({"modified_at": datetime.now(timezone.utc)})
        if email := self.__get_email(kwargs.get("get_user_context")):
            document["computed_values"].update({"modified_by": email})

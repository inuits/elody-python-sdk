from elody.object_configurations.elody_configuration import (
    ElodyConfiguration,
)
from elody.util import send_cloudevent
from os import getenv


class JobConfiguration(ElodyConfiguration):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {"collection": "jobs", "collection_history": ""}
        return {**super().crud(), **crud}

    def document_info(self):
        return super().document_info()

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return super().serialization(from_format, to_format)

    def validation(self):
        return super().validation()

    def _creator(self, post_body, *, get_user_context={}, **_):
        document = super()._creator(post_body)
        if email := get_user_context().email:
            document["last_editor"] = email
        return document

    def _post_crud_hook(self, *, crud, document, get_rabbit, **kwargs):
        if crud == "create":
            send_cloudevent(
                get_rabbit(),
                getenv("MQ_EXCHANGE", "dams"),
                "dams.job_created",
                document,
            )
            if parent_id := kwargs.get("parent_id"):
                send_cloudevent(
                    get_rabbit(),
                    getenv("MQ_EXCHANGE", "dams"),
                    "dams.job_changed",
                    {
                        "id": parent_id,
                        "patch": {
                            "relations": [
                                {"key": document["_id"], "type": "isParentJobOf"}
                            ]
                        },
                    },
                )
        elif crud == "update":
            send_cloudevent(
                get_rabbit(),
                getenv("MQ_EXCHANGE", "dams"),
                "dams.job_changed",
                document,
            )

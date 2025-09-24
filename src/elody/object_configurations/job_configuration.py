from datetime import datetime, timezone
from enum import Enum
from os import getenv
from typing import Literal

from elody.object_configurations.elody_configuration import (
    ElodyConfiguration,
)
from elody.util import send_cloudevent


class Status(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    WARNING = "warning"


class JobConfiguration(ElodyConfiguration):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {
            "collection": "jobs",
            "collection_history": "",
            "add_document_to_job": lambda *args, **kwargs: self._add_document_to_job(
                *args, **kwargs
            ),
            "init_job": lambda *args, **kwargs: self._init_job(*args, **kwargs),
            "start_job": lambda *args, **kwargs: self._start_job(*args, **kwargs),
            "finish_job": lambda *args, **kwargs: self._finish_job(*args, **kwargs),
            "finish_job_with_warning": lambda *args, **kwargs: self._finish_job_with_warning(
                *args, **kwargs
            ),
            "fail_job": lambda *args, **kwargs: self._fail_job(*args, **kwargs),
            "handle_parent_job_finished": lambda *args, **kwargs: self._handle_parent_job_finished(
                *args, **kwargs
            ),
        }
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
        elif crud == "update":
            send_cloudevent(
                get_rabbit(),
                getenv("MQ_EXCHANGE", "dams"),
                "dams.job_changed",
                document,
            )

    def _add_document_to_job(
        self,
        id,
        id_of_document_job_was_initiated_for,
        *,
        get_rabbit,
    ):
        relations = []
        if id_of_document_job_was_initiated_for:
            relations.append(
                {"key": id_of_document_job_was_initiated_for, "type": "isJobFor"}
            )
        document = {
            "id": id,
            "patch": {
                "relations": (relations),
            },
        }
        self._post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

    def _init_job(
        self,
        name,
        job_type,
        *,
        get_rabbit,
        get_user_context=None,
        user_email=None,
        parent_id=None,
        id_of_document_job_was_initiated_for=None,
        track_async_children: bool | None = None,
    ) -> str:
        relations = []
        if parent_id:
            relations.append({"key": parent_id, "type": "hasParentJob"})
        if id_of_document_job_was_initiated_for:
            relations.append(
                {"key": id_of_document_job_was_initiated_for, "type": "isJobFor"}
            )

        metadata = [
            {"key": "name", "value": name},
            {"key": "status", "value": Status.QUEUED.value},
            {"key": "type", "value": job_type},
        ]

        if track_async_children:
            metadata.append(
                {
                    "key": "child_jobs",
                    "value": {
                        Status.QUEUED.value: 0,
                        Status.RUNNING.value: 0,
                        Status.FAILED.value: 0,
                        Status.FINISHED.value: 0,
                        Status.WARNING.value: 0,
                    },
                }
            )

        job = self.crud()["creator"](
            {
                "metadata": metadata,
                "relations": relations,
                "type": "job",
            },
            get_user_context=get_user_context
            or (lambda: type("UserContext", (object,), {"email": user_email})()),
        )

        self._post_crud_hook(
            crud="create", document=job, parent_id=parent_id, get_rabbit=get_rabbit
        )
        return job["_id"]

    def _start_job(
        self,
        id,
        *,
        get_rabbit,
    ):
        document = {
            "id": id,
            "patch": {
                "started_at": datetime.now(timezone.utc),
                "metadata": [{"key": "status", "value": Status.RUNNING.value}],
            },
        }
        self._post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

    def _finish_job(
        self,
        id,
        *,
        get_rabbit,
    ):
        document = {
            "id": id,
            "patch": {
                "metadata": [{"key": "status", "value": Status.FINISHED.value}],
            },
        }
        self._post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

    def _finish_job_with_warning(
        self,
        id,
        *,
        get_rabbit,
        info_message=None,
    ):
        document = {
            "id": id,
            "patch": {
                "metadata": [
                    {"key": "status", "value": Status.FINISHED.value},
                    {"key": "info", "value": info_message},
                ],
            },
        }
        self._post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

    def _fail_job(self, id, exception_message, *, get_rabbit):

        status, message = self.__handle_error_warnings(exception_message)

        document = {
            "id": id,
            "patch": {
                "metadata": [
                    {"key": "info", "value": message},
                    {
                        "key": "status",
                        "value": (status.value),
                    },
                ]
            },
        }
        self._post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

    def _handle_parent_job_finished(self, id, parent_child_status, *, get_rabbit):

        child_jobs_failed = parent_child_status[Status.FAILED.value]
        child_jobs_warning = parent_child_status[Status.WARNING.value]

        if child_jobs_failed == 0:
            if child_jobs_warning:
                self.crud()["finish_job_with_warning"](
                    id, get_rabbit=get_rabbit, info_message="Some subjobs have warnings"
                )
            self.crud()["finish_job"](id, get_rabbit=get_rabbit)
        else:
            self.crud()["fail_job"](
                id, exception_message="some subjobs failed", get_rabbit=get_rabbit
            )

    def __handle_error_warnings(
        self,
        errormessage: str,
    ) -> tuple[Literal[Status.WARNING] | Literal[Status.FAILED], str]:

        if errormessage.startswith("W4009"):
            return Status.WARNING, errormessage

        return Status.FAILED, errormessage

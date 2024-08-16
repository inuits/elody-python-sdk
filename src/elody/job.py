from elody.object_configurations.job_configuration import JobConfiguration


_config = JobConfiguration()
_create = _config.crud()["creator"]
_post_crud_hook = _config.crud()["post_crud_hook"]


def start_job(name, type, *, get_rabbit, parent_id=None, get_user_context=None) -> str:
    job = _create(
        {
            "metadata": [
                {"key": "name", "value": name},
                {"key": "status", "value": "running"},
                {"key": "type", "value": type},
            ],
            "relations": (
                [{"key": parent_id, "type": "hasParentJob"}] if parent_id else []
            ),
            "type": "job",
        },
        get_user_context=get_user_context,
    )
    _post_crud_hook(
        crud="create", document=job, parent_id=parent_id, get_rabbit=get_rabbit
    )
    return job["_id"]


def finish_job(
    id,
    id_of_document_job_was_initiated_for=None,
    type_of_document_job_was_initiated_for=None,
    *,
    get_rabbit
):
    document = {
        "id": id,
        "patch": {
            "metadata": [{"key": "status", "value": "finished"}],
            "relations": (
                [{"key": id_of_document_job_was_initiated_for, "type": "isJobOf"}]
                if id_of_document_job_was_initiated_for
                else []
            ),
        },
    }
    _post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

    if id_of_document_job_was_initiated_for and type_of_document_job_was_initiated_for:
        document = {
            "document_info_job_was_initiated_for": {
                "id": id_of_document_job_was_initiated_for,
                "type": type_of_document_job_was_initiated_for,
            },
            "patch": {"relations": [{"key": id, "type": "hasJob"}]},
        }
        _post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)


def fail_job(id, exception_message, *, get_rabbit):
    document = {
        "id": id,
        "patch": {
            "metadata": [
                {"key": "info", "value": exception_message},
                {"key": "status", "value": "failed"},
            ]
        },
    }
    _post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

from elody.object_configurations.job_configuration import JobConfiguration


_config = JobConfiguration()
_create = _config.crud()["creator"]
_post_crud_hook = _config.crud()["post_crud_hook"]


def start_job(
    name,
    job_type,
    *,
    get_rabbit,
    get_user_context=None,
    user_email=None,
    parent_id=None,
    id_of_document_job_was_initiated_for=None,
    type_of_document_job_was_initiated_for=None,
) -> str:
    relations = []
    if parent_id:
        relations.append({"key": parent_id, "type": "hasParentJob"})
    if id_of_document_job_was_initiated_for and type_of_document_job_was_initiated_for:
        relations.append(
            {"key": id_of_document_job_was_initiated_for, "type": "isJobOf"}
        )

    job = _create(
        {
            "metadata": [
                {"key": "name", "value": name},
                {"key": "status", "value": "running"},
                {"key": "type", "value": job_type},
            ],
            "relations": relations,
            "type": "job",
        },
        get_user_context=get_user_context
        or (lambda: type("UserContext", (object,), {"email": user_email})()),
    )
    del job["computed_values"]["created_at"]

    _post_crud_hook(
        crud="create", document=job, parent_id=parent_id, get_rabbit=get_rabbit
    )
    __patch_document_job_was_initiated_for(
        id_of_document_job_was_initiated_for,
        type_of_document_job_was_initiated_for,
        get_rabbit,
    )
    return job["_id"]


def finish_job(
    id,
    id_of_document_job_was_initiated_for=None,
    type_of_document_job_was_initiated_for=None,
    *,
    get_rabbit,
):
    document = {
        "id": id,
        "patch": {
            "metadata": [{"key": "status", "value": "finished"}],
            "relations": ([] if id_of_document_job_was_initiated_for else []),
        },
    }
    _post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)
    __patch_document_job_was_initiated_for(
        id_of_document_job_was_initiated_for,
        type_of_document_job_was_initiated_for,
        get_rabbit,
    )


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


def __patch_document_job_was_initiated_for(id, type, get_rabbit):
    if id and type:
        document = {
            "document_info_job_was_initiated_for": {"id": id, "type": type},
            "patch": {"relations": [{"key": id, "type": "hasJob"}]},
        }
        _post_crud_hook(crud="update", document=document, get_rabbit=get_rabbit)

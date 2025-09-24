try:
    from config import get_object_configuration_mapper

    _config = get_object_configuration_mapper().get("job")
except ModuleNotFoundError:
    from elody.object_configurations.job_configuration import JobConfiguration

    _config = JobConfiguration()


def add_document_to_job(
    id,
    id_of_document_job_was_initiated_for,
    *,
    get_rabbit,
):
    _config.crud()["add_document_to_job"](
        id=id,
        id_of_document_job_was_initiated_for=id_of_document_job_was_initiated_for,
        get_rabbit=get_rabbit,
    )


def init_job(
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
    return _config.crud()["init_job"](
        name=name,
        job_type=job_type,
        get_rabbit=get_rabbit,
        get_user_context=get_user_context,
        user_email=user_email,
        parent_id=parent_id,
        id_of_document_job_was_initiated_for=id_of_document_job_was_initiated_for,
        track_async_children=track_async_children,
    )


def start_job(
    id,
    *,
    get_rabbit,
):
    _config.crud()["start_job"](
        id=id,
        get_rabbit=get_rabbit,
    )


def finish_job(
    id,
    *,
    get_rabbit,
):
    _config.crud()["finish_job"](
        id=id,
        get_rabbit=get_rabbit,
    )


def soft_finish_job(
    id,
    *,
    get_rabbit,
):
    _config.crud()["soft_finish_job"](id=id, get_rabbit=get_rabbit)


def fail_job(id, exception_message, *, get_rabbit):
    _config.crud()["fail_job"](
        id=id, exception_message=exception_message, get_rabbit=get_rabbit
    )


def handle_parent_job_finished(id, parent_child_status, *, get_rabbit):
    _config.crud()["handle_parent_job_finished"](
        id=id, parent_child_status=parent_child_status, get_rabbit=get_rabbit
    )

from elody.error_codes import ErrorCode, get_error_code, get_read
from configuration import get_object_configuration_mapper  # pyright: ignore
from flask_restful import abort  # pyright: ignore
from serialization.serialize import serialize  # pyright: ignore


def get_content(item, request, content):
    return serialize(
        content,
        type=item.get("type"),
        from_format=serialize.get_format(
            (request.view_args or {}).get("spec", "elody"), request.args
        ),
        to_format=get_object_configuration_mapper().get(item["type"]).SCHEMA_TYPE,
    )


def get_item(storage_manager, user_context_bag, view_args):
    view_args = view_args or {}
    id = view_args.get("id")
    resolve_collections = user_context_bag.get("collection_resolver")
    if not resolve_collections:
        abort(
            403,
            message=f"{get_error_code(ErrorCode.UNDEFINED_COLLECTION_RESOLVER, get_read())} - Collection resolver not defined for user.",
        )
    collections = resolve_collections(collection=view_args.get("collection"), id=id)
    for collection in collections:
        if item := storage_manager.get_db_engine().get_item_from_collection_by_id(
            collection, id
        ):
            return item
    else:
        abort(
            404,
            message=f"{get_error_code(ErrorCode.ITEM_NOT_FOUND, get_read())} | id:{id} - Item with id {id} does not exist.",
        )

from configuration import get_object_configuration_mapper  # pyright: ignore
from elody.error_codes import ErrorCode, get_error_code, get_read
from elody.util import flatten_dict
from serialization.serialize import serialize  # pyright: ignore
from werkzeug.exceptions import NotFound


def generate_filter_key_and_lookup_from_restricted_key(key):
    if (keys := key.split("@", 1)) and len(keys) == 1:
        return key, {}

    local_field = keys[0]
    document_type, key = keys[1].split("-", 1)
    collection = (
        get_object_configuration_mapper().get(document_type).crud()["collection"]
    )
    lookup = {
        "from": collection,
        "local_field": local_field,
        "foreign_field": "identifiers",
        "as": f"__lookup.virtual_relations.{document_type}",
    }
    return f"{lookup['as']}.{key}", lookup


def get_content(item, request, content):
    return serialize(
        content,
        type=item.get("type"),
        from_format=serialize.get_format(
            (request.view_args or {}).get("spec", "elody"), request.args
        ),
        to_format=get_object_configuration_mapper().get(item["type"]).SCHEMA_TYPE,
    )


def get_flat_item_and_object_lists(item):
    config = get_object_configuration_mapper().get(item["type"])
    object_lists = config.document_info().get("object_lists", {})
    return flatten_dict(object_lists, item), object_lists


def get_item(storage_manager, user_context_bag, view_args) -> dict:
    view_args = view_args or {}
    if id := view_args.get("id"):
        resolve_collections = user_context_bag.get("collection_resolver")
        collections = resolve_collections(collection=view_args.get("collection"), id=id)
        for collection in collections:
            if item := storage_manager.get_db_engine().get_item_from_collection_by_id(
                collection, id
            ):
                return item

    raise NotFound(
        f"{get_error_code(ErrorCode.ITEM_NOT_FOUND, get_read())} | id:{id} - Item with id {id} does not exist."
    )

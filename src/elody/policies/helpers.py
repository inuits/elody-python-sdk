from flask_restful import abort  # pyright: ignore


def get_item(storage_manager, user_context_bag, view_args):
    view_args = view_args or {}
    id = view_args.get("id")
    resolve_collections = user_context_bag["collection_resolver"]
    collections = resolve_collections(collection=view_args.get("collection"), id=id)
    for collection in collections:
        if item := storage_manager.get_db_engine().get_item_from_collection_by_id(
            collection, id
        ):
            return item
    else:
        abort(404, message=f"Item with id {id} does not exist.")

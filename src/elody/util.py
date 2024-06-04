import json
import mimetypes

from cloudevents.conversion import to_dict
from cloudevents.http import CloudEvent
from collections.abc import MutableMapping, Iterable
from datetime import datetime, timezone


class CustomJSONEncoder(json.JSONEncoder):
    def __convert_datetime(self, obj):
        if obj.tzinfo is None:
            obj = obj.astimezone(timezone.utc)
        return obj.isoformat()

    def default(self, obj):
        if not isinstance(obj, datetime):
            return super().default(obj)
        return self.__convert_datetime(obj)

    def encode(self, obj):
        if not isinstance(obj, datetime):
            return super().encode(obj)
        return self.__convert_datetime(obj)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def custom_json_dumps(obj):
    return json.dumps(obj, cls=CustomJSONEncoder)


def flatten_dict(object_lists, data: MutableMapping, parent_key=""):
    flat_dict = {}
    for key, value in __flatten_dict_generator(object_lists, data, parent_key):
        if key in flat_dict:
            if not isinstance(flat_dict[key], list):
                flat_dict[key] = [flat_dict[key]]
            flat_dict[key].append(value)
        else:
            flat_dict[key] = value
    return flat_dict


def __flatten_dict_generator(object_lists, data: MutableMapping, parent_key):
    for key, value in data.items():
        flattened_key = f"{parent_key}.{key}" if parent_key else key
        if isinstance(value, MutableMapping):
            yield from flatten_dict(object_lists, value, flattened_key).items()
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            if any(isinstance(item, MutableMapping) for item in value):
                for item in value:
                    item_key = item.get(object_lists.get(key))
                    if item_key:
                        if isinstance(item, MutableMapping):
                            yield from flatten_dict(
                                object_lists, item, f"{flattened_key}.{item_key}"
                            ).items()
                        else:
                            yield f"{flattened_key}.{item_key}", item["value"]
            else:
                yield flattened_key, value
        else:
            yield flattened_key, value


def get_raw_id(item):
    return item.get("_key", item["_id"])


def get_item_metadata_value(item, key):
    for item in item.get("metadata", []):
        if item["key"] == key:
            return item["value"]
    return ""


def get_mimetype_from_filename(filename):
    mime = mimetypes.guess_type(filename, False)[0]
    return mime if mime else "application/octet-stream"


def interpret_flat_key(flat_key: str, object_lists):
    keys_info = []
    index = 0

    flat_key_parts = flat_key.split(".")
    while index < len(flat_key_parts):
        info = {
            "key": flat_key_parts[index],
            "is_object_list": flat_key_parts[index] in object_lists.keys(),
        }

        if info["is_object_list"]:
            combined_key = "".join(
                [f"{info['key']}." for info in keys_info if not info["is_object_list"]]
            )
            info["key"] = f"{combined_key}{info['key']}"
            info["object_key"] = flat_key_parts[index + 1]
            keys_info = [info for info in keys_info if info["is_object_list"]]

        keys_info.append(info)
        index += 2 if info["is_object_list"] else 1

    return keys_info


def mediafile_is_public(mediafile):
    publication_status = get_item_metadata_value(mediafile, "publication_status")
    copyright_color = get_item_metadata_value(mediafile, "copyright_color")
    return publication_status.lower() in [
        "beschermd",
        "expliciet",
        "publiek",
    ] or copyright_color.lower() in ["green", "groen"]


def read_json_as_dict(filename, logger):
    try:
        with open(filename) as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as ex:
        logger.error(f"Could not read {filename} as a dict: {ex}")
    return {}


def send_cloudevent(mq_client, source, routing_key, data):
    event = to_dict(CloudEvent({"source": source, "type": routing_key}, data))
    mq_client.send(event, routing_key=routing_key)


def signal_child_relation_changed(mq_client, collection, id):
    data = {"parent_id": id, "collection": collection}
    send_cloudevent(mq_client, "dams", "dams.child_relation_changed", data)


def signal_edge_changed(mq_client, parent_ids_from_changed_edges):
    data = {
        "location": f'/entities?ids={",".join(parent_ids_from_changed_edges)}&skip_relations=1'
    }
    send_cloudevent(mq_client, "dams", "dams.edge_changed", data)


def signal_entity_changed(mq_client, entity):
    data = {
        "location": f"/entities/{get_raw_id(entity)}",
        "type": entity.get("type", "unspecified"),
    }
    send_cloudevent(mq_client, "dams", "dams.entity_changed", data)


def signal_entity_deleted(mq_client, entity):
    data = {"_id": get_raw_id(entity), "type": entity.get("type", "unspecified")}
    send_cloudevent(mq_client, "dams", "dams.entity_deleted", data)


def signal_mediafiles_added_for_entity(mq_client, entity, mediafiles):
    data = {"entity": entity, "mediafiles": mediafiles}
    send_cloudevent(mq_client, "dams", "dams.mediafiles_added_for_entity", data)


def signal_mediafile_changed(mq_client, old_mediafile, mediafile):
    data = {"old_mediafile": old_mediafile, "mediafile": mediafile}
    send_cloudevent(mq_client, "dams", "dams.mediafile_changed", data)


def signal_mediafile_deleted(mq_client, mediafile, linked_entities):
    data = {"mediafile": mediafile, "linked_entities": linked_entities}
    send_cloudevent(mq_client, "dams", "dams.mediafile_deleted", data)


def signal_update_copyright_color_entity(mq_client, entity_id):
    data = {"_id": entity_id}
    send_cloudevent(mq_client, "dams", "dams.update_copyright_color_entity", data)


def signal_update_copyright_color_mediafile(mq_client, mediafile_id):
    data = {"_id": mediafile_id}
    send_cloudevent(mq_client, "dams", "dams.update_copyright_color_mediafile", data)


def signal_upload_file(mq_client, upload_links, selected_folder):
    data = {"upload_links": upload_links, "selected_folder": selected_folder}
    send_cloudevent(mq_client, "dams", "dams.upload_file", data)

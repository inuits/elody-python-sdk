import json
import mimetypes
import re as regex

from cloudevents.conversion import to_dict
from cloudevents.http import CloudEvent
from collections.abc import MutableMapping, Iterable
from copy import deepcopy
from datetime import datetime, timezone
from os import getenv


URL_UNFRIENDLY_CHARS = {
    " ": "%20",
    "!": "%21",
    "'": "%22",
    "#": "%23",
    "%": "%25",
    "&": "%26",
    '"': "%27",
    "(": "%28",
    ")": "%29",
    "*": "%2A",
    "+": "%2B",
    ",": "%2C",
    "/": "%2F",
    ";": "%3B",
    "<": "%3C",
    "=": "%3D",
    ">": "%3E",
    "?": "%3F",
    "@": "%40",
    "[": "%5B",
    "\\": "%5C",
    "]": "%5D",
    "^": "%5E",
    "`": "%60",
    "{": "%7B",
    "|": "%7C",
    "}": "%7D",
    "~": "%7E",
}


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
    if not parent_key:
        data = deepcopy(data)
    flat_dict = {}
    for key, value in __flatten_dict_generator(object_lists, data, parent_key):
        if key in flat_dict:
            if not isinstance(flat_dict[key], list):
                flat_dict[key] = [flat_dict[key]]
            if isinstance(value, list):
                flat_dict[key].extend(value)
            else:
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
            if all(isinstance(item, MutableMapping) for item in value):
                item_key = None
                for item in value:
                    item_key = item.get(object_lists.get(key))
                    if item_key:
                        if isinstance(item, MutableMapping):
                            yield from flatten_dict(
                                object_lists, item, f"{flattened_key}.{item_key}"
                            ).items()
                        else:
                            yield f"{flattened_key}.{item_key}", item["value"]
                if not item_key:
                    yield flattened_key, value
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

    flat_key_parts = regex.split(r"\.(?=(?:[^`]*`[^`]*`)*[^`]*$)", flat_key)
    while index < len(flat_key_parts):
        info = {
            "key": flat_key_parts[index],
            "object_list": (
                flat_key_parts[index]
                if flat_key_parts[index] in object_lists.keys()
                else ""
            ),
        }

        if info["object_list"]:
            combined_key = "".join(
                [f"{info['key']}." for info in keys_info if not info["object_list"]]
            )
            info["key"] = f"{combined_key}{info['key']}"
            info["object_key"] = flat_key_parts[index + 1]
            keys_info = [info for info in keys_info if info["object_list"]]

        keys_info.append(info)
        index += 2 if info["object_list"] else 1

    return keys_info


def mediafile_is_public(mediafile):
    publication_status = get_item_metadata_value(mediafile, "publication_status")
    copyright_color = get_item_metadata_value(mediafile, "copyright_color")
    return publication_status.lower() in [
        "beschermd",
        "expliciet",
        "publiek",
    ] or copyright_color.lower() in ["green", "groen"]


def parse_string_to_bool(value):
    if isinstance(value, str):
        value_lower = value.strip().lower()
        if value_lower in ["true", "yes", "1", "y"]:
            return True
        elif value_lower in ["false", "no", "0", "n"]:
            return False
    return value


def read_json_as_dict(filename, logger):
    try:
        with open(filename) as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as ex:
        logger.error(f"Could not read {filename} as a dict: {ex}")
    return {}


def parse_url_unfriendly_string(
    input: str, *, replace_char=None, return_unfriendly_chars=False
):
    unfriendly_chars = []
    result = input
    for char, encoded in URL_UNFRIENDLY_CHARS.items():
        if char in input:
            unfriendly_chars.append(char)
            replacement = encoded if replace_char is None else replace_char
            result = result.replace(char, replacement)
    if return_unfriendly_chars:
        return result, unfriendly_chars
    return result


def send_cloudevent(mq_client, source, routing_key, data, exchange_name=None):
    event = to_dict(CloudEvent({"source": source, "type": routing_key}, data))
    if getenv("AMQP_MANAGER", "amqpstorm_flask") in ["amqpstorm_flask"]:
        mq_client.send(event, routing_key=routing_key, exchange_name=exchange_name)
    else:
        mq_client.send(event, routing_key=routing_key)


def signal_child_relation_changed(mq_client, collection, id):
    data = {"parent_id": id, "collection": collection}
    send_cloudevent(mq_client, "dams", "dams.child_relation_changed", data)


def signal_edge_changed(mq_client, parent_ids_from_changed_edges):
    data = {
        "location": f'/entities?ids={",".join(parent_ids_from_changed_edges)}&skip_relations=1'
    }
    send_cloudevent(mq_client, "dams", "dams.edge_changed", data)


def signal_entity_changed(mq_client, entity, unchanged_entity=None):
    data = {
        "location": f"/entities/{get_raw_id(entity)}",
        "type": entity.get("type", "unspecified"),
        "unchanged_entity": unchanged_entity,
    }
    send_cloudevent(mq_client, "dams", "dams.entity_changed", data)


def signal_entity_deleted(mq_client, entity):
    data = {"_id": get_raw_id(entity), "type": entity.get("type", "unspecified")}
    send_cloudevent(mq_client, "dams", "dams.entity_deleted", data)


def signal_mediafiles_added_for_entity(mq_client, entity, mediafiles):
    data = {"entity": entity, "mediafiles": mediafiles}
    send_cloudevent(mq_client, "dams", "dams.mediafiles_added_for_entity", data)


def signal_relations_deleted_for_entity(mq_client, entity, relations):
    data = {"entity": entity, "relations": relations}
    send_cloudevent(mq_client, "dams", "dams.relations_deleted_for_entity", data)


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


def signal_upload_external_mediafile(
    mq_client, external_url, mediafile, ticket_id, job_id
):
    data = {
        "external_url": external_url,
        "mediafile": mediafile,
        "ticket_id": ticket_id,
        "job_id": job_id,
    }
    send_cloudevent(mq_client, "dams", "dams.upload_external_mediafile", data)

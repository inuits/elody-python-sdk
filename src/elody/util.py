import json
import mimetypes

from cloudevents.conversion import to_dict
from cloudevents.http import CloudEvent
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


def __send_cloudevent(mq_client, routing_key, data):
    attributes = {"type": routing_key, "source": "dams"}
    event = to_dict(CloudEvent(attributes, data))
    mq_client.send(event, routing_key=routing_key)


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


def signal_child_relation_changed(mq_client, collection, id):
    data = {"parent_id": id, "collection": collection}
    __send_cloudevent(mq_client, "dams.child_relation_changed", data)


def signal_edge_changed(mq_client, parent_ids_from_changed_edges):
    data = {
        "location": f'/entities?ids={",".join(parent_ids_from_changed_edges)}&skip_relations=1'
    }
    __send_cloudevent(mq_client, "dams.edge_changed", data)


def signal_entity_changed(mq_client, entity):
    data = {
        "location": f"/entities/{get_raw_id(entity)}",
        "type": entity.get("type", "unspecified"),
    }
    __send_cloudevent(mq_client, "dams.entity_changed", data)


def signal_entity_deleted(mq_client, entity):
    data = {"_id": get_raw_id(entity), "type": entity.get("type", "unspecified")}
    __send_cloudevent(mq_client, "dams.entity_deleted", data)


def signal_mediafile_changed(mq_client, old_mediafile, mediafile):
    data = {"old_mediafile": old_mediafile, "mediafile": mediafile}
    __send_cloudevent(mq_client, "dams.mediafile_changed", data)


def signal_mediafile_deleted(mq_client, mediafile, linked_entities):
    data = {"mediafile": mediafile, "linked_entities": linked_entities}
    __send_cloudevent(mq_client, "dams.mediafile_deleted", data)

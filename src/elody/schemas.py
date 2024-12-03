box_visit_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "default": {},
    "required": ["code", "start_time"],
    "properties": {
        "_id": {"type": "string"},
        "identifiers": {
            "type": "array",
            "default": [],
            "items": {"$id": "#/properties/identifiers/items"},
        },
        "type": {
            "type": "string",
            "default": "",
        },
        "metadata": {
            "type": "array",
            "default": [],
            "additionalItems": True,
            "items": {"$id": "#/properties/metadata/items"},
        },
        "code": {
            "type": "string",
        },
        "start_time": {
            "type": "string",
        },
        "touch_table_time": {
            "type": "string",
        },
        "frames_seen_last_visit": {
            "type": "string",
        },
    },
}

entity_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "default": {},
    "required": ["type"],
    "properties": {
        "_id": {"type": "string"},
        "identifiers": {
            "type": "array",
            "default": [],
            "items": {"$id": "#/properties/identifiers/items"},
        },
        "type": {
            "type": "string",
            "default": "",
        },
        "metadata": {
            "type": "array",
            "default": [],
            "additionalItems": True,
            "items": {"$id": "#/properties/metadata/items"},
        },
        "primary_mediafile_id": {"type": "string", "default": ""},
        "primary_thumbnail_file_location": {"type": "string", "default": ""},
        "data": {
            "type": "object",
            "title": "The data schema",
            "description": "An explanation about the purpose of this instance.",
            "default": {},
            "required": [],
            "properties": {
                "@context": {
                    "$id": "#/properties/data/properties/%40context",
                    "type": ["array", "string"],
                    "default": [],
                    "additionalItems": True,
                    "items": {"$id": "#/properties/data/properties/%40context/items"},
                },
                "@id": {"type": "string", "default": ""},
                "@type": {"type": "string", "default": ""},
                "memberOf": {
                    "type": "string",
                },
            },
            "additionalProperties": True,
        },
    },
    "additionalProperties": True,
}

key_value_store_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "identifiers": {
            "type": "array",
            "default": [],
            "items": {"$id": "#/properties/identifiers/items"},
        },
        "items": {
            "type": "object",
            "default": {},
        },
    },
}

mediafile_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "default": {},
    "properties": {
        "filename": {
            "type": "string",
        },
        "original_file_location": {
            "type": "string",
        },
        "thumbnail_file_location": {
            "type": "string",
        },
        "entities": {
            "type": "array",
            "default": [],
            "items": {
                "anyOf": [
                    {
                        "type": "string",
                    }
                ]
            },
        },
    },
}

saved_search_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "default": {},
    "required": ["definition", "private", "type"],
    "properties": {
        "_id": {"type": "string"},
        "definition": {
            "type": "array",
            "default": [],
            "additionalItems": True,
            "items": {"$id": "#/properties/metadata/items"},
        },
        "identifiers": {
            "type": "array",
            "default": [],
            "items": {"$id": "#/properties/identifiers/items"},
        },
        "metadata": {
            "type": "array",
            "default": [],
            "additionalItems": True,
            "items": {"$id": "#/properties/metadata/items"},
        },
        "private": {
            "type": "boolean",
            "default": True,
        },
        "type": {
            "type": "string",
            "default": "",
        },
        "user": {
            "type": "string",
            "default": "",
        },
    },
    "additionalProperties": True,
}

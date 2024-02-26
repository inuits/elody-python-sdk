import csv
import re

from io import StringIO
from elody.exceptions import (
    ColumnNotFoundException,
    IncorrectTypeException,
    InvalidObjectException,
)
from elody.validator import validate_json
from elody.schemas import entity_schema, mediafile_schema


class CSVParser:
    top_level_fields = ["type", "filename"]
    identifier_fields = ["identifiers", "identifier", "object_id", "entity_id"]
    schema_mapping = {
        "entity": entity_schema,
        "entities": entity_schema,
        "mediafile": mediafile_schema,
        "mediafiles": mediafile_schema,
    }

    def __init__(self, csvstring):
        self.csvstring = csvstring
        self.reader = csv.DictReader(self.__csv_string_to_file_object())

    def _get_metadata_object(self, key, value, lang="en"):
        return {
            "key": key,
            "value": value,
            "lang": lang,
        }

    def _get_relation_object(self, type, key):
        return {
            "type": type,
            "key": key,
        }

    def _is_relation_field(self, field):
        if re.fullmatch("(has|is)([A-Z][a-z]+)+", field):
            return True
        return False

    def __csv_string_to_file_object(self):
        return StringIO(self.csvstring)


class CSVSingleObject(CSVParser):
    def __init__(self, csvstring, object_type="entity"):
        super().__init__(csvstring)
        self.identifiers = list()
        self.metadata = list()
        self.object_type = object_type
        self.objects = list()
        self.relations = list()
        self.__init_fields()

    def get_entity(self):
        return self.get_type("entity")

    def get_mediafile(self):
        return self.get_type("mediafile")

    def get_type(self, type="entity"):
        if self.object_type != type:
            raise IncorrectTypeException(f"Not a {type}!")
        object = dict()
        for property_name, property in {
            "metadata": self.metadata,
            "relations": self.relations,
            "identifiers": self.identifiers,
        }.items():
            if property:
                object[property_name] = property
        for top_level_field in self.top_level_fields:
            if getattr(self, top_level_field, None):
                object[top_level_field] = getattr(self, top_level_field)
        if validation_error := validate_json(
            object, self.schema_mapping.get(type, entity_schema)
        ):
            raise InvalidObjectException(
                f"{type} doesn't have a valid format. {validation_error}"
            )
        return object

    def __fill_identifiers(self, identifier):
        if identifier and identifier not in self.identifiers:
            self.identifiers.append(identifier)

    def __fill_metadata(self, key, value):
        if value:
            self.metadata.append(self._get_metadata_object(key, value))

    def __fill_relations(self, type, key):
        if key:
            self.relations.append(self._get_relation_object(type, key))

    def __init_fields(self):
        for row in self.reader:
            for key, value in row.items():
                if self._is_relation_field(key):
                    self.__fill_relations(key, value)
                elif key in self.identifier_fields:
                    self.__fill_identifiers(value)
                elif key in self.top_level_fields and value:
                    setattr(self, key, value)
                else:
                    self.__fill_metadata(key, value)


class CSVMultiObject(CSVParser):
    def __init__(
        self,
        csvstring,
        index_mapping=None,
        object_field_mapping=None,
        required_metadata_values=None,
    ):
        super().__init__(csvstring)
        self.index_mapping = dict()
        if index_mapping:
            self.index_mapping = index_mapping
        self.object_field_mapping = dict()
        if object_field_mapping:
            self.object_field_mapping = object_field_mapping
        self.required_metadata_values = required_metadata_values
        self.objects = dict()
        self.errors = dict()
        self.__fill_objects_from_csv()

    def get_entities(self):
        return self.objects.get("entities", list())

    def get_errors(self):
        return self.errors

    def get_mediafiles(self):
        return self.objects.get("mediafiles", list())

    def __field_allowed(self, target_object_type, key, value):
        for object_type, fields in self.object_field_mapping.items():
            for _ in [x for x in fields if x == key]:
                if object_type == target_object_type:
                    return bool(value)
                return False
            if object_type == target_object_type:
                return False
        return bool(value)

    def __fill_objects_from_csv(self):
        indexed_dict = dict()
        for row in self.reader:
            if not all(x in row.keys() for x in self.index_mapping.values()):
                raise ColumnNotFoundException(
                    f"Not all identifying columns are present in CSV"
                )
            previous_id = None
            for type, identifying_column in self.index_mapping.items():
                id = row[identifying_column]
                if type not in indexed_dict:
                    indexed_dict[type] = dict()
                if id not in indexed_dict[type]:
                    indexed_dict[type][id] = dict()
                indexed_dict[type][id]["matching_id"] = id
                if previous_id:
                    indexed_dict[type][id]["matching_id"] = previous_id
                previous_id = id
                for key, value in row.items():
                    if self._is_relation_field(key) and self.__field_allowed(
                        type, key, value
                    ):
                        indexed_dict[type][id].setdefault("relations", list())
                        indexed_dict[type][id]["relations"].append(
                            self._get_relation_object(key, value)
                        )
                    elif key in self.identifier_fields and self.__field_allowed(
                        type, key, value
                    ):
                        indexed_dict[type][id].setdefault("identifiers", list())
                        if value not in indexed_dict[type][id]["identifiers"]:
                            indexed_dict[type][id]["identifiers"].append(value)
                    elif key in self.top_level_fields and self.__field_allowed(
                        type, key, value
                    ):
                        indexed_dict[type][id][key] = value
                    elif (
                        key not in self.index_mapping.values()
                        and self.__field_allowed(type, key, value)
                    ):
                        indexed_dict[type][id].setdefault("metadata", list())
                        indexed_dict[type][id]["metadata"].append(
                            self._get_metadata_object(key, value)
                        )
        self.__validate_indexed_dict(indexed_dict)
        self.__add_required_fields(indexed_dict)
        for object_type, objects in indexed_dict.items():
            self.objects[object_type] = list(objects.values())

    def __add_required_fields(self, indexed_dict):
        if not self.required_metadata_values:
            return
        for object_type, objects in indexed_dict.items():
            for required_key, required_value in self.required_metadata_values.get(
                object_type, dict()
            ).items():
                for object in objects.values():
                    for metadata in object.get("metadata", list()):
                        if metadata.get("key") == required_key:
                            break
                    else:
                        if "metadata" not in object:
                            object["metadata"] = list()
                        object["metadata"].append(
                            self._get_metadata_object(required_key, required_value)
                        )

    def __validate_indexed_dict(self, indexed_dict):
        for object_type, objects in indexed_dict.items():
            error_ids = list()
            for object_id, object in objects.items():
                if validation_error := validate_json(
                    object, self.schema_mapping.get(object_type, entity_schema)
                ):
                    error_ids.append(object_id)
                    if object_type not in self.errors:
                        self.errors[object_type] = list()
                    self.errors[object_type].append(
                        f"{object_type} with index {object_id} doesn't have a valid format. {validation_error}"
                    )
            for error_id in error_ids:
                del objects[error_id]

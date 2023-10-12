import csv
import re

from io import StringIO
from elody.exceptions import ColumnNotFoundException, IncorrectTypeException


class CSVParser:
    top_level_fields = ["type", "filename"]
    identifier_fields = ["identifiers", "identifier", "object_id", "entity_id"]

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
        if re.fullmatch("(has|is)[A-Z][a-z]+", field):
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
    def __init__(self, csvstring, index_mapping=None, object_field_mapping=None):
        super().__init__(csvstring)
        self.index_mapping = dict()
        if index_mapping:
            self.index_mapping = index_mapping
        self.object_field_mapping = dict()
        if object_field_mapping:
            self.object_field_mapping = object_field_mapping
        self.objects = dict()
        self.__fill_objects_from_csv()

    def get_entities(self):
        return self.objects.get("entities", list())

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
        for metadata_type, objects in indexed_dict.items():
            self.objects[metadata_type] = list(objects.values())

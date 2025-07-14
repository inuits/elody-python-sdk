import csv
import re

from io import StringIO
from elody.error_codes import ErrorCode, get_error_code, get_write
from elody.exceptions import (
    ColumnNotFoundException,
    IncorrectTypeException,
    InvalidObjectException,
    InvalidValueException,
)
from elody.validator import validate_json
from elody.schemas import entity_schema, mediafile_schema
from dateutil import parser


class CSVParser:
    top_level_fields = ["type", "filename", "file_identifier"]
    identifier_fields = ["identifiers", "identifier", "object_id", "entity_id"]
    schema_mapping = {
        "entity": entity_schema,
        "entities": entity_schema,
        "mediafile": mediafile_schema,
        "mediafiles": mediafile_schema,
    }

    def __init__(self, csvstring=None, csvfile=None):
        if csvstring:
            self.csvstring = csvstring
            self.reader = self.__get_reader_from_csv(self.__csv_string_to_file_object())
        elif csvfile:
            self.reader = self.__get_reader_from_csv(csvfile)

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

    def __get_reader_from_csv(self, csv_file):
        csv_dialect = csv.Sniffer().sniff(csv_file.read())
        csv_file.seek(0)
        return csv.DictReader(csv_file, dialect=csv_dialect)


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
        metadata_field_mapping=None,
        include_indexed_field=False,
        top_level_fields_mapping=None,
        external_file_sources=None,
    ):
        super().__init__(csvstring)
        self.index_mapping = index_mapping if index_mapping else dict()
        self.object_field_mapping = (
            object_field_mapping if object_field_mapping else dict()
        )
        self.required_metadata_values = (
            required_metadata_values if required_metadata_values else dict()
        )
        self.metadata_field_mapping = (
            metadata_field_mapping if metadata_field_mapping else dict()
        )
        self.objects = dict()
        self.errors = dict()
        self.include_indexed_field = include_indexed_field
        self.top_level_fields_mapping = (
            top_level_fields_mapping if top_level_fields_mapping else dict()
        )
        self.external_file_sources = (
            external_file_sources if external_file_sources else []
        )
        self.line_numbers = []
        self.__fill_objects_from_csv()
        self.__rename_top_level_fields()

    def get_entities(self):
        return self.objects.get("entities", list())

    def get_errors(self):
        return self.errors

    def get_line_numbers(self):
        return self.line_numbers

    def get_line_number(self, entity_identifier=None, mediafile_identifier=None):
        for line_number, line_info in enumerate(self.line_numbers, start=1):
            if (
                entity_identifier
                and line_info.get("entity_identifier") != entity_identifier
            ):
                continue
            if (
                mediafile_identifier
                and line_info.get("mediafile_identifier") != mediafile_identifier
            ):
                continue
            return line_number
        return None

    def set_error(self, type, errors):
        self.errors[type] = errors

    def get_top_level_fields_mapping(self, type):
        return self.top_level_fields_mapping.get(type, {})

    def get_mediafiles(self):
        return self.objects.get("mediafiles", list())

    def __determine_language(self, row):
        if "language" in row:
            return row.get("language")
        elif "lang" in row:
            return row.get("lang")
        else:
            return "en"

    def __field_allowed(self, target_object_type, key, value):
        for object_type, fields in self.object_field_mapping.items():
            for _ in [x for x in fields if x == key]:
                if object_type == target_object_type:
                    return bool(value)
                return False
            if object_type == target_object_type:
                return False
        return bool(value)

    def is_datetime(self, value):
        try:
            int(value)
            return False
        except (ValueError, TypeError):
            try:
                parser.parse(value)
                return True
            except (ValueError, TypeError):
                return False

    def parse_datetime(self, value):
        return parser.parse(value)

    def __fill_objects_from_csv(self):
        indexed_dict = dict()
        external_mediafiles_ids = []
        for row_number, row in enumerate(self.reader, start=1):
            normalized_mapping = {
                key.lstrip("?"): value for key, value in self.index_mapping.items()
            }
            entity_identifier_column = normalized_mapping.get("entities")
            mediafile_identifier_column = normalized_mapping.get("mediafiles")
            entity_identifier = (
                row.get(entity_identifier_column) if entity_identifier_column else None
            )
            mediafile_identifier = (
                row.get(mediafile_identifier_column)
                if mediafile_identifier_column
                else None
            )
            self.line_numbers.append(
                {
                    "line": row_number,
                    "entity_identifier": entity_identifier,
                    "mediafile_identifier": mediafile_identifier,
                }
            )
            mandatory_columns = [
                v for k, v in self.index_mapping.items() if not k.startswith("?")
            ]
            missing_columns = [x for x in mandatory_columns if x not in row.keys()]
            if missing_columns:
                raise ColumnNotFoundException(f"{', '.join(missing_columns)}")
            lang = self.__determine_language(row)
            previous_id = None
            for type, identifying_column in self.index_mapping.items():
                is_type_optional = False
                if type.startswith("?"):
                    is_type_optional = True
                    type = type.lstrip("?")
                if not row.get(identifying_column) and is_type_optional:
                    continue
                id = row[identifying_column]
                if type not in indexed_dict:
                    indexed_dict[type] = dict()
                if id not in indexed_dict[type]:
                    indexed_dict[type][id] = dict()
                indexed_dict[type][id]["matching_id"] = id
                if previous_id:
                    indexed_dict[type][id]["matching_id"] = previous_id
                previous_id = id
                file_source = None
                for key, value in row.items():
                    if not value:
                        continue
                    if not key or isinstance(value, list):
                        if len(value) == 1 and value[0] == "":
                            continue
                        if "invalid_value" not in self.get_errors():
                            self.set_error("invalid_value", list())
                        message = f'{get_error_code(ErrorCode.INVALID_VALUE, get_write())} | value:{value} | line_number:{row_number} - The value "{value}" is invalid, most likely caused by exceeding allowed columns.'
                        self.get_errors()["invalid_value"].append(message)
                    original_value = value
                    if key != identifying_column:
                        value = value.lower()
                    if key == "file_source":
                        file_source = value
                    if (
                        key == "file_identifier"
                        and file_source in self.external_file_sources
                    ):
                        matching_id = indexed_dict[type][id]["matching_id"]
                        if not any(matching_id in id for id in external_mediafiles_ids):
                            external_mediafiles_ids.append({matching_id: file_source})
                        if "entities" not in indexed_dict:
                            indexed_dict["entities"] = dict()
                        if id in indexed_dict["entities"]:
                            indexed_dict["entities"][id]["file_identifier"] = value
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
                        indexed_dict[type][id][key] = original_value
                    elif (
                        key not in self.index_mapping.values()
                        or self.include_indexed_field
                    ) and self.__field_allowed(type, key, value):
                        metadata_info = self.metadata_field_mapping.get(key, {})
                        if metadata_info.get("target") == type or not metadata_info:
                            metadata_key = metadata_info.get("map_to", key)
                            indexed_dict[type][id].setdefault("metadata", list())
                            options = metadata_info.get("value_options")
                            if self.is_datetime(value):
                                original_value = self.parse_datetime(value)
                            if options and value not in options:
                                if "invalid_value" not in self.get_errors():
                                    self.set_error("invalid_value", list())
                                message = f'{get_error_code(ErrorCode.INVALID_VALUE, get_write())} | value:{value} | options:{options}| line_number:{row_number} - The value "{value}" is invalid, these are the valid values: {options}'
                                self.get_errors()["invalid_value"].append(message)

                            indexed_dict[type][id]["metadata"].append(
                                self._get_metadata_object(
                                    metadata_key, original_value, lang
                                )
                            )
        self.__validate_indexed_dict(indexed_dict)
        self.__add_required_fields(indexed_dict)
        for object_type, objects in indexed_dict.items():
            self.objects[object_type] = list(objects.values())
        if external_mediafiles_ids:
            for mediafile in self.objects["mediafiles"]:
                matching_id = mediafile["matching_id"]
                for entry in external_mediafiles_ids:
                    if matching_id in entry:
                        file_source = entry[matching_id]
                        dynamic_key = f"is_{file_source}_mediafile"
                        mediafile[dynamic_key] = True
                        break

    def __add_required_fields(self, indexed_dict):
        if not self.required_metadata_values:
            return
        for object_type, objects in indexed_dict.items():
            required_fields = self.required_metadata_values.get(object_type, {})
            for required_key, required_value in required_fields.items():
                for object in objects.values():
                    if "metadata" not in object:
                        object["metadata"] = []
                    if not any(
                        metadata.get("key") == required_key
                        for metadata in object["metadata"]
                    ):
                        if required_value is not None:
                            metadata_object = self._get_metadata_object(
                                required_key, required_value
                            )
                        else:
                            raise ColumnNotFoundException(required_key)
                        object["metadata"].append(metadata_object)

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

    def __rename_top_level_fields(self):
        def rename_fields(items, mapping):
            for item in items:
                for old_key, new_key in mapping.items():
                    if old_key in item:
                        item[new_key] = item.pop(old_key)

        mediafiles = self.get_mediafiles()
        entities = self.get_entities()
        mediafiles_mapping = self.get_top_level_fields_mapping("mediafiles")
        entities_mapping = self.get_top_level_fields_mapping("entities")

        rename_fields(mediafiles, mediafiles_mapping)
        rename_fields(entities, entities_mapping)

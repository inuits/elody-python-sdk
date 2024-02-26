from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate


def validate_json(json, schema):
    try:
        validate(instance=json, schema=schema)
    except ValidationError as ve:
        return ve.message
    return None

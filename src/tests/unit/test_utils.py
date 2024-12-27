import pytest
import json

from elody.util import (
    parse_string_to_bool,
    interpret_flat_key,
    __flatten_dict_generator,
    get_raw_id,
    get_item_metadata_value,
    get_mimetype_from_filename,
    mediafile_is_public,
    read_json_as_dict,
    parse_url_unfriendly_string,
    CustomJSONEncoder,
)
from data import mediafile1, mediafile2
from datetime import datetime, timezone
from unittest.mock import mock_open, patch, MagicMock


def test_default_method_with_datetime():
    encoder = CustomJSONEncoder()
    dt = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    result = encoder.default(dt)
    assert result == "2023-10-01T12:00:00+00:00"


def test_default_method_with_naive_datetime():
    encoder = CustomJSONEncoder()
    dt = datetime(2023, 10, 1, 12, 0, 0)
    result = encoder.default(dt)
    assert result == "2023-10-01T10:00:00+00:00"


def test_encode_method_with_non_datetime():
    encoder = CustomJSONEncoder()
    obj = {"key": "value"}
    result = encoder.encode(obj)
    expected_result = json.dumps(obj)
    assert result == expected_result


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        # Truthy values
        ("true", True),
        ("yes", True),
        ("1", True),
        ("Y", True),
        ("   true   ", True),
        # Falsy values
        ("false", False),
        ("no", False),
        ("0", False),
        ("N", False),
        ("   no   ", False),
        # Non-string inputs
        (123, 123),
        (None, None),
        (True, True),
        (False, False),
        # Edge cases
        ("", ""),  # Empty string
        ("maybe", "maybe"),  # Unexpected string
        ("random", "random"),  # Another unexpected string
    ],
)
def test_parse_string_to_bool(input_value, expected_output):
    assert parse_string_to_bool(input_value) == expected_output


def test_interpret_flat_key():
    object_lists = {"user": ["name", "email"], "address": ["street", "city"]}

    # Test case 1
    flat_key = "user.name"
    expected_output = [{"key": "user", "object_list": "user", "object_key": "name"}]
    assert interpret_flat_key(flat_key, object_lists) == expected_output

    # Test case 2
    flat_key = "user.email"
    expected_output = [{"key": "user", "object_list": "user", "object_key": "email"}]
    assert interpret_flat_key(flat_key, object_lists) == expected_output

    # Test case 3
    flat_key = "address.street"
    expected_output = [
        {"key": "address", "object_list": "address", "object_key": "street"}
    ]
    assert interpret_flat_key(flat_key, object_lists) == expected_output

    # Test case 4
    flat_key = "address.city"
    expected_output = [
        {"key": "address", "object_list": "address", "object_key": "city"}
    ]
    assert interpret_flat_key(flat_key, object_lists) == expected_output


def test_flatten_dict_generator():
    object_lists = {"items": "id"}

    data = {
        "name": "John",
        "address": {"street": "123 Main St", "city": "Anytown"},
        "items": [{"id": "1", "value": "item1"}, {"id": "2", "value": "item2"}],
        "tags": ["tag1", "tag2"],
    }

    expected_output = {
        "name": "John",
        "address.street": "123 Main St",
        "address.city": "Anytown",
        "items.1.id": "1",
        "items.1.value": "item1",
        "items.2.id": "2",
        "items.2.value": "item2",
        "tags": ["tag1", "tag2"],
    }

    result = dict(__flatten_dict_generator(object_lists, data, ""))
    assert result == expected_output


def test_flatten_dict_generator_nested():
    object_lists = {"items": "id"}

    data = {
        "user": {
            "name": "John",
            "address": {"street": "123 Main St", "city": "Anytown"},
            "items": [{"id": "1", "value": "item1"}, {"id": "2", "value": "item2"}],
            "tags": ["tag1", "tag2"],
        }
    }

    expected_output = {
        "user.name": "John",
        "user.address.street": "123 Main St",
        "user.address.city": "Anytown",
        "user.items.1.id": "1",
        "user.items.1.value": "item1",
        "user.items.2.id": "2",
        "user.items.2.value": "item2",
        "user.tags": ["tag1", "tag2"],
    }

    result = dict(__flatten_dict_generator(object_lists, data, ""))
    assert result == expected_output


def test_flatten_dict_generator_no_items():
    object_lists = {"items": "id"}

    data = {
        "name": "John",
        "address": {"street": "123 Main St", "city": "Anytown"},
        "items": [{"value": "item1"}, {"value": "item2"}],
        "tags": ["tag1", "tag2"],
    }

    expected_output = {
        "name": "John",
        "address.street": "123 Main St",
        "address.city": "Anytown",
        "items": [{"value": "item1"}, {"value": "item2"}],
        "tags": ["tag1", "tag2"],
    }

    result = dict(__flatten_dict_generator(object_lists, data, ""))
    assert result == expected_output


@pytest.mark.parametrize(
    "mediafile_modification, expected_output",
    [
        (lambda mf: mf, "2476c1a2-c323-499e-9e28-6d1562296f3f"),
        (lambda mf: mf.update({"_key": "test-key"}) or mf, "test-key"),
    ],
)
def test_get_raw_id(mediafile_modification, expected_output):
    modified_mediafile = mediafile_modification(mediafile1.copy())
    assert get_raw_id(modified_mediafile) == expected_output

    # Test for KeyError
    modified_mediafile.pop("_key", None)
    modified_mediafile.pop("_id", None)
    with pytest.raises(KeyError):
        get_raw_id(modified_mediafile)


@pytest.mark.parametrize(
    "metadata_key, expected_output",
    [
        ("copyright_color", "green"),
        ("copyright_color_calculation", "automatic"),
        ("not_existing", ""),
    ],
)
def test_get_item_metadata_value(metadata_key, expected_output):
    assert get_item_metadata_value(mediafile1, metadata_key) == expected_output


@pytest.mark.parametrize(
    "filename, expected_output",
    [
        ("test.jpg", "image/jpeg"),
        ("test.png", "image/png"),
        ("test.tif", "image/tiff"),
        ("test.mp4", "video/mp4"),
        ("test", "application/octet-stream"),
    ],
)
def test_get_mimetype_from_filename(filename, expected_output):
    assert get_mimetype_from_filename(filename) == expected_output


@pytest.mark.parametrize(
    "mediafile, expected_output",
    [
        (mediafile1, True),
        (mediafile2, False),
    ],
)
def test_mediafile_is_public(mediafile, expected_output):
    assert mediafile_is_public(mediafile) == expected_output


@pytest.fixture
def logger():
    return MagicMock()


def test_read_json_as_dict_success(logger):
    filename = "test.json"
    json_content = '{"key": "value"}'
    expected_output = {"key": "value"}

    with patch("builtins.open", mock_open(read_data=json_content)):
        result = read_json_as_dict(filename, logger)
        assert result == expected_output
        logger.error.assert_not_called()


def test_read_json_as_dict_file_not_found(logger):
    filename = "test.json"

    with patch("builtins.open", side_effect=FileNotFoundError):
        result = read_json_as_dict(filename, logger)
        assert result == {}
        logger.error.assert_called_once_with(f"Could not read {filename} as a dict: ")


def test_read_json_as_dict_invalid_json(logger):
    filename = "test.json"
    invalid_json_content = '{"key": "value"'  # Missing closing brace

    with patch("builtins.open", mock_open(read_data=invalid_json_content)):
        result = read_json_as_dict(filename, logger)
        assert result == {}
        logger.error.assert_called_once_with(
            f"Could not read {filename} as a dict: Expecting ',' delimiter: line 1 column 16 (char 15)"
        )


@pytest.mark.parametrize(
    "input_str, replace_char, return_unfriendly_chars, expected_output",
    [
        # Test cases with default behavior
        ("Hello World!", None, False, "Hello%20World%21"),
        ("abc/def", None, False, "abc%2Fdef"),
        # Test cases with replace_char specified
        ("Hello World!", "-", False, "Hello-World-"),
        ("abc/def", "*", False, "abc*def"),
        # Test cases with return_unfriendly_chars enabled
        ("Hello World!", None, True, ("Hello%20World%21", [" ", "!"])),
        ("abc/def", None, True, ("abc%2Fdef", ["/"])),
        ("Hello World!", "-", True, ("Hello-World-", [" ", "!"])),
        # Edge cases
        ("", None, False, ""),
        ("NoSpecialChars", None, False, "NoSpecialChars"),
    ],
)
def test_parse_url_unfriendly_string(
    input_str, replace_char, return_unfriendly_chars, expected_output
):
    result = parse_url_unfriendly_string(
        input_str,
        replace_char=replace_char,
        return_unfriendly_chars=return_unfriendly_chars,
    )
    assert result == expected_output

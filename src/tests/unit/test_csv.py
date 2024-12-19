import pytest
from elody.exceptions import (
    ColumnNotFoundException,
    InvalidValueException,
)
from elody.csv import CSVMultiObject


sample_basic_csv_digipolis = """external_id,external_system,type,file_source,file_identifier,asset_copyright_color,mediafile_copyright_color,photographer,license
tg:lhaq:8363:m1,arches,asset,file,meeuw.jpg,orange,red,Jos,test
"""

sample_basic_csv_digipolis_wrong_values = """external_id,external_system,type,file_source,file_identifier,asset_copyright_color,mediafile_copyright_color,photographer,license
tg:lhaq:8363:m1,arches,asset,file,meeuw.jpg,orange,red,Jos,test
tg:lhaq:8364:m2,arches,asset,file,meeuw2.jpg,blue,green,Jane,test
tg:lhaq:8365:m3,arches,asset,file,meeuw3.jpg,yellow,purple,John,test
"""

sample_meemoo_csv_digipolis = """external_id,external_system,type,file_source,file_identifier,asset_copyright_color,mediafile_copyright_color,photographer,license
tg:lhaq:8569:m1,arches,asset,file,tijger.jpg,green,red,fotograaf,license
tg:lhaq:8568:m1,arches,asset,meemoo,5717m4t678,green,red,fotograaf,license
tg:lhaq:8570:m1,arches,asset,meemoo,8w3809mt4j,green,red,fotograaf,license
"""

sample_basic_csv_vliz = """same_entity,title,description,coordinates,media_keyword,language,asset_category,location_type,marine_region,event,project,partner,creator_person,owner_partner,type,filename,content_drager,external_link,usage_guidelines,usage_guidelines_until,embargo,qualityRating,mediafile_keyword,mediafile_creator_person,mediafile_owner_partner,confidentiality,mediafile_person
1,This is a media entity,This is a description,,keyword1,,,,,,,,person1,partner1,media,leeuw.jpg,,,,,,,keyword,mediaperson,mediapartner,,
"""

sample_basic_csv_vliz_missing_values = """title,description,coordinates,media_keyword,language,asset_category,location_type,marine_region,event,project,partner,creator_person,owner_partner,type,filename,content_drager,external_link,usage_guidelines,usage_guidelines_until,embargo,qualityRating,mediafile_keyword,mediafile_creator_person,mediafile_owner_partner,confidentiality,mediafile_person
This is a media entity,This is a description,,keyword1,,,,,,,,person1,partner1,media,leeuw.jpg,,,,,,,keyword,mediaperson,mediapartner,,
"""

sample_multiple_keywords_csv_vliz = """same_entity,title,description,coordinates,media_keyword,language,asset_category,location_type,marine_region,event,project,partner,creator_person,owner_partner,type,filename,content_drager,external_link,usage_guidelines,usage_guidelines_until,embargo,qualityRating,mediafile_keyword,mediafile_creator_person,mediafile_owner_partner,confidentiality,mediafile_person
1,This is a media entity,This is a description,,keyword1,,,,,,,,person1,partner1,media,leeuw.jpg,,,,,,,keyword,mediaperson,mediapartner,,
1,,,,keyword2,,,,,,,,,,,leeuw.jpg,,,,,,,keyword_mediafile,,,,
"""

sample_csv_without_mediafile_digi = """external_id,external_system,type,asset_copyright_color
tg:lhps:32930:m1,arches,asset,green
"""

sample_csv_without_mediafile_vliz = """same_entity,title,description,coordinates,media_keyword,language,asset_category,location_type,marine_region,event,project,partner,creator_person,owner_partner,type
1,This is a media entity without a mediafile,This is a description,,keyword1,,,,,,,,person1,partner1,media
"""

expected_basic_objects_digipolis = {
    "entities": [
        {
            "matching_id": "tg:lhaq:8363:m1",
            "metadata": [
                {"key": "external_id", "value": "tg:lhaq:8363:m1", "lang": "en"},
                {"key": "external_system", "value": "arches", "lang": "en"},
                {"key": "file_source", "value": "file", "lang": "en"},
                {"key": "copyright_color", "value": "orange", "lang": "en"},
            ],
            "type": "asset",
        }
    ],
    "mediafiles": [
        {
            "matching_id": "tg:lhaq:8363:m1",
            "metadata": [
                {"key": "copyright_color", "value": "red", "lang": "en"},
                {"key": "photographer", "value": "Jos", "lang": "en"},
            ],
            "filename": "meeuw.jpg",
        }
    ],
}

expected_meemoo_objects_digipolis = {
    "entities": [
        {
            "matching_id": "tg:lhaq:8569:m1",
            "metadata": [
                {"key": "external_id", "value": "tg:lhaq:8569:m1", "lang": "en"},
                {"key": "external_system", "value": "arches", "lang": "en"},
                {"key": "file_source", "value": "file", "lang": "en"},
                {"key": "copyright_color", "value": "green", "lang": "en"},
            ],
            "type": "asset",
        },
        {
            "matching_id": "tg:lhaq:8568:m1",
            "metadata": [
                {"key": "external_id", "value": "tg:lhaq:8568:m1", "lang": "en"},
                {"key": "external_system", "value": "arches", "lang": "en"},
                {"key": "file_source", "value": "meemoo", "lang": "en"},
                {"key": "copyright_color", "value": "green", "lang": "en"},
            ],
            "type": "asset",
            "file_identifier": "5717m4t678",
        },
        {
            "matching_id": "tg:lhaq:8570:m1",
            "metadata": [
                {"key": "external_id", "value": "tg:lhaq:8570:m1", "lang": "en"},
                {"key": "external_system", "value": "arches", "lang": "en"},
                {"key": "file_source", "value": "meemoo", "lang": "en"},
                {"key": "copyright_color", "value": "green", "lang": "en"},
            ],
            "type": "asset",
            "file_identifier": "8w3809mt4j",
        },
    ],
    "mediafiles": [
        {
            "matching_id": "tg:lhaq:8569:m1",
            "metadata": [
                {"key": "copyright_color", "value": "red", "lang": "en"},
                {"key": "photographer", "value": "fotograaf", "lang": "en"},
            ],
            "filename": "tijger.jpg",
        },
        {
            "matching_id": "tg:lhaq:8568:m1",
            "metadata": [
                {"key": "copyright_color", "value": "red", "lang": "en"},
                {"key": "photographer", "value": "fotograaf", "lang": "en"},
            ],
            "is_meemoo_mediafile": True,
            "filename": "5717m4t678",
        },
        {
            "matching_id": "tg:lhaq:8570:m1",
            "metadata": [
                {"key": "copyright_color", "value": "red", "lang": "en"},
                {"key": "photographer", "value": "fotograaf", "lang": "en"},
            ],
            "is_meemoo_mediafile": True,
            "filename": "8w3809mt4j",
        },
    ],
}

expected_basic_objects_vliz = {
    "entities": [
        {
            "matching_id": "1",
            "metadata": [
                {"key": "title", "value": "This is a media entity", "lang": ""},
                {"key": "description", "value": "This is a description", "lang": ""},
                {"key": "media_keyword", "value": "keyword1", "lang": ""},
                {"key": "creator_person", "value": "person1", "lang": ""},
                {"key": "owner_partner", "value": "partner1", "lang": ""},
            ],
            "type": "media",
        }
    ],
    "mediafiles": [
        {
            "matching_id": "1",
            "filename": "leeuw.jpg",
            "metadata": [
                {"key": "mediafile_keyword", "value": "keyword", "lang": ""},
                {"key": "mediafile_creator_person", "value": "mediaperson", "lang": ""},
                {"key": "mediafile_owner_partner", "value": "mediapartner", "lang": ""},
            ],
        }
    ],
}

expected_multiple_keywords_objects_vliz = {
    "entities": [
        {
            "matching_id": "1",
            "metadata": [
                {"key": "title", "value": "This is a media entity", "lang": ""},
                {"key": "description", "value": "This is a description", "lang": ""},
                {"key": "media_keyword", "value": "keyword1", "lang": ""},
                {"key": "creator_person", "value": "person1", "lang": ""},
                {"key": "owner_partner", "value": "partner1", "lang": ""},
                {"key": "media_keyword", "value": "keyword2", "lang": ""},
            ],
            "type": "media",
        }
    ],
    "mediafiles": [
        {
            "matching_id": "1",
            "filename": "leeuw.jpg",
            "metadata": [
                {"key": "mediafile_keyword", "value": "keyword", "lang": ""},
                {"key": "mediafile_creator_person", "value": "mediaperson", "lang": ""},
                {"key": "mediafile_owner_partner", "value": "mediapartner", "lang": ""},
                {"key": "mediafile_keyword", "value": "keyword_mediafile", "lang": ""},
            ],
        }
    ],
}

expected_only_entities_object_digipolis = {
    "entities": [
        {
            "matching_id": "tg:lhps:32930:m1",
            "metadata": [
                {"key": "external_id", "value": "tg:lhps:32930:m1", "lang": "en"},
                {"key": "external_system", "value": "arches", "lang": "en"},
                {"key": "copyright_color", "value": "green", "lang": "en"},
            ],
            "type": "asset",
        }
    ]
}

expected_only_entities_object_vliz = {
    "entities": [
        {
            "matching_id": "1",
            "metadata": [
                {
                    "key": "title",
                    "value": "This is a media entity without a mediafile",
                    "lang": "",
                },
                {"key": "description", "value": "This is a description", "lang": ""},
                {"key": "media_keyword", "value": "keyword1", "lang": ""},
                {"key": "creator_person", "value": "person1", "lang": ""},
                {"key": "owner_partner", "value": "partner1", "lang": ""},
            ],
            "type": "media",
        }
    ]
}


def init_digipolis_csv_object(csv):
    csv_multi_object = CSVMultiObject(
        csv,
        index_mapping={
            "entities": "external_id",
            "?mediafiles": "file_identifier",
        },
        object_field_mapping={
            "mediafiles": [
                "filename",
                "file_identifier",
                "publication_status",
                "mediafile_copyright_color",
                "mediafile_description" "license",
                "photographer",
                "publication_status",
                "back_office",
                "scan",
                "portrait_rights",
            ],
            "entities": [
                "type",
                "asset_copyright_color",
                "asset_description",
                "creator",
                "tag",
                "external_id",
                "external_system",
                "closed_deposit",
                "file_source",
            ],
        },
        required_metadata_values={"mediafiles": {"copyright_color": "red"}},
        metadata_field_mapping={
            "asset_copyright_color": {
                "target": "entities",
                "map_to": "copyright_color",
                "value_options": ["", "automatic", "green", "orange", "red"],
            },
            "mediafile_copyright_color": {
                "target": "mediafiles",
                "map_to": "copyright_color",
                "value_options": ["", "automatic", "green", "orange", "red"],
            },
            "asset_description": {
                "target": "asset",
                "map_to": "description",
            },
            "mediafile_description": {
                "target": "mediafiles",
                "map_to": "description",
            },
        },
        include_indexed_field=True,
        top_level_fields_mapping={"mediafiles": {"file_identifier": "filename"}},
        external_file_sources=["meemoo"],
    )

    return csv_multi_object


def init_vliz_csv_object(csv):
    csv_multi_object = CSVMultiObject(
        csv,
        index_mapping={"entities": "same_entity", "?mediafiles": "filename"},
        object_field_mapping={
            "mediafiles": [
                "filename",
                "content_drager",
                "external_link",
                "usage_guidelines",
                "usage_guidelines_until",
                "embargo",
                "qualityRating",
                "mediafile_keyword",
                "confidentiality",
                "subtitle",
                "mediafile_person",
                "mediafile_owner_person",
                "mediafile_owner_partner",
                "mediafile_creator_person",
                "mediafile_creator_partner",
            ],
            "entities": [
                "type",
                "title",
                "description",
                "coordinates",
                "media_keyword",
                "map_keyword",
                "publication_keyword",
                "language",
                "asset_category",
                "location_type",
                "marine_region",
                "event",
                "project",
                "partner",
                "content_map_type",
                "formal_map_type",
                "date_of_publication",
                "date_of_last_revision",
                "content_date",
                "spatialCoverage",
                "projection",
                "minLocation",
                "maxLocation",
                "formal_document_type",
                "content_document_type",
                "owner_person",
                "owner_partner",
                "creator_person",
                "creator_partner",
            ],
        },
        required_metadata_values={},
        metadata_field_mapping={
            "map_owner": {
                "target": "entities",
                "map_to": "owner",
            },
            "map_creator": {
                "target": "entities",
                "map_to": "creator",
            },
            "mediafile_owner": {
                "target": "mediafiles",
                "map_to": "owner",
            },
            "mediafile_creator": {
                "target": "mediafiles",
                "map_to": "creator",
            },
            "mediafile_person": {
                "target": "mediafiles",
                "map_to": "person",
            },
        },
        include_indexed_field=False,
    )

    return csv_multi_object


# Test CSVMultiObject DIGIPOLIS
def test_basic_csv_digipolis():
    csv_multi_object = init_digipolis_csv_object(sample_basic_csv_digipolis)
    assert csv_multi_object.objects == expected_basic_objects_digipolis


def test_basic_csv_digipolis_with_wrong_values():
    with pytest.raises(InvalidValueException):
        init_digipolis_csv_object(sample_basic_csv_digipolis_wrong_values)


def test_meemoo_csv_digipolis():
    csv_multi_object = init_digipolis_csv_object(sample_meemoo_csv_digipolis)
    assert csv_multi_object.objects == expected_meemoo_objects_digipolis


def test_csv_with_only_an_entity_digipolis():
    csv_multi_object = init_digipolis_csv_object(sample_csv_without_mediafile_digi)
    assert csv_multi_object.objects == expected_only_entities_object_digipolis


# Tests CSVMultiObject VLIZ
def test_basic_csv_vliz():
    csv_multi_object = init_vliz_csv_object(sample_basic_csv_vliz)
    assert csv_multi_object.objects == expected_basic_objects_vliz


def test_basic_csv_digipolis_with_wrong_values():
    with pytest.raises(ColumnNotFoundException):
        init_vliz_csv_object(sample_basic_csv_vliz_missing_values)


def test_multiple_keywords_csv_vliz():
    csv_multi_object = init_vliz_csv_object(sample_multiple_keywords_csv_vliz)
    assert csv_multi_object.objects == expected_multiple_keywords_objects_vliz


def test_csv_with_only_an_entity_vliz():
    csv_multi_object = init_vliz_csv_object(sample_csv_without_mediafile_vliz)
    assert csv_multi_object.objects == expected_only_entities_object_vliz

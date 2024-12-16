mediafile1 = {
    "_id": "2476c1a2-c323-499e-9e28-6d1562296f3f",
    "filename": "41695fc83cdf1af3166dc77cc0d41e7a-bever.jpg",
    "date_created": {"$date": "2024-11-28T08:33:47.479Z"},
    "version": 3,
    "thumbnail_file_location": "/iiif/3/41695fc83cdf1af3166dc77cc0d41e7a-bever.jpg/full/,150/0/default.jpg",
    "original_file_location": "/download/41695fc83cdf1af3166dc77cc0d41e7a-bever.jpg",
    "metadata": [
        {"key": "copyright_color", "value": "green", "lang": "en"},
        {"key": "copyright_color_calculation", "value": "automatic"},
    ],
    "identifiers": [
        "2476c1a2-c323-499e-9e28-6d1562296f3f",
        "41695fc83cdf1af3166dc77cc0d41e7a",
    ],
    "type": "mediafile",
    "sort": {
        "copyright_color": [{"value": "green", "lang": "en"}],
        "copyright_color_calculation": [{"value": "green"}],
    },
    "relations": [
        {
            "key": "c4d1f89a-2858-4dd6-ba6b-cb333b4a5d7f",
            "label": "hasMediafile",
            "type": "belongsTo",
            "is_primary": False,
            "is_primary_thumbnail": False,
            "metadata": [{"key": "order", "value": 2}],
            "sort": {"order": [{"value": 2}]},
        },
        {"key": "0645e24b-5223-4d91-bae1-2e1226033bef", "type": "hasLicense"},
        {"key": "4efa288e-ffa8-4850-bfc9-f942b63867fa", "type": "hasPhotographer"},
        {
            "key": "d067d0c8-f63c-435e-825d-1e4d2ede63f7",
            "label": "hasChild",
            "type": "hasChild",
            "metadata": [{"key": "order", "value": 4}],
            "sort": {"order": [{"value": 4}]},
        },
    ],
    "file_creation_date": {"$date": "2024-11-28T08:33:47.612Z"},
    "original_filename": "bever.jpg",
    "mimetype": "image/jpeg",
    "date_updated": {"$date": "2024-11-28T08:33:47.612Z"},
    "last_editor": "developers@inuits.eu",
    "img_height": 554,
    "img_width": 711,
}


mediafile2 = {
    "_id": "abfa72af-43ac-4c40-b969-0561eae4c122",
    "filename": "0494340e99df984a6a3d8dfd15df74a7-transcode-8w3809mt4j-1.jpg",
    "md5sum": "0494340e99df984a6a3d8dfd15df74a7",
    "transcode_file_location": "/download/0494340e99df984a6a3d8dfd15df74a7-transcode-8w3809mt4j-1.jpg",
    "thumbnail_file_location": "/iiif/3/0494340e99df984a6a3d8dfd15df74a7-transcode-8w3809mt4j-1.jpg/full/,150/0/default.jpg",
    "mimetype": "image/jpeg",
    "metadata": [{"key": "copyright_color", "value": "orange"}],
    "identifiers": [
        "abfa72af-43ac-4c40-b969-0561eae4c122",
        "0494340e99df984a6a3d8dfd15df74a7",
    ],
    "type": "mediafile",
    "sort": {"copyright_color": [{"value": "orange"}]},
    "relations": [
        {
            "key": "c4698e82-c2c3-4dda-b7d7-6d8556d34c6e",
            "label": "hasChild",
            "type": "belongsToParent",
            "metadata": [{"key": "order", "value": 4}],
            "sort": {"order": [{"value": 4}]},
        }
    ],
}

import os
import requests

from .exceptions import NonUniqueException, NotFoundException


class Client:
    def __init__(
        self, elody_collection_url=None, static_jwt=None, extra_headers=None, proxy=None
    ):
        self.elody_collection_url = elody_collection_url or os.environ.get(
            "ELODY_COLLECTION_URL", None
        )
        self.static_jwt = static_jwt or os.environ.get("STATIC_JWT", None)
        self.headers = {"Authorization": f"Bearer {self.static_jwt}"}
        if extra_headers:
            self.headers = {**self.headers, **extra_headers}
        self.proxies = None
        if proxy:
            self.proxies = {
                "https": proxy,
                "http": proxy,
            }

    def __create_mediafile(self, entity_id, mediafile):
        url = f"{self.elody_collection_url}/entities/{entity_id}/mediafiles"
        headers = {**self.headers, **{"Accept": "text/uri-list"}}
        response = requests.post(
            url, json=mediafile, headers=headers, proxies=self.proxies
        )
        return self.__handle_response(response, "Failed to create mediafile", "text")

    def create_mediafile_with_filename(
        self,
        filename,
        technical_origin="original",
        original_filename=None,
        institution_id=None,
    ):
        original_filename = original_filename or filename
        data = {
            "filename": filename,
            "original_filename": original_filename,
            "type": "mediafile",
            "technical_origin": technical_origin,
        }
        if institution_id:
            data.update({"metadata": [{"key": "institution", "value": institution_id}]})
        req = requests.post(
            f"{self.elody_collection_url}/mediafiles",
            json=data,
            headers=self.headers,
            proxies=self.proxies,
        )
        if req.status_code != 201:
            raise Exception(req.text.strip())
        return req

    def create_ticket(self, mediafile_name):
        req = requests.post(
            f"{self.elody_collection_url}/tickets",
            json={"filename": mediafile_name},
            headers=self.headers,
            proxies=self.proxies,
        )
        if req.status_code != 201:
            raise Exception(req.text.strip())
        return req.text.strip().replace('"', "")

    def __get_upload_location(
        self,
        entity_id,
        filename,
        is_public=True,
        identifiers=None,
        mediafile_object=None,
    ):
        if not identifiers:
            identifiers = list()
        if not mediafile_object:
            mediafile_object = dict()
        metadata = []
        if is_public:
            metadata = [
                {
                    "key": "publication_status",
                    "value": "publiek",
                }
            ]
        mediafile = {
            **{
                "filename": filename,
                "metadata": metadata,
                "identifiers": identifiers,
            },
            **mediafile_object,
        }
        return self.__create_mediafile(entity_id, mediafile)

    def __handle_response(self, response, error_message, response_type="json"):
        if response.status_code == 409:
            raise NonUniqueException(response.text.strip())
        if response.status_code == 404:
            raise NotFoundException(response.text.strip())
        if response.status_code not in range(200, 300):
            raise Exception(f"{error_message}: {response.text.strip()}")
        match response_type:
            case "json":
                return response.json()
            case "text":
                return response.text.strip()
            case _:
                return response.json()

    def add_entity_mediafiles(self, identifier, payload):
        url = f"{self.elody_collection_url}/entities/{identifier}/mediafiles"
        response = requests.post(
            url, json=payload, headers=self.headers, proxies=self.proxies
        )
        return self.__handle_response(response, "Failed to add mediafiles")

    def add_object(self, collection, payload, params=None):
        url = f"{self.elody_collection_url}/{collection}"
        response = requests.post(
            url, json=payload, headers=self.headers, params=params, proxies=self.proxies
        )
        return self.__handle_response(response, "Failed to add object")

    def add_object_metadata(self, collection, identifier, payload):
        if collection == "entities":
            url = f"{self.elody_collection_url}/{collection}/{identifier}/metadata"
            payload = payload if isinstance(payload, list) else [payload]
            response = requests.patch(
                url, json=payload, headers=self.headers, proxies=self.proxies
            )
            if response.status_code == 400 and response.json()["message"].endswith(
                "has no metadata"
            ):
                response = requests.post(
                    url, json=payload, headers=self.headers, proxies=self.proxies
                )
            return self.__handle_response(response, "Failed to add metadata")
        else:
            url = f"{self.elody_collection_url}/{collection}/{identifier}"
            payload = {"metadata": payload if isinstance(payload, list) else [payload]}
            response = requests.patch(
                url, json=payload, headers=self.headers, proxies=self.proxies
            )
            return self.__handle_response(response, "Failed to add metadata")

    def delete_object(self, collection, identifier):
        url = f"{self.elody_collection_url}/{collection}/{identifier}"
        response = requests.delete(url, headers=self.headers, proxies=self.proxies)
        return self.__handle_response(response, "Failed to delete object", "text")

    def get_all_objects(self, collection):
        url = f"{self.elody_collection_url}/{collection}"
        response = requests.get(url, headers=self.headers, proxies=self.proxies)
        return self.__handle_response(response, "Failed to get objects")

    def get_mediafiles_and_check_existence(self, mediafile_ids):
        mediafile_image_data = []
        for mediafile_id in mediafile_ids:
            response = self.get_object("mediafiles", mediafile_id)
            mediafile_image_data.append(response)
        return mediafile_image_data

    def get_object(self, collection, identifier):
        url = f"{self.elody_collection_url}/{collection}/{identifier}"
        response = requests.get(url, headers=self.headers, proxies=self.proxies)
        return self.__handle_response(response, "Failed to get object")

    def update_object(self, collection, identifier, payload, overwrite=True):
        url = f"{self.elody_collection_url}/{collection}/{identifier}"
        if overwrite:
            response = requests.put(
                url, json=payload, headers=self.headers, proxies=self.proxies
            )
        else:
            response = requests.patch(
                url, json=payload, headers=self.headers, proxies=self.proxies
            )
        return self.__handle_response(response, "Failed to update object")

    def update_object_relations(self, collection, identifier, payload):
        url = f"{self.elody_collection_url}/{collection}/{identifier}/relations"
        response = requests.patch(
            url, json=payload, headers=self.headers, proxies=self.proxies
        )
        return self.__handle_response(response, "Failed to update object relations")

    def upload_file_from_url(
        self,
        entity_id,
        filename,
        file_url,
        identifiers=None,
        upload_location_replace_map=None,
        mediafile_object=None,
        user_email=None,
    ):
        if not identifiers:
            identifiers = list()
        if not upload_location_replace_map:
            upload_location_replace_map = dict()
        upload_location = self.__get_upload_location(
            entity_id, filename, False, identifiers, mediafile_object
        )
        for current_location, new_location in upload_location_replace_map.items():
            upload_location = upload_location.replace(current_location, new_location)
        upload_location = upload_location.replace('"', "")
        if user_email and "&user_email" not in upload_location:
            upload_location = f"{upload_location}&user_email={user_email}"
        print(upload_location)
        mediafile = requests.get(file_url, proxies=self.proxies).content
        response = requests.post(
            upload_location,
            files={"file": mediafile},
            headers=self.headers,
            proxies=self.proxies,
        )
        return self.__handle_response(response, "Failed to upload mediafile")

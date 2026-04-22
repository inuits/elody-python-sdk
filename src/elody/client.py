import requests

from .exceptions import NonUniqueException, NotFoundException
from hashlib import md5
from os import environ
from requests.exceptions import ConnectionError
from time import sleep
from urllib.parse import urlparse, parse_qs


class Client:
    def __init__(
        self,
        elody_collection_url=None,
        static_jwt=None,
        extra_headers=None,
        proxy=None,
        *,
        elody_storage_api_url=None,
    ):
        self.elody_collection_url = elody_collection_url or environ.get(
            "ELODY_COLLECTION_URL", None
        )
        self.elody_storage_api_url = elody_storage_api_url or environ.get(
            "ELODY_STORAGE_API_URL", None
        )
        self.static_jwt = static_jwt or environ.get("STATIC_JWT", None)
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

        parsed_upload_location = urlparse(upload_location)
        mediafile_id = parse_qs(parsed_upload_location.query).get("id", [None])[0]
        if not mediafile_id:
            raise ValueError(f"Could not extract mediafile_id from {upload_location}")

        response = requests.post(
            f"{self.elody_storage_api_url}/upload/init-stream",
            params={"mediafile_id": mediafile_id},
            headers=self.headers,
            proxies=self.proxies,
        )
        response.raise_for_status()
        stream_info = response.json()

        mediafile_md5sum = md5()
        md5_state = None
        chunks_info = []
        retry_count = 0
        while True:
            exception = None
            try:
                if md5_state is not None:
                    mediafile_md5sum = md5_state.copy()

                response = requests.get(
                    f"{self.elody_storage_api_url}/upload/stream-status",
                    params=stream_info,
                    headers=self.headers,
                    proxies=self.proxies,
                )
                response.raise_for_status()
                existing_chunks = {
                    chunk["sequence_number"]: chunk["hash"]
                    for chunk in response.json().get("uploaded_chunks", [])
                }

                chunk_size = 50 * (1024**2)
                max_uploaded_chunk = (
                    max(existing_chunks.keys()) if existing_chunks else 0
                )
                start_byte = max_uploaded_chunk * chunk_size

                download_headers = {}
                if start_byte > 0 and existing_chunks:
                    download_headers["Range"] = f"bytes={start_byte}-"

                with requests.get(
                    file_url,
                    headers=download_headers,
                    proxies=self.proxies,
                    stream=True,
                    timeout=None,
                ) as mediafile_stream:
                    mediafile_stream.raise_for_status()
                    if mediafile_stream.status_code == 200 and start_byte > 0:
                        print(
                            "Server ignored Range header. Starting download from 0..."
                        )
                        mediafile_md5sum = md5()
                        md5_state = None
                        chunks_info = []
                        start_byte = 0
                        max_uploaded_chunk = 0

                    content_length = int(
                        mediafile_stream.headers.get("Content-Length", 0)
                    )
                    bytes_sent = start_byte
                    for i, chunk in enumerate(
                        mediafile_stream.iter_content(chunk_size=chunk_size),
                        start=max_uploaded_chunk + 1,
                    ):
                        if not chunk:
                            break
                        mediafile_md5sum.update(chunk)
                        bytes_sent += len(chunk)
                        print(
                            f"Progress: {bytes_sent / (1024**2):.2f} MB / {content_length / (1024**2):.2f} MB ({(bytes_sent / content_length) * 100 if content_length > 0 else 0:.2f}%)",
                            end="\r",
                        )
                        if i in existing_chunks:
                            chunks_info.append(
                                {"sequence_number": i, "hash": existing_chunks[i]}
                            )
                            continue

                        response = requests.post(
                            f"{self.elody_storage_api_url}/upload/sign-chunk",
                            json={**stream_info, "chunk_sequence": i},
                            headers=self.headers,
                            proxies=self.proxies,
                        )
                        response.raise_for_status()
                        upload_url = response.json()["upload_url"]

                        response = requests.put(upload_url, data=chunk, timeout=600)
                        response.raise_for_status()
                        etag = response.headers["ETag"]

                        chunks_info.append({"sequence_number": i, "hash": etag})
                        md5_state = mediafile_md5sum.copy()

                response = requests.post(
                    f"{self.elody_storage_api_url}/upload/complete-stream",
                    json={
                        **stream_info,
                        "chunks_info": chunks_info,
                        "file_info": {
                            "md5sum": mediafile_md5sum.hexdigest(),
                            "name": filename,
                        },
                    },
                    headers=self.headers,
                    proxies=self.proxies,
                )
            except ConnectionError as e:
                exception = e
            except (Exception, KeyboardInterrupt) as e:
                retry_count = 4
                exception = e
            if not exception:
                break

            retry_count += 1
            if retry_count >= 4:
                print(f"Failed to upload mediafile: {exception}. Aborting stream...")
                requests.post(
                    f"{self.elody_storage_api_url}/upload/abort-stream",
                    json=stream_info,
                    headers=self.headers,
                    proxies=self.proxies,
                )
                raise exception

            sleep_time = 10**retry_count
            print(
                f"Upload error: {exception}. Retrying in {sleep_time}s... (Attempt {retry_count}/4)"
            )
            sleep(sleep_time)

        print()
        return self.__handle_response(response, "Failed to upload mediafile")

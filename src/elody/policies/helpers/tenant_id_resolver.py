import re as regex

from flask import Request
from storage.storagemanager import StorageManager  # pyright: ignore
from elody.util import get_item_metadata_value


class TenantIdResolver:
    def resolve(self, request):
        endpoints = [
            EntityGetRequest,
            EntityPostRequest,
            EntityDetailGetRequest,
            EntityDetailUpdateRequest,
            EntityDetailDeleteRequest,
            EntityDetailGetRelationsRequest,
            EntityDetailUpdateRelationsRequest,
            EntityDetailCreateRelationsRequest,
            EntityDetailDeleteRelationsRequest,
            EntityDetailGetMetadataRequest,
            EntityDetailUpdateMetadataRequest,
            EntityDetailCreateMetadataRequest,
            EntityDetailGetMediafilesRequest,
            EntityDetailCreateMediafilesRequest,
            MediafileGetRequest,
            MediafilePostRequest,
            MediafileDetailGetRequest,
            MediafileDetailUpdateRequest,
            MediafileDetailDeleteRequest,
            MediafileDetailGetDerivativesRequest,
            MediafileDetailCreateDerivativesRequest,
            MediafileDetailDeleteDerivativesRequest,
        ]

        for endpoint in endpoints:
            tenant_id = endpoint().get_tenant_id(request)
            if tenant_id != None:
                return tenant_id
        return "tenant:super"


class BaseRequest:
    def __init__(self) -> None:
        self.storage = StorageManager().get_db_engine()
        self.super_tenant_id = "tenant:super"
        self.global_types = [
            "language",
            "type",
            "collectionForm",
            "institution",
            "tag",
            "triple",
            "person",
            "externalRecord",
            "verzameling",
        ]

    def _get_tenant_id_from_entity(self, entity_id):
        entity_relations = self.storage.get_collection_item_relations(
            "entities", entity_id
        )
        entity = self.storage.get_item_from_collection_by_id("entities", entity_id)
        if entity_relations:
            for entity_relation in entity_relations:
                if entity_relation.get("type") == "isIn":
                    tenant = self.storage.get_item_from_collection_by_id(
                        "entities", entity_relation.get("key")
                    )
                    return tenant["_id"]
        if entity:
            return f"tenant:{get_item_metadata_value(entity, 'museum')}"

    def _get_tenant_id_from_mediafile(self, mediafile_id):
        mediafile_relations = self.storage.get_collection_item_relations(
            "mediafiles", mediafile_id
        )

        for relation in mediafile_relations:
            if relation.get("type") == "belongsTo":
                return self._get_tenant_id_from_entity(relation.get("key"))

    def _get_tenant_id_from_body(self, item):
        if item["type"] in self.global_types or item["type"] == "museum":
            return "tenant:super"
        museum_id = get_item_metadata_value(item, "museum")
        return f"tenant:{museum_id}"


class EntityGetRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities(?:\?(.*))?$", request.path)
            and request.method == "GET"
        ):
            return self.super_tenant_id
        return None


class EntityPostRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities(?:\?(.*))?$", request.path)
            and request.method == "POST"
        ):
            return self._get_tenant_id_from_body(request.json)
        return None


class EntityDetailGetRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if regex.match(r"/entities/(.+)$", request.path) and request.method == "GET":
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailUpdateRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if regex.match(r"^/entities/(.+)$", request.path) and request.method in [
            "PUT",
            "PATCH",
        ]:
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailDeleteRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/([^/]+)$", request.path)
            and request.method == "DELETE"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailGetRelationsRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/(.+)/relations$", request.path)
            and request.method == "GET"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailUpdateRelationsRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if regex.match(
            r"^/entities/(.+)/relations$", request.path
        ) and request.method in ["PUT", "PATCH"]:
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailCreateRelationsRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/(.+)/relations$", request.path)
            and request.method == "POST"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailDeleteRelationsRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/(.+)/relations$", request.path)
            and request.method == "DELETE"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailGetMetadataRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/(.+)/metadata$", request.path)
            and request.method == "GET"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailUpdateMetadataRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if regex.match(
            r"^/entities/(.+)/metadata$", request.path
        ) and request.method in ["PUT", "PATCH"]:
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailCreateMetadataRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/(.+)/metadata$", request.path)
            and request.method == "POST"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailGetMediafilesRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/(.+)/mediafiles$", request.path)
            and request.method == "GET"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


class EntityDetailCreateMediafilesRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/entities/(.+)/mediafiles$", request.path)
            and request.method == "POST"
        ):
            return self._get_tenant_id_from_entity(request.view_args.get("id"))
        return None


# Mediafiles
class MediafileGetRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/mediafiles(?:\?(.*))?$", request.path)
            and request.method == "GET"
        ):
            return self.super_tenant_id
        return None


class MediafilePostRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/mediafiles(?:\?(.*))?$", request.path)
            and request.method == "POST"
        ):
            return self._get_tenant_id_from_body(request.json)
        return None


class MediafileDetailGetRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/mediafiles/([^/]+)$", request.path)
            and request.method == "GET"
        ):
            return self.super_tenant_id
        return None


class MediafileDetailUpdateRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if regex.match(r"^/mediafiles/(.+)$", request.path) and request.method in [
            "PUT",
            "PATCH",
        ]:
            return self._get_tenant_id_from_mediafile(request.view_args.get("id"))
        return None


class MediafileDetailDeleteRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/mediafiles/(.+)$", request.path)
            and request.method == "DELETE"
        ):
            return self._get_tenant_id_from_mediafile(request.view_args.get("id"))
        return None


class MediafileDetailGetDerivativesRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/mediafiles/(.+)/derivatives$", request.path)
            and request.method == "GET"
        ):
            return self.super_tenant_id
        return None


class MediafileDetailCreateDerivativesRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/mediafiles/(.+)/derivatives$", request.path)
            and request.method == "POST"
        ):
            return self._get_tenant_id_from_mediafile(request.view_args.get("id"))
        return None


class MediafileDetailDeleteDerivativesRequest(BaseRequest):
    def get_tenant_id(self, request: Request) -> str | None:
        if (
            regex.match(r"^/mediafiles/(.+)/derivatives$", request.path)
            and request.method == "DELETE"
        ):
            return self._get_tenant_id_from_mediafile(request.view_args.get("id"))
        return None

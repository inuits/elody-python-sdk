import re as regex

from elody.error_codes import ErrorCode, get_error_code, get_read, get_write
from flask import Request
from flask_restful import abort
from storage.storagemanager import StorageManager  # pyright: ignore
from elody.util import get_item_metadata_value
from elody.exceptions import NotFoundException, NoTenantException


class TenantIdResolver:
    def resolve(self, request, user):
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
                relations = user.get("relations", [])
                if len(relations) < 1:
                    raise NoTenantException(f"User is not attached to a tenant")
                has_tenant_relation = any(
                    relation.get("type") == "hasTenant"
                    and relation.get("key") == tenant_id
                    for relation in relations
                )
                if has_tenant_relation:
                    return tenant_id
                elif len(relations) == 1 and relations[0].get("key") == "tenant:super":
                    return "tenant:super"
                else:
                    return relations[0].get("key")
        return "tenant:super"


class BaseRequest:
    def __init__(self) -> None:
        self.storage = StorageManager().get_db_engine()
        self.super_tenant_id = "tenant:super"
        # TODO refactor this in a more generic way
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
            "arches_record",
            "manufacturer",
            "photographer",
            "creator",
            "assetPart",
            "set",
            "download",
            "license",
            "share_link"
            # TODO Mediafile should have a link to an asset
            "mediafile",
            "savedSearch",
            "original_data",
            "user",
        ]

    def _get_tenant_id_from_entity(self, entity_id):
        entity_relations = self.storage.get_collection_item_relations(
            "entities", entity_id
        )
        entity = self.storage.get_item_from_collection_by_id("entities", entity_id)
        if not entity:
            abort(
                404,
                message=f"{get_error_code(ErrorCode.ITEM_NOT_FOUND, get_read())} | id:{entity_id} - Item with id {entity_id} doesn't exist",
            )
        type = entity.get("type")
        if type in self.global_types:
            return "tenant:super"
        if entity_relations:
            for entity_relation in entity_relations:
                if entity_relation.get("type") == "isIn":
                    tenant = self.storage.get_item_from_collection_by_id(
                        "entities", entity_relation.get("key")
                    )
                    return tenant["_id"]
                if entity_relation.get("type") == "hasInstitution":
                    instution_relations = self.storage.get_collection_item_relations(
                        "entities", entity_relation.get("key")
                    )
                    for institution_relation in instution_relations:
                        if institution_relation.get("type") == "defines":
                            return institution_relation.get("key")

        if entity and get_item_metadata_value(entity, "institution"):
            return f"tenant:{get_item_metadata_value(entity, 'institution')}"
        abort(
            400,
            message=f"{get_error_code(ErrorCode.ENTITY_HAS_NO_TENANT, get_read())} - Entity has no tenant, and is suppose to have one.",
        )

    def _get_tenant_id_from_mediafile(self, mediafile_id):
        mediafile_relations = self.storage.get_collection_item_relations(
            "mediafiles", mediafile_id
        )

        for relation in mediafile_relations:
            if relation.get("type") == "belongsTo":
                return self._get_tenant_id_from_entity(relation.get("key"))

    # TODO Mediafile should have a link to an asset
    def _get_tenant_id_from_body(self, item, soft_call=False):
        if soft_call:
            return "tenant:super"
        if item.get("type") in self.global_types or item.get("type") == "institution":
            return "tenant:super"
        if item.get("type") == "asset":
            for relation in item.get("relations", []):
                if relation.get("type") in [
                    "hasArchesLink",
                    "hasAdlibLink",
                    "hasBrocadeLink",
                ]:
                    return "tenant:super"
        institution_id = get_item_metadata_value(item, "institution")
        if not institution_id:
            for relation in item.get("relations", []):
                if relation.get("type") == "hasInstitution":
                    institution_id = relation.get("key")
        if institution_id:
            return f"tenant:{institution_id}"
        abort(
            400,
            message=f"{get_error_code(ErrorCode.ENTITY_HAS_NO_TENANT, get_read())} - Item in body doesn't have an institution.",
        )


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
            is_soft_call = request.args.get("soft") is not None
            return self._get_tenant_id_from_body(request.json, soft_call=is_soft_call)
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
            is_soft_call = request.args.get("soft") is not None
            return self._get_tenant_id_from_body(request.json, soft_call=is_soft_call)
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

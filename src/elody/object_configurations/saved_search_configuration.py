from elody.object_configurations.elody_configuration import ElodyConfiguration


class SavedSearchConfiguration(ElodyConfiguration):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {"collection": "saved_searches", "collection_history": ""}
        return {**super().crud(), **crud}

    def document_info(self):
        return super().document_info()

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return super().serialization(from_format, to_format)

    def validation(self):
        return super().validation()

class BaseObjectMigrator:
    def __init__(self, *, status, silent=False):
        self._status = status
        self._silent = silent

    @property
    def status(self):
        return self._status

    @property
    def silent(self):
        return self._silent

    def bulk_migrate(self, *, dry_run=False):  # pyright: ignore
        pass

    def lazy_migrate(self, item, *, dry_run=False):  # pyright: ignore
        return item

class DuplicateExternalLinkException(Exception):
    def __init__(self, message, external_id=None):
        super().__init__(message)
        self.external_id = external_id


class DuplicateFileException(Exception):
    def __init__(self, message, filename=None, md5sum=None):
        super().__init__(message)
        self.message = message
        self.filename = filename
        self.md5sum = md5sum


class ColumnNotFoundException(Exception):
    pass


class FileNotFoundException(Exception):
    pass


class IncorrectTypeException(Exception):
    pass


class InvalidExtensionException(Exception):
    pass


class InvalidObjectException(Exception):
    pass


class InvalidValueException(Exception):
    pass


class NoMediafilesException(Exception):
    pass


class NonUniqueException(Exception):
    pass


class NotFoundException(Exception):
    pass


class NoTenantException(Exception):
    pass


class UnsupportedVersionException(Exception):
    pass

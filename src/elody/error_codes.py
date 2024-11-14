from enum import Enum


class ErrorCode(Enum):
    READ = "R"
    WRITE = "W"

    # General error codes
    UNKNOWN_ERROR = "0002"
    COLLECTION_NOT_FOUND = "0003"
    ITEM_NOT_FOUND = "0004"
    HISTORY_ITEM_NOT_FOUND = "0005"
    CANNOT_SPECIFY_BOTH = "0006"
    ALREADY_PARENT = "0007"
    METADATA_KEY_UNDEFINED = "0008"
    ENTITY_HAS_NO_TENANT = "0009"
    MEDIAFILE_NOT_FOUND = "0010"

    # Auth error codes
    INVALID_CREDENTIALS = "1001"
    ACCOUNT_LOCKED = "1002"
    INSUFFICIENT_PERMISSIONS = "1003"
    NO_PERMISSIONS = "1004"
    UNDEFINED_COLLECTION_RESOLVER = "1005"
    TENANT_NOT_FOUND = "1006"
    XTENANT_HAS_NO_TENANT_DEFENING_ENTITY = "1007"

    # Database error codes
    DATABASE_NOT_INITIALIZED = "2000"
    DATABASE_CONNECTION_FAILED = "2001"
    QUERY_EXECUTION_FAILED = "2002"
    DUPLICATE_ENTRY = "2003"

    # Network error codes
    NETWORK_UNAVAILABLE = "3001"
    TIMEOUT = "3002"
    SERVER_NOT_FOUND = "3003"

    # File handling error codes
    FILE_NOT_FOUND = "4001"
    FILE_ACCESS_DENIED = "4002"
    FILE_CORRUPTED = "4003"
    NO_FILENAME_SPECIFIED = "4004"
    NO_TICKET_ID_SPECIFIED = "4005"
    TICKET_NOT_FOUND = "4006"
    TICKET_EXPIRED = "4007"
    PROVIDE_MEDIAFILE_ID_OR_TICKET_ID = "4008"
    DUPLICATE_FILE = "4009"
    NO_BUCKET_SPECIFIED = "4010"

    # Validation error codes
    INVALID_INPUT = "5001"
    REQUIRED_FIELD_MISSING = "5002"
    INVALID_FORMAT = "5003"
    INVALID_TYPE = "5004"
    COLUMN_NOT_FOUND = "5005"
    ONLY_TYPE_CSV_ALLOWED = "5006"
    NO_METADATA_AVAILABLE = "5007"
    INVALID_DATETIME = "5008"
    UNSUPPORTED_TYPE = "5009"
    CONTENT_NOT_FOUND = "5010"
    VALIDATION_ERROR = "5011"

    # Filter error codes
    NO_MATCHER_FOR_FILTER_REQUEST = "6001"
    UNDEFINED_FILTER_FOR_INPUT_TYPE = "6002"
    UNSUPPORTED_OPERATOR = "6003"

    # Migration error codes
    UNABLE_TO_UPDATE_SCHEMA_VERSION = "7001"
    LAZY_MIGRATION_SCHEMA_TYPE_MISMATCH = "7002"

    # External Services
    SERVICE_UNAVAILABLE = "8001"

    # Arches error codes
    ARCHES_ERROR = "11000"
    ARCHES_CONNECTION_UNAVAILABLE = "11001"
    ARCHES_RECORD_NOT_FOUND = "11002"


def get_error_code(error_code, prefix):
    if prefix not in [ErrorCode.READ.value, ErrorCode.WRITE.value]:
        raise ValueError("Prefix must be 'R' for read or 'W' for write.")
    return f"{prefix}{error_code.value}"


def get_read():
    return ErrorCode.READ.value


def get_write():
    return ErrorCode.WRITE.value

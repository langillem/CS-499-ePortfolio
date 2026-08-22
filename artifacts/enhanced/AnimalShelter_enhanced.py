################################################################################
# AnimalShelter.py
#
# Description:
#     Production-oriented MongoDB data-access layer for the AAC animal shelter
#     database. The module provides secure configuration, connection verification,
#     CRUD operations, validation, pagination, sorting, streaming, soft deletion,
#     audit fields, structured operation results, index management, and optional
#     Pandas DataFrame output for analytics.
#
# Dependencies:
#     - pymongo
#     - pandas (optional; required only for read_dataframe)
#
# Author: Michael Langille
# Original Course: CS-340 Client/Server Development
# Enhanced for: CS-499 Computer Science Capstone
# Version: 2.0
################################################################################

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote_plus

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
    ConfigurationError,
    DuplicateKeyError,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 27017
DEFAULT_AUTH_SOURCE = "admin"
DEFAULT_DATABASE = "aac"
DEFAULT_COLLECTION = "animals"
DEFAULT_TIMEOUT_MS = 5_000
DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 500

SortSpec = Sequence[Tuple[str, int]]


class AnimalShelterError(Exception):
    """Base exception for AnimalShelter errors."""


class DatabaseConnectionError(AnimalShelterError):
    """Raised when a MongoDB connection cannot be established."""


class DataValidationError(AnimalShelterError):
    """Raised when input data fails validation."""


class DatabaseOperationError(AnimalShelterError):
    """Raised when a MongoDB operation fails."""


@dataclass(frozen=True)
class OperationResult:
    """Structured result returned by detailed write operations."""

    success: bool
    message: str
    inserted_id: Optional[str] = None
    matched_count: int = 0
    modified_count: int = 0
    deleted_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Return the result as a plain dictionary."""
        return asdict(self)


class AnimalShelter:
    """
    MongoDB data-access object for the AAC animals collection.

    The class keeps the original CRUD interface for compatibility while adding
    detailed result methods, secure configuration, query controls, connection
    lifecycle management, and analytics-oriented output.
    """

    def __init__(
        self,
        username: str = "",
        password: str = "",
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        auth_source: str = DEFAULT_AUTH_SOURCE,
        db_name: str = DEFAULT_DATABASE,
        collection_name: str = DEFAULT_COLLECTION,
        tls: bool = False,
        *,
        uri: Optional[str] = None,
        use_srv: bool = False,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        verify_connection: bool = True,
        required_fields: Optional[Sequence[str]] = None,
        audit_fields: bool = True,
    ) -> None:
        """
        Establish a MongoDB connection and select the target collection.

        Args:
            username: MongoDB username. Prefer environment-based configuration.
            password: MongoDB password. Prefer environment-based configuration.
            host: MongoDB host.
            port: MongoDB port. Ignored for SRV connections.
            auth_source: Authentication database.
            db_name: Application database name.
            collection_name: MongoDB collection name.
            tls: Enable TLS for a standard MongoDB connection.
            uri: Complete MongoDB URI. When supplied, it takes precedence.
            use_srv: Use the mongodb+srv URI scheme.
            timeout_ms: Server-selection and connection timeout in milliseconds.
            verify_connection: Ping MongoDB during initialization.
            required_fields: Fields required for newly created documents.
            audit_fields: Automatically maintain created/updated timestamps.

        Raises:
            DataValidationError: If configuration values are invalid.
            DatabaseConnectionError: If MongoDB cannot be reached.
        """
        if not isinstance(port, int) or port <= 0:
            raise DataValidationError("port must be a positive integer")
        if not isinstance(timeout_ms, int) or timeout_ms <= 0:
            raise DataValidationError("timeout_ms must be a positive integer")
        if not db_name or not collection_name:
            raise DataValidationError("db_name and collection_name are required")
        if use_srv and port != DEFAULT_PORT:
            LOGGER.debug("The port value is ignored for an SRV connection.")

        self._required_fields = tuple(required_fields or ())
        self._audit_fields = bool(audit_fields)
        self._closed = False

        connection_uri = uri or self._build_uri(
            username=username,
            password=password,
            host=host,
            port=port,
            auth_source=auth_source,
            tls=tls,
            use_srv=use_srv,
        )

        try:
            self.client: MongoClient[Dict[str, Any]] = MongoClient(
                connection_uri,
                tls=tls if not use_srv else None,
                serverSelectionTimeoutMS=timeout_ms,
                connectTimeoutMS=timeout_ms,
                appname="CS499-AnimalShelter",
            )
            self.database: Database[Dict[str, Any]] = self.client[db_name]
            self.collection: Collection[Dict[str, Any]] = self.database[
                collection_name
            ]

            if verify_connection:
                self.client.admin.command("ping")

            LOGGER.info(
                "Connected to MongoDB database '%s', collection '%s'.",
                db_name,
                collection_name,
            )
        except (
            ConfigurationError,
            OperationFailure,
            ServerSelectionTimeoutError,
            PyMongoError,
        ) as exc:
            LOGGER.exception("MongoDB connection failed.")
            raise DatabaseConnectionError(
                "Failed to connect to MongoDB. Check the server, credentials, "
                "authentication source, TLS settings, and network access."
            ) from exc

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "MONGO_",
        verify_connection: bool = True,
        required_fields: Optional[Sequence[str]] = None,
        audit_fields: bool = True,
    ) -> "AnimalShelter":
        """
        Create an instance from environment variables.

        Recognized variables:
            MONGO_URI
            MONGO_USERNAME
            MONGO_PASSWORD
            MONGO_HOST
            MONGO_PORT
            MONGO_AUTH_SOURCE
            MONGO_DB_NAME
            MONGO_COLLECTION_NAME
            MONGO_TLS
            MONGO_USE_SRV
            MONGO_TIMEOUT_MS
        """
        def env(name: str, default: Optional[str] = None) -> Optional[str]:
            return os.getenv(f"{prefix}{name}", default)

        def as_bool(value: Optional[str], default: bool = False) -> bool:
            if value is None:
                return default
            return value.strip().lower() in {"1", "true", "yes", "on"}

        try:
            port = int(env("PORT", str(DEFAULT_PORT)) or DEFAULT_PORT)
            timeout_ms = int(
                env("TIMEOUT_MS", str(DEFAULT_TIMEOUT_MS)) or DEFAULT_TIMEOUT_MS
            )
        except ValueError as exc:
            raise DataValidationError(
                "MONGO_PORT and MONGO_TIMEOUT_MS must be integers"
            ) from exc

        return cls(
            username=env("USERNAME", "") or "",
            password=env("PASSWORD", "") or "",
            host=env("HOST", DEFAULT_HOST) or DEFAULT_HOST,
            port=port,
            auth_source=env("AUTH_SOURCE", DEFAULT_AUTH_SOURCE)
            or DEFAULT_AUTH_SOURCE,
            db_name=env("DB_NAME", DEFAULT_DATABASE) or DEFAULT_DATABASE,
            collection_name=env("COLLECTION_NAME", DEFAULT_COLLECTION)
            or DEFAULT_COLLECTION,
            tls=as_bool(env("TLS"), False),
            uri=env("URI"),
            use_srv=as_bool(env("USE_SRV"), False),
            timeout_ms=timeout_ms,
            verify_connection=verify_connection,
            required_fields=required_fields,
            audit_fields=audit_fields,
        )

    @staticmethod
    def _build_uri(
        *,
        username: str,
        password: str,
        host: str,
        port: int,
        auth_source: str,
        tls: bool,
        use_srv: bool,
    ) -> str:
        """Build a MongoDB URI without logging credentials."""
        if not host:
            raise DataValidationError("host is required")

        credentials = ""
        if username or password:
            if not username or not password:
                raise DataValidationError(
                    "username and password must be provided together"
                )
            credentials = f"{quote_plus(username)}:{quote_plus(password)}@"

        scheme = "mongodb+srv" if use_srv else "mongodb"
        location = host if use_srv else f"{host}:{port}"
        options = [f"authSource={quote_plus(auth_source)}"]
        if tls and not use_srv:
            options.append("tls=true")

        return f"{scheme}://{credentials}{location}/?{'&'.join(options)}"

    def __enter__(self) -> "AnimalShelter":
        """Return this instance for use in a with statement."""
        self._ensure_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """Close the MongoDB client when leaving a with statement."""
        self.close()

    def close(self) -> None:
        """Close the MongoDB client safely."""
        if not self._closed:
            self.client.close()
            self._closed = True
            LOGGER.info("MongoDB connection closed.")

    def health_check(self) -> bool:
        """Return True when MongoDB responds to a ping command."""
        self._ensure_open()
        try:
            return self.client.admin.command("ping").get("ok") == 1.0
        except PyMongoError as exc:
            LOGGER.warning("MongoDB health check failed: %s", exc)
            return False

    def _ensure_open(self) -> None:
        if self._closed:
            raise DatabaseConnectionError("The MongoDB connection is closed")

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _validate_mapping(
        value: Mapping[str, Any],
        *,
        name: str,
        allow_empty: bool = False,
    ) -> Dict[str, Any]:
        if not isinstance(value, Mapping):
            raise DataValidationError(f"{name} must be a mapping")
        result = dict(value)
        if not allow_empty and not result:
            raise DataValidationError(f"{name} cannot be empty")
        return result

    @classmethod
    def _contains_forbidden_operator(cls, value: Any) -> bool:
        """Reject server-side JavaScript operators in external query input."""
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if key in {"$where", "$function", "$accumulator"}:
                    return True
                if cls._contains_forbidden_operator(nested):
                    return True
        elif isinstance(value, (list, tuple)):
            return any(cls._contains_forbidden_operator(item) for item in value)
        return False

    def _validate_query(
        self,
        query: Optional[Mapping[str, Any]],
        *,
        allow_empty: bool,
    ) -> Dict[str, Any]:
        result = self._validate_mapping(
            query or {}, name="query", allow_empty=allow_empty
        )
        if self._contains_forbidden_operator(result):
            raise DataValidationError(
                "query contains a prohibited server-side JavaScript operator"
            )
        return result

    def _validate_document(self, document: Mapping[str, Any]) -> Dict[str, Any]:
        result = self._validate_mapping(document, name="document")
        missing = [
            field
            for field in self._required_fields
            if field not in result or result[field] in (None, "")
        ]
        if missing:
            raise DataValidationError(
                f"document is missing required fields: {', '.join(missing)}"
            )
        if self._contains_forbidden_operator(result):
            raise DataValidationError(
                "document contains a prohibited server-side JavaScript operator"
            )
        return result

    @staticmethod
    def _validate_sort(sort: Optional[SortSpec]) -> Optional[List[Tuple[str, int]]]:
        if sort is None:
            return None

        validated: List[Tuple[str, int]] = []
        for item in sort:
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                raise DataValidationError(
                    "sort must contain (field, direction) pairs"
                )
            field, direction = item
            if not isinstance(field, str) or not field:
                raise DataValidationError("sort field must be a nonempty string")
            if direction not in (ASCENDING, -1):
                raise DataValidationError(
                    "sort direction must be ASCENDING/1 or DESCENDING/-1"
                )
            validated.append((field, int(direction)))
        return validated

    @staticmethod
    def _pagination_values(
        *,
        limit: Optional[int],
        page: Optional[int],
        page_size: Optional[int],
        skip: int,
    ) -> Tuple[int, Optional[int]]:
        if not isinstance(skip, int) or skip < 0:
            raise DataValidationError("skip must be a nonnegative integer")
        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise DataValidationError("limit must be a positive integer")

        if page is None and page_size is None:
            bounded_limit = min(limit, MAX_PAGE_SIZE) if limit else None
            return skip, bounded_limit

        effective_page = 1 if page is None else page
        effective_page_size = (
            DEFAULT_PAGE_SIZE if page_size is None else page_size
        )

        if not isinstance(effective_page, int) or effective_page < 1:
            raise DataValidationError("page must be an integer greater than zero")
        if (
            not isinstance(effective_page_size, int)
            or effective_page_size < 1
            or effective_page_size > MAX_PAGE_SIZE
        ):
            raise DataValidationError(
                f"page_size must be between 1 and {MAX_PAGE_SIZE}"
            )

        calculated_skip = skip + (effective_page - 1) * effective_page_size
        effective_limit = (
            min(limit, effective_page_size) if limit else effective_page_size
        )
        return calculated_skip, effective_limit

    # -------------------------------------------------------------------------
    # CREATE
    # -------------------------------------------------------------------------
    def create(self, document: Mapping[str, Any]) -> bool:
        """Insert one document and return True when acknowledged."""
        try:
            return self.create_detailed(document).success
        except AnimalShelterError as exc:
            LOGGER.error("Create failed: %s", exc)
            return False

    def create_detailed(self, document: Mapping[str, Any]) -> OperationResult:
        """Insert one validated document and return structured metadata."""
        self._ensure_open()
        new_document = self._validate_document(document)

        if self._audit_fields:
            now = self._utc_now()
            new_document.setdefault("created_at", now)
            new_document.setdefault("updated_at", now)
            new_document.setdefault("is_deleted", False)

        try:
            result = self.collection.insert_one(new_document)
            return OperationResult(
                success=bool(result.acknowledged),
                message="Document inserted successfully.",
                inserted_id=str(result.inserted_id),
            )
        except DuplicateKeyError as exc:
            raise DataValidationError(
                "A document with the same unique key already exists"
            ) from exc
        except PyMongoError as exc:
            LOGGER.exception("MongoDB insert failed.")
            raise DatabaseOperationError("Unable to create the document") from exc

    # -------------------------------------------------------------------------
    # READ
    # -------------------------------------------------------------------------
    def read(
        self,
        query: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, int]] = None,
        limit: Optional[int] = None,
        *,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        skip: int = 0,
        sort: Optional[SortSpec] = None,
        include_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve matching documents with pagination, sorting, and projection.

        For backward compatibility, calling read(query, projection, limit) still
        returns a list. Pagination is activated when page or page_size is supplied.
        """
        self._ensure_open()
        validated_query = self._validate_query(query, allow_empty=True)
        validated_projection = (
            self._validate_mapping(
                projection, name="projection", allow_empty=True
            )
            if projection is not None
            else None
        )
        validated_sort = self._validate_sort(sort)
        effective_skip, effective_limit = self._pagination_values(
            limit=limit,
            page=page,
            page_size=page_size,
            skip=skip,
        )

        if not include_deleted and "is_deleted" not in validated_query:
            validated_query["is_deleted"] = {"$ne": True}

        try:
            cursor = self.collection.find(
                validated_query,
                validated_projection,
            ).skip(effective_skip)

            if validated_sort:
                cursor = cursor.sort(validated_sort)
            if effective_limit:
                cursor = cursor.limit(effective_limit)

            return list(cursor)
        except PyMongoError as exc:
            LOGGER.exception("MongoDB read failed.")
            raise DatabaseOperationError("Unable to read documents") from exc

    def stream(
        self,
        query: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, int]] = None,
        *,
        batch_size: int = 100,
        sort: Optional[SortSpec] = None,
        include_deleted: bool = False,
    ) -> Iterator[Dict[str, Any]]:
        """Yield matching documents without loading the entire result into memory."""
        self._ensure_open()
        if not isinstance(batch_size, int) or batch_size < 1:
            raise DataValidationError("batch_size must be a positive integer")

        validated_query = self._validate_query(query, allow_empty=True)
        validated_projection = (
            self._validate_mapping(
                projection, name="projection", allow_empty=True
            )
            if projection is not None
            else None
        )
        validated_sort = self._validate_sort(sort)

        if not include_deleted and "is_deleted" not in validated_query:
            validated_query["is_deleted"] = {"$ne": True}

        try:
            cursor = self.collection.find(
                validated_query,
                validated_projection,
            ).batch_size(batch_size)
            if validated_sort:
                cursor = cursor.sort(validated_sort)
            yield from cursor
        except PyMongoError as exc:
            LOGGER.exception("MongoDB streaming read failed.")
            raise DatabaseOperationError("Unable to stream documents") from exc

    def read_dataframe(
        self,
        query: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, int]] = None,
        limit: Optional[int] = None,
        *,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        skip: int = 0,
        sort: Optional[SortSpec] = None,
        include_deleted: bool = False,
        stringify_object_ids: bool = True,
    ) -> Any:
        """
        Return query results as a Pandas DataFrame for vectorized analysis.

        Pandas is imported lazily so CRUD use does not require the dependency.
        Result limits and pagination should be used for large collections.
        """
        try:
            import pandas as pd
        except ImportError as exc:
            raise DatabaseOperationError(
                "Pandas is required for read_dataframe. "
                "Install it with: pip install pandas"
            ) from exc

        records = self.read(
            query=query,
            projection=projection,
            limit=limit,
            page=page,
            page_size=page_size,
            skip=skip,
            sort=sort,
            include_deleted=include_deleted,
        )
        dataframe = pd.DataFrame.from_records(records)

        if (
            stringify_object_ids
            and not dataframe.empty
            and "_id" in dataframe.columns
        ):
            dataframe["_id"] = dataframe["_id"].astype(str)

        return dataframe

    def count(
        self,
        query: Optional[Mapping[str, Any]] = None,
        *,
        include_deleted: bool = False,
    ) -> int:
        """Count documents matching a validated query."""
        self._ensure_open()
        validated_query = self._validate_query(query, allow_empty=True)
        if not include_deleted and "is_deleted" not in validated_query:
            validated_query["is_deleted"] = {"$ne": True}
        try:
            return int(self.collection.count_documents(validated_query))
        except PyMongoError as exc:
            raise DatabaseOperationError("Unable to count documents") from exc

    # -------------------------------------------------------------------------
    # UPDATE
    # -------------------------------------------------------------------------
    def update(
        self,
        query: Mapping[str, Any],
        update_values: Mapping[str, Any],
        many: bool = False,
    ) -> int:
        """Update documents and return the number modified."""
        try:
            return self.update_detailed(
                query=query,
                update_values=update_values,
                many=many,
            ).modified_count
        except AnimalShelterError as exc:
            LOGGER.error("Update failed: %s", exc)
            return 0

    def update_detailed(
        self,
        query: Mapping[str, Any],
        update_values: Mapping[str, Any],
        many: bool = False,
    ) -> OperationResult:
        """Update one or many documents and return matched/modified counts."""
        self._ensure_open()
        validated_query = self._validate_query(query, allow_empty=False)
        validated_update = self._validate_mapping(
            update_values, name="update_values"
        )

        has_operator = any(
            isinstance(key, str) and key.startswith("$")
            for key in validated_update
        )
        update_document = (
            dict(validated_update)
            if has_operator
            else {"$set": dict(validated_update)}
        )

        if self._audit_fields:
            update_document.setdefault("$set", {})
            if not isinstance(update_document["$set"], dict):
                raise DataValidationError("$set must contain a mapping")
            update_document["$set"]["updated_at"] = self._utc_now()

        try:
            result = (
                self.collection.update_many(validated_query, update_document)
                if many
                else self.collection.update_one(validated_query, update_document)
            )
            matched = int(result.matched_count or 0)
            modified = int(result.modified_count or 0)
            message = (
                "Documents updated successfully."
                if modified
                else (
                    "Documents matched, but no values changed."
                    if matched
                    else "No documents matched the query."
                )
            )
            return OperationResult(
                success=bool(result.acknowledged),
                message=message,
                matched_count=matched,
                modified_count=modified,
            )
        except PyMongoError as exc:
            LOGGER.exception("MongoDB update failed.")
            raise DatabaseOperationError("Unable to update documents") from exc

    # -------------------------------------------------------------------------
    # DELETE
    # -------------------------------------------------------------------------
    def delete(self, query: Mapping[str, Any], many: bool = False) -> int:
        """
        Permanently delete documents and return the number deleted.

        For safer record retention, prefer soft_delete().
        """
        try:
            return self.delete_detailed(query=query, many=many).deleted_count
        except AnimalShelterError as exc:
            LOGGER.error("Delete failed: %s", exc)
            return 0

    def delete_detailed(
        self,
        query: Mapping[str, Any],
        many: bool = False,
    ) -> OperationResult:
        """Permanently delete one or many documents."""
        self._ensure_open()
        validated_query = self._validate_query(query, allow_empty=False)

        try:
            result = (
                self.collection.delete_many(validated_query)
                if many
                else self.collection.delete_one(validated_query)
            )
            deleted = int(result.deleted_count or 0)
            return OperationResult(
                success=bool(result.acknowledged),
                message=(
                    "Documents permanently deleted."
                    if deleted
                    else "No documents matched the query."
                ),
                deleted_count=deleted,
            )
        except PyMongoError as exc:
            LOGGER.exception("MongoDB delete failed.")
            raise DatabaseOperationError(
                "Unable to permanently delete documents"
            ) from exc

    def soft_delete(
        self,
        query: Mapping[str, Any],
        *,
        many: bool = False,
        deleted_by: Optional[str] = None,
    ) -> OperationResult:
        """Mark records as deleted while preserving them for audit/history."""
        values: Dict[str, Any] = {
            "is_deleted": True,
            "deleted_at": self._utc_now(),
        }
        if deleted_by:
            values["deleted_by"] = deleted_by
        return self.update_detailed(query, {"$set": values}, many=many)

    def restore(
        self,
        query: Mapping[str, Any],
        *,
        many: bool = False,
    ) -> OperationResult:
        """Restore soft-deleted records."""
        validated_query = self._validate_query(query, allow_empty=False)
        validated_query["is_deleted"] = True
        return self.update_detailed(
            validated_query,
            {
                "$set": {
                    "is_deleted": False,
                    "restored_at": self._utc_now(),
                },
                "$unset": {
                    "deleted_at": "",
                    "deleted_by": "",
                },
            },
            many=many,
        )

    # -------------------------------------------------------------------------
    # INDEX MANAGEMENT
    # -------------------------------------------------------------------------
    def ensure_indexes(
        self,
        *,
        unique_animal_id: bool = False,
        include_common_indexes: bool = True,
    ) -> List[str]:
        """
        Create indexes that support common AAC query patterns.

        Set unique_animal_id=True only after confirming existing data contains
        no duplicate animal_id values.
        """
        self._ensure_open()
        created: List[str] = []

        try:
            if unique_animal_id:
                created.append(
                    self.collection.create_index(
                        [("animal_id", ASCENDING)],
                        unique=True,
                        name="uq_animal_id",
                    )
                )

            if include_common_indexes:
                created.extend(
                    [
                        self.collection.create_index(
                            [("animal_type", ASCENDING)],
                            name="ix_animal_type",
                        ),
                        self.collection.create_index(
                            [("breed", ASCENDING)],
                            name="ix_breed",
                        ),
                        self.collection.create_index(
                            [
                                ("animal_type", ASCENDING),
                                ("outcome_type", ASCENDING),
                            ],
                            name="ix_animal_type_outcome_type",
                        ),
                        self.collection.create_index(
                            [("is_deleted", ASCENDING)],
                            name="ix_is_deleted",
                        ),
                    ]
                )
            return created
        except PyMongoError as exc:
            LOGGER.exception("MongoDB index creation failed.")
            raise DatabaseOperationError("Unable to create indexes") from exc


__all__ = [
    "AnimalShelter",
    "AnimalShelterError",
    "DatabaseConnectionError",
    "DatabaseOperationError",
    "DataValidationError",
    "OperationResult",
]

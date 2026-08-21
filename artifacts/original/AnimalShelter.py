################################################################################
# AnimalShelter.py
#
# Description:
#     This module provides a Python interface for performing CRUD (Create, Read,
#     Update, Delete) operations on an animal shelter database stored in MongoDB.
#     It encapsulates database connection logic and provides a clean API for
#     managing animal records in the AAC (Animal Adoption Center) database.
#
# Database Configuration:
#     - Database: aac
#     - Collection: animals
#     - Host: localhost
#     - Port: 27017
#     - Authentication: Required (username/password)
#
# Features:
#     - Automatic connection management to MongoDB
#     - Full CRUD operations with error handling
#     - Query-based search, update, and delete operations
#     - Prevents invalid or unsafe operations through validation
#
# Usage Example:
#     from AnimalShelter import AnimalShelter
#
#     # Initialize connection
#     shelter = AnimalShelter("your_username", "your_password")
#
#     # Create a new animal record
#     new_animal = {
#         "name": "Frank",
#         "animal_type": "Dog",
#         "breed": "Golden Retriever",
#         "age_upon_outcome": "6 years"
#     }
#     shelter.create(new_animal)
#
#     # Read animals
#     dogs = shelter.read({"animal_type": "Dog"})
#
#     # Update records
#     shelter.update({"name": "Frank"}, {"$set": {"age_upon_outcome": "7 years"}})
#
#     # Delete records
#     shelter.delete({"name": "Frank"})
#
# Dependencies:
#     - pymongo: MongoDB driver for Python
#
# Author: Michael Langille
# Date: 2025-10-08
# Version: 1.0
# Course: CS-340: Client/Server Development
#
# Notes:
#     - Ensure MongoDB is running before using this class.
#     - The aac database and animals collection must exist.
#     - User credentials must have read/write permissions.
################################################################################

from typing import Any, Dict, List, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError


class AnimalShelter:
    """
    A class providing CRUD operations for the AAC (Animal Adoption Center) animals
    collection in MongoDB. This class encapsulates all database logic, ensuring
    consistent connection handling and structured CRUD functionality.
    """

    def __init__(
        self,
        username: str = "",
        password: str = "",
        host: str = "localhost",
        port: int = 27017,
        auth_source: str = "admin",
        db_name: str = "aac",
        collection_name: str = "animals",
        tls: bool = False
    ) -> None:
        """
        Constructor: Establishes a connection to MongoDB and selects the target
        database and collection.

        Args:
            username (str): MongoDB username.
            password (str): MongoDB password.
            host (str): MongoDB host (default: 'localhost').
            port (int): MongoDB port (default: 27017).
            auth_source (str): Authentication database (default: 'admin').
            db_name (str): Database name (default: 'aac').
            collection_name (str): Collection name (default: 'animals').
            tls (bool): Use TLS/SSL if required (default: False).

        Raises:
            RuntimeError: If MongoDB connection fails.
        """
        try:
            # Build the MongoDB connection string
            scheme = "mongodb+srv" if tls else "mongodb"
            uri = f"{scheme}://{username}:{password}@{host}:{port}/?authSource={auth_source}"

            # Connect to MongoDB
            self.client = MongoClient(uri, tls=tls)

            # Select database and collection
            self.database = self.client[db_name]
            self.collection = self.database[collection_name]

        except PyMongoError as exc:
            # Handle and report connection errors
            raise RuntimeError(f"Failed to connect to MongoDB: {exc}") from exc

    # -------------------------------------------------------------------------
    # CREATE
    # -------------------------------------------------------------------------
    def create(self, document: Dict[str, Any]) -> bool:
        """
        Inserts a new document into the MongoDB collection.

        Args:
            document (dict): The document to insert, represented as key/value pairs.

        Returns:
            bool: True if the insert succeeds, False otherwise.
        """
        # Validate that the document is a non-empty dictionary
        if not isinstance(document, dict) or not document:
            return False

        try:
            # Insert document into the collection
            result = self.collection.insert_one(document)

            # Return True if MongoDB acknowledged the operation
            return bool(result.acknowledged)

        except PyMongoError as exc:
            # Log and handle any insertion errors
            print(f"Create error: {exc}")
            return False

    # -------------------------------------------------------------------------
    # READ
    # -------------------------------------------------------------------------
    def read(
        self,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves documents from the MongoDB collection based on the provided query.

        Args:
            query (dict, optional): MongoDB-style query filter (default: {} to return all).
            projection (dict, optional): Specifies which fields to include/exclude.
            limit (int, optional): Maximum number of documents to return.

        Returns:
            list: A list of documents matching the query. Returns an empty list if
                  no documents are found or if an error occurs.
        """
        try:
            # Perform the find() query on the collection
            cursor = self.collection.find(query or {}, projection or {})

            # Apply limit if specified
            if isinstance(limit, int) and limit > 0:
                cursor = cursor.limit(limit)

            # Convert the cursor to a list and return
            return list(cursor)

        except PyMongoError as exc:
            print(f"Read error: {exc}")
            return []

    # -------------------------------------------------------------------------
    # UPDATE
    # -------------------------------------------------------------------------
    def update(
        self,
        query: Dict[str, Any],
        update_values: Dict[str, Any],
        many: bool = False
    ) -> int:
        """
        Updates one or more documents in the MongoDB collection that match the query.

        Args:
            query (dict): The filter to locate documents.
            update_values (dict): The values to update.
            many (bool): If True, updates all matching documents; otherwise updates one.

        Returns:
            int: The number of documents modified.
        """
        # Validate query and update data
        if not isinstance(query, dict) or not query:
            return 0
        if not isinstance(update_values, dict) or not update_values:
            return 0

        try:
            # Determine if an update operator ($set, etc.) is included
            has_operator = any(k.startswith("$") for k in update_values.keys())
            update_doc = update_values if has_operator else {"$set": update_values}

            # Execute update depending on 'many' flag
            if many:
                result = self.collection.update_many(query, update_doc)
            else:
                result = self.collection.update_one(query, update_doc)

            # Return the number of modified documents
            return int(result.modified_count or 0)

        except PyMongoError as exc:
            print(f"Update error: {exc}")
            return 0

    # -------------------------------------------------------------------------
    # DELETE
    # -------------------------------------------------------------------------
    def delete(self, query: Dict[str, Any], many: bool = False) -> int:
        """
        Deletes one or more documents from the MongoDB collection that match the query.

        Args:
            query (dict): The filter to locate documents for deletion.
            many (bool): If True, deletes all matching documents; otherwise deletes one.

        Returns:
            int: The number of documents deleted.
        """
        # Validate the query before attempting deletion
        if not isinstance(query, dict) or not query:
            return 0

        try:
            # Perform deletion depending on 'many' flag
            if many:
                result = self.collection.delete_many(query)
            else:
                result = self.collection.delete_one(query)

            # Return count of deleted documents
            return int(result.deleted_count or 0)

        except PyMongoError as exc:
            print(f"Delete error: {exc}")
            return 0

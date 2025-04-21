"""Schema migrator for handling document schema migrations."""

from typing import Dict, Any, Callable, Optional
from .schema_registry import DocumentType, SchemaRegistry
import logging

class SchemaMigrationError(Exception):
    """Exception raised when schema migration fails."""
    pass

class SchemaMigrator:
    """Handles migration of documents between schema versions."""

    # Migration functions for inventory documents
    _inventory_migrations: Dict[str, Dict[str, Callable]] = {
        # Example migration from v1 to v2:
        # "v1_to_v2": lambda doc: {
        #     **doc,
        #     "_schema_version": "v2",
        #     "new_field": "default_value"
        # }
    }

    # Migration functions for transaction documents
    _transaction_migrations: Dict[str, Dict[str, Callable]] = {
        # Example migration from v1 to v2:
        # "v1_to_v2": lambda doc: {
        #     **doc,
        #     "_schema_version": "v2",
        #     "new_field": "default_value"
        # }
    }

    @classmethod
    def migrate_document(
        cls,
        document: Dict[str, Any],
        doc_type: DocumentType,
        target_version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Migrate a document to a target schema version.
        
        If no target version is specified, migrates to the latest version.
        
        Args:
            document: The document to migrate.
            doc_type: The type of document (inventory or transaction).
            target_version: Optional target version. If None, migrates to latest.
            
        Returns:
            The migrated document.
            
        Raises:
            SchemaMigrationError: If migration fails or path not found.
        """
        current_version = document.get("_schema_version")
        if not current_version:
            current_version = "v1"  # Assume unversioned documents are v1
            document["_schema_version"] = current_version

        if target_version is None:
            target_version = SchemaRegistry.get_latest_version(doc_type)

        if current_version == target_version:
            return document

        # Get the appropriate migration functions
        migrations = (
            cls._inventory_migrations if doc_type == DocumentType.INVENTORY
            else cls._transaction_migrations
        )

        try:
            # Find migration path
            migration_path = cls._find_migration_path(
                current_version,
                target_version,
                migrations
            )

            # Apply migrations in sequence
            migrated_doc = document.copy()
            for from_ver, to_ver in zip(migration_path[:-1], migration_path[1:]):
                migration_key = f"{from_ver}_to_{to_ver}"
                if migration_key not in migrations:
                    raise SchemaMigrationError(
                        f"No migration function found for {migration_key}"
                    )
                migrated_doc = migrations[migration_key](migrated_doc)

            return migrated_doc

        except Exception as e:
            error_msg = (
                f"Failed to migrate {doc_type.value} document from "
                f"{current_version} to {target_version}: {str(e)}"
            )
            logging.error(error_msg)
            raise SchemaMigrationError(error_msg) from e

    @staticmethod
    def _find_migration_path(
        current_version: str,
        target_version: str,
        migrations: Dict[str, Callable]
    ) -> list[str]:
        """Find the shortest path between two schema versions.
        
        Args:
            current_version: Starting version.
            target_version: Target version.
            migrations: Available migration functions.
            
        Returns:
            List of versions forming the migration path.
            
        Raises:
            SchemaMigrationError: If no path is found.
        """
        # Extract all versions from migration functions
        versions = {current_version, target_version}
        for key in migrations:
            from_ver, to_ver = key.split("_to_")
            versions.add(from_ver)
            versions.add(to_ver)

        # Simple BFS to find shortest path
        queue = [(current_version, [current_version])]
        visited = {current_version}

        while queue:
            version, path = queue.pop(0)
            if version == target_version:
                return path

            # Find all possible next versions
            for key in migrations:
                from_ver, to_ver = key.split("_to_")
                if from_ver == version and to_ver not in visited:
                    visited.add(to_ver)
                    queue.append((to_ver, path + [to_ver]))

        raise SchemaMigrationError(
            f"No migration path found from {current_version} to {target_version}"
        )

    @classmethod
    def register_migration(
        cls,
        doc_type: DocumentType,
        from_version: str,
        to_version: str,
        migration_func: Callable[[Dict[str, Any]], Dict[str, Any]]
    ) -> None:
        """Register a new migration function.
        
        Args:
            doc_type: The document type for this migration.
            from_version: Source schema version.
            to_version: Target schema version.
            migration_func: Function that transforms documents from source to target version.
        """
        migration_key = f"{from_version}_to_{to_version}"
        migrations = (
            cls._inventory_migrations if doc_type == DocumentType.INVENTORY
            else cls._transaction_migrations
        )
        migrations[migration_key] = migration_func
        logging.info(
            f"Registered migration function for {doc_type.value}: {migration_key}"
        ) 
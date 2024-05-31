from abc import ABC, abstractmethod
from typing import List, Tuple


class DatabaseInterface(ABC):
    @abstractmethod
    def init_connection(self):
        """Initialize the database connection."""
        pass

    @abstractmethod
    def close_connection(self):
        """Close the database connection."""
        pass

    @abstractmethod
    def list_databases(self, db_target) -> Tuple[List[str], List[Tuple[str, str]]]:
        """List all databases and their sizes.

        Returns:
            List[Tuple[str, str]]: A list of tuples, where each tuple contains database name and size.
        """
        pass

    @abstractmethod
    def fetch_total_database_size(self) -> int:
        """Fetch the total size of all databases.

        Returns:
            int: Total size of all databases.
        """
        pass

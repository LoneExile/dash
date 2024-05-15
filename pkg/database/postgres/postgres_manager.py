import psycopg

from pkg.config import cfg
from pkg.database.database_interface import DatabaseInterface


class PostgresManager(DatabaseInterface):
    def __init__(self):
        self.host = cfg.postgres.PgHost
        self.database_name = cfg.postgres.PgDatabase
        self.port = cfg.postgres.PgPort
        self.user = cfg.postgres.PgUsername
        self.password = cfg.postgres.PgPassword
        self.dsn = f"dbname={self.database_name} user={self.user} password={self.password} host={self.host} port={self.port}"
        self.conn = None
        self.conn = psycopg.connect(self.dsn)

    def init_connection(self):
        """Initialize a new database connection using the DSN provided."""
        self.conn = psycopg.connect(self.dsn)

    def close_connection(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def list_databases(self):
        """List all non-template databases and their sizes."""
        if self.conn is None:
            self.init_connection()

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT datname, pg_size_pretty(pg_database_size(datname))
                FROM pg_database
                WHERE datistemplate = false;
                """
            )

            return cur.fetchall()

    def fetch_total_database_size(self):
        """Fetches the total size of all databases."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT SUM(pg_database_size(datname)) AS total_size FROM pg_database WHERE datistemplate = false;"
            )
            result = cur.fetchone()
            if result is None:
                return 0
            total_size = result[0]
            return total_size

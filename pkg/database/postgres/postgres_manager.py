import subprocess

import psycopg
from pkg.database.postgres import Postgres
from typing import List, Tuple


class PostgresManager(Postgres):
    def __init__(self):
        super().__init__()

    def init_connection(self, db_target=None):
        """Initialize a new database connection using the DSN provided."""
        if db_target:
            self.database_name = db_target
        self.dsn = f"dbname={self.database_name} user={self.user} password={self.password} host={self.host} port={self.port}"
        self.conn = psycopg.connect(self.dsn)

    def close_connection(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def get_psql_path(self):
        """Find the path to the psql executable."""
        # TODO: Add support for Windows
        psql_path = subprocess.run(
            ["which", "psql"], capture_output=True, text=True
        ).stdout.strip()
        if not psql_path:
            raise EnvironmentError(
                "psql command not found. Please ensure PostgreSQL client is installed and in PATH."
            )
        return psql_path

    def get_pg_dump_path(self):
        """Find the path to the pg_dump executable."""
        # TODO: Add support for Windows
        pg_dump_path = subprocess.run(
            ["which", "pg_dump"], capture_output=True, text=True
        ).stdout.strip()
        if not pg_dump_path:
            raise EnvironmentError(
                "pg_dump command not found. Please ensure PostgreSQL client is installed and in PATH."
            )
        return pg_dump_path

    def list_databases(self) -> Tuple[List[str], List[Tuple[str, str]]]:
        """List all non-template databases and their sizes."""
        if self.conn is None:
            self.init_connection()

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT datname as database_name, pg_size_pretty(pg_database_size(datname)) as size
                FROM pg_database
                WHERE datistemplate = false;
                """
            )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return columns, rows

    def list_tables(self, database_name):
        """List all tables in a given database."""
        if self.conn is None:
            self.init_connection()

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(table_name)))
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_catalog = '{database_name}';
                """
            )

            return cur.fetchall()

    def sum_table_sizes(self, database_name):
        """Sum the sizes of all tables in a given database."""
        if self.conn is None:
            self.init_connection()

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                WITH table_sizes AS (
                    SELECT
                    table_name,
                    pg_total_relation_size(quote_ident(table_name)) AS total_size
                    FROM
                    information_schema.tables
                    WHERE
                    table_schema = 'public'
                    AND table_catalog = '{database_name}'
                    )
                SELECT
                pg_size_pretty(SUM(total_size)) AS pretty_size
                FROM
                table_sizes;
                """
            )

            result = cur.fetchone()
            if result is None:
                return 0
            total_size = result[0]
            return total_size

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

    def escape_name(self, name):
        """Escape a given name."""
        return '"' + '\\"' + name + '\\"' + '"'

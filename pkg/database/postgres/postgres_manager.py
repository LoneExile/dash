import subprocess
from typing import List, Tuple

import psycopg
from jinja2 import Environment
from pkg.database.postgres import Postgres


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
        pg_dump_path = subprocess.run(
            ["which", "pg_dump"], capture_output=True, text=True
        ).stdout.strip()
        if not pg_dump_path:
            raise EnvironmentError(
                "pg_dump command not found. Please ensure PostgreSQL client is installed and in PATH."
            )
        return pg_dump_path

    def list_databases(self, db_target) -> Tuple[List[str], List[Tuple[str, str]]]:
        """List all non-template databases and their sizes."""
        if self.conn is None:
            self.init_connection(db_target)

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

    def list_tables(self, db_target):
        """List all tables in a given database."""
        if self.conn is None:
            self.init_connection(db_target)

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(table_name)))
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_catalog = '{db_target}';
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

    def get_table_size(self, table_name, db_target=None):
        if self.name_escaping:
            table_name = '"' + table_name + '"'
        if self.conn is None:
            self.init_connection(db_target)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    pg_total_relation_size('{table_name}')
                """
            )
            size = cur.fetchone()
            return size[0] if size else None

    def get_data_single(self, sql_file_path, db_target=None):
        """Get data from a SQL query file."""
        with open(sql_file_path, "r") as file:
            query = file.read()

        if self.conn is None:
            self.init_connection(db_target)

        with self.conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchall()[0][0]
            return row

    def get_data_single_by_id(self, sql_file_path, id, db_target=None):
        with open(sql_file_path, "r") as file:
            sql_query = file.read()

        env = Environment()
        template = env.from_string(sql_query)
        rendered_sql_query = template.render(ID=id)

        if self.conn is None:
            self.init_connection(db_target)

        with self.conn.cursor() as cur:
            cur.execute(rendered_sql_query)
            rows = cur.fetchall()
            # return rows
            data_list = [str(item[0]) for item in rows if item[0] is not None]
            return data_list

    def run_query(self, sql_file_path, db_target=None):
        """Run a SQL query file."""
        with open(sql_file_path, "r") as file:
            query = file.read()

        if self.conn is None:
            self.init_connection(db_target)

        with self.conn.cursor() as cur:
            try:
                cur.execute(query)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(e)

    def get_connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database_name}"

    def run_query_psql(self, sql_file_path, db_target=None):
        """Run a SQL query file using psql."""
        if self.conn is None:
            self.init_connection(db_target)

        psql_path = self.get_psql_path()

        command = (
            f"{psql_path} --host {self.host} --port {self.port} "
            f"--username {self.user} --dbname {self.database_name} "
            f"--file {sql_file_path}"
        )

        subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True,
            universal_newlines=True,
            env={"PGPASSWORD": self.password},
        )

    def insert_data_from_table(self, source_table, target_table, db_target=None):
        """Insert data from one table to another."""
        if self.conn is None:
            self.init_connection(db_target)

        if self.name_escaping:
            source_table = '"' + source_table + '"'
            target_table = '"' + target_table + '"'

        with self.conn.cursor() as cur:
            try:
                cur.execute(f"INSERT INTO {target_table} SELECT * FROM {source_table};")
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(e)

    def run_query_template(self, sql_file_path, db_target=None, **kwargs):
        """Run a SQL query file with Jinja2 templating."""
        with open(sql_file_path, "r") as file:
            query = file.read()

        env = Environment()
        template = env.from_string(query)
        rendered_query = template.render(**kwargs)

        if self.conn is None:
            self.init_connection(db_target)

        with self.conn.cursor() as cur:
            try:
                cur.execute(rendered_query)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(e)

    def get_data__by_list_template(self, sql_file_path, db_target=None, **kwargs):
        """Get data from a SQL query file using a list of IDs."""
        with open(sql_file_path, "r") as file:
            sql_query = file.read()

        env = Environment()
        template = env.from_string(sql_query)
        rendered_sql_query = template.render(**kwargs)

        if self.conn is None:
            self.init_connection(db_target)

        with self.conn.cursor() as cur:
            cur.execute(rendered_sql_query)
            rows = cur.fetchall()[0][0]
            return rows
            # data_list = [str(item[0]) for item in rows if item[0] is not None]
            # return data_list

    def get_data_list(self, sql_file_path, db_target=None, **kwargs):
        """Get data from a SQL query file."""
        with open(sql_file_path, "r") as file:
            query = file.read()

        env = Environment()
        template = env.from_string(query)
        rendered_sql_query = template.render(**kwargs)

        if self.conn is None:
            self.init_connection(db_target)

        with self.conn.cursor() as cur:
            cur.execute(rendered_sql_query)
            rows = cur.fetchall()
            data_list = [str(item[0]) for item in rows]
            return data_list

    def drop_table(self, table_name, db_target=None):
        """Drop a table."""
        if self.conn is None:
            self.init_connection(db_target)

        if self.name_escaping:
            table_name = '"' + table_name + '"'

        with self.conn.cursor() as cur:
            try:
                cur.execute(f"DROP TABLE {table_name};")
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(e)

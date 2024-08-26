import sqlite3

from jinja2 import Environment

from internal.db import Db


class DbDatabaseSqlite(Db):
    def __init__(self):
        super().__init__()

    def init(self, db_path):
        conn = sqlite3.connect(db_path)
        return conn

    def run_query_template(self, cur, sql_file_path, **kwargs):
        """Run a SQL query file with Jinja2 templating."""
        with open(sql_file_path, "r") as file:
            query = file.read()

        env = Environment()
        template = env.from_string(query)
        rendered_query = template.render(**kwargs)
        cur.execute(rendered_query)
        cur.connection.commit()

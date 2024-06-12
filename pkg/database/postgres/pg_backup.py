import csv
import os
import subprocess

from pkg.database.postgres.postgres_manager import PostgresManager


class DbBackup(PostgresManager):
    def __init__(self):
        super().__init__()

    def backup_table(self, table_name, backup_file, db_target, backup_dir=None):
        try:
            if self.name_escaping:
                table_name = self.helpers.escape_name(table_name)

            if self.conn is None:
                self.init_connection(db_target)

            if backup_dir is not None:
                self.backup_dir = backup_dir

            pg_dump_path = self.get_pg_dump_path()
            backup_file = f"{backup_file}.sql"

            os.makedirs(self.backup_dir, exist_ok=True)

            command = (
                f"{pg_dump_path} --host {self.host} --port {self.port} "
                f"--username {self.user} --format plain --column-inserts "
                f"--verbose --file {self.backup_dir + backup_file} --table {table_name} "
                f"--create --if-exists {self.database_name} -c"
            )

            result = subprocess.run(  # noqa: F841
                command,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                universal_newlines=True,
                env={"PGPASSWORD": self.password},
            )
            # output = result.stdout
            # error = result.stderr

            # if output:
            #     self.fmt.log(f"[bold green]{output}[/bold green]")
            # if error:
            #     self.fmt.log(f"[bold red]{error}[/bold red]")

        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")

    def backup_sql(self, query_file_path, backup_file="latest", db_target=None):
        csv_file_path = "temp.csv"
        try:
            with open(query_file_path, "r") as file:
                query = file.read()

            os.makedirs(self.backup_dir, exist_ok=True)

            if self.conn is None:
                self.init_connection(db_target)

            with self.conn.cursor() as cur:
                with open(csv_file_path, "w") as f:
                    with cur.copy(query) as copy:
                        for data in copy:
                            f.write(data.tobytes().decode("utf-8"))

            if os.path.getsize(csv_file_path) == 0:
                print(
                    "The CSV file is empty. Please check the query and connection parameters."
                )
            else:
                insert_statements = []
                with open(csv_file_path, newline="") as csvfile:
                    reader = csv.reader(csvfile)
                    headers = next(reader)

                    for row in reader:
                        values = [f"'{value}'" if value else "NULL" for value in row]
                        insert_statement = f"INSERT INTO \"ApplicationLogs\" ({', '.join(headers)}) VALUES ({', '.join(values)});"
                        insert_statements.append(insert_statement)

                with open(self.backup_dir + backup_file + ".sql", "w") as sqlfile:
                    sqlfile.write("\n".join(insert_statements))

        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")
        finally:
            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)

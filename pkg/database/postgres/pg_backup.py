import csv
import os
import subprocess

from pkg.database.postgres.postgres_manager import PostgresManager


class DbBackup(PostgresManager):
    def __init__(self):
        super().__init__()

    def backup_table(self, table_name, backup_file):
        try:
            if self.name_escaping:
                table_name = self.helpers.escape_name(table_name)

            pg_dump_path = self.get_pg_dump_path()
            backup_file = f"{backup_file}.sql"

            command = f"{pg_dump_path} --host {self.host} --port {self.port} --username {self.user} --format plain --verbose --file {self.backup_dir + backup_file} --table {table_name} {self.database_name}"
            result = subprocess.run(
                command,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                universal_newlines=True,
                env={"PGPASSWORD": self.password},
            )
            output = result.stdout
            error = result.stderr

            if output:
                self.fmt.log(f"[bold green]{output}[/bold green]")
            if error:
                self.fmt.log(f"[bold red]{error}[/bold red]")

            self.fmt.print(
                f"Backup of table [bold blue]{table_name}[/bold blue] completed to [bold blue]{self.backup_dir + backup_file}[/bold blue]"
            )
        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")

    def backup_sql(self, query_file_path, backup_file="latest"):
        csv_file_path = "temp.csv"
        try:
            with open(query_file_path, "r") as file:
                query = file.read()

            os.makedirs(self.backup_dir, exist_ok=True)

            if self.conn is None:
                self.init_connection("Basel")

            with self.conn.cursor() as cur:
                with open(csv_file_path, "w") as f:
                    with cur.copy(query) as copy:
                        for data in copy:
                            f.write(data.tobytes().decode("utf-8"))

            # self.fmt.print(
            #     f"Backup completed to [bold blue]{csv_file_path}[/bold blue]"
            # )
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

                # print(
                #     f"SQL insert statements have been written to {self.backup_dir + backup_file + '.sql'}"
                # )

        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")
        finally:
            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)

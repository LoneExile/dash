from pkg.database.postgres.postgres_manager import PostgresManager
import subprocess


class DbBackup(PostgresManager):
    def __init__(self):
        super().__init__()

    def backup_table(self, table_name, backup_file):
        try:
            if self.name_escaping:
                table_name = self.escape_name(table_name)

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

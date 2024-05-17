from pkg.database.postgres.postgres_manager import PostgresManager
import subprocess


class DbRestore(PostgresManager):
    def __init__(self):
        super().__init__()

    def restore_table(self, table, file_path):
        """Restore a table from a backup file."""
        try:
            psql_path = self.get_psql_path()
            cmd = f"{psql_path} -h {self.host} -p {self.port} -U {self.user} -d {table} -f {file_path}"
            result = subprocess.run(
                cmd,
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
                f"Restored table [bold blue]{table}[/bold blue] from file: [bold blue]{file_path}[/bold blue]"
            )

        except Exception as e:
            self.fmt(f"An error occurred: [bold red]{e}[/bold red]")

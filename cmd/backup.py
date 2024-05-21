from typing import Optional

import typer
from pkg.database.postgres.pg_backup import DbBackup
from typing_extensions import Annotated
from pkg.term.formatter.rich import TermFormatter

backupDb = typer.Typer(invoke_without_command=True)
bak = DbBackup()
fmt = TermFormatter()


@backupDb.callback()
def main(ctx: typer.Context):
    """Backup database."""
    if ctx.invoked_subcommand is None:
        raise typer.Exit(code=typer.main.get_command(backupDb)(["--help"]))


@backupDb.command()
def all():
    print("Backing up all item")
    bak.backup_sql("./books/demo/chapters/anyName/ApplicationLogs1/delete.sql")


@backupDb.command()
def target(
    table: Annotated[Optional[str], typer.Argument()],
    file: Annotated[Optional[str], typer.Argument()],
):
    fmt.print(
        f"Backing up table: [bold blue]{table}[/bold blue] to file: [bold blue]{file}[/bold blue]"
    )
    bak.backup_table(table, file)

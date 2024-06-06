from typing import Optional

import typer
from pkg.database.postgres.pg_restore import DbRestore
from typing_extensions import Annotated
from pkg.term.formatter.rich import TermFormatter

restoreDb = typer.Typer(invoke_without_command=True)
rst = DbRestore()
fmt = TermFormatter()


@restoreDb.callback()
def main(ctx: typer.Context):
    """Restore database."""
    if ctx.invoked_subcommand is None:
        raise typer.Exit(code=typer.main.get_command(restoreDb)(["--help"]))


@restoreDb.command()
def all():
    print("Backing up all item")


@restoreDb.command()
def target(
    table: Annotated[Optional[str], typer.Argument()],
    file: Annotated[Optional[str], typer.Argument()],
    db_target: Annotated[Optional[str], typer.Argument()] = None,
):
    fmt.print(
        f"Restoring up schema: [bold blue]{table}[/bold blue] from file: [bold blue]{file}[/bold blue]"
    )
    rst.restore_table(table, file, db_target)

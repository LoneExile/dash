from typing import Optional

import typer
from internal.db.database import DbDatabase
from internal.db.table import DbTable
from typing_extensions import Annotated

inspector = typer.Typer(invoke_without_command=True)
db = DbDatabase()
tb = DbTable()


@inspector.callback()
def main(ctx: typer.Context):
    """Inspect the database."""
    if ctx.invoked_subcommand is None:
        db.inspect()


@inspector.command()
def tables(schema: Annotated[Optional[str], typer.Argument()]):
    """List all tables in the database."""
    tb.list_tables(schema)

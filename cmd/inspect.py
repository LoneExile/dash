import typer

from pkg.database.postgres.postgres_manager import PostgresManager
from pkg.term.formatter.rich import TermFormatter

inspector = typer.Typer(invoke_without_command=True)


@inspector.callback()
def main(ctx: typer.Context):
    """Inspect the database."""
    if ctx.invoked_subcommand is None:
        inspect()


def inspect():
    fmt = TermFormatter()
    dbm = PostgresManager()

    try:
        db_list = dbm.list_databases()
        fmt.print_table(db_list, ["Database Name", "Size"])
        total_db_size = round(dbm.fetch_total_database_size() / 1024 / 1024 / 1024, 2)

        fmt.print(f"Total DB Size: [bold green]{total_db_size} GB[/bold green]")
    finally:
        dbm.close_connection()

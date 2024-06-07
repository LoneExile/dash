import os
from typing import Optional

import typer
from internal.db.database import DbDatabase
from internal.db.table import DbTable
from internal.reader.reader_manager import ReaderManager
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from internal.utils import Utils
from pkg.config import cfg
from pkg.database.postgres.pg_backup import DbBackup
from pkg.term.formatter.rich import TermFormatter
from typing_extensions import Annotated

backupDb = typer.Typer(invoke_without_command=True)
bak = DbBackup()
fmt = TermFormatter()

db = DbDatabase()
tb = DbTable()
rm = ReaderManager()
utils = Utils()
v1 = ProcessStructureV1()


@backupDb.callback()
def main(
    ctx: typer.Context,
    book: Annotated[
        str,
        typer.Option(
            "--book",
            "-b",
            help="Inspect by Book.",
        ),
    ] = None,
    clean: Annotated[
        bool,
        typer.Option(
            "--clean",
            "-C",
            help="Clean up the database after backup.",
        ),
    ] = False,
):
    """Backup database."""
    if book is not None:
        path = cfg.Books.Location + book
        is_path = os.path.exists(path)
        if not is_path:
            raise typer.BadParameter(f"Path does not exist: {path}")
        dir_struc = rm.read_directory(path)
        appendix_dir = utils.find_file("appendix.yaml", path)
        rm.appendix = rm.load_appendix(appendix_dir[0])

        if "apiVersion" not in rm.appendix:
            raise Exception("apiVersion not found in appendix.yaml")

        match rm.appendix["apiVersion"]:
            case "v1":
                try:
                    v1.clean = clean
                    v1.appendix = rm.appendix
                    v1.book = book
                    v1.process_structure_v1(dir_struc, ModeKeys.BACKUP_CREATE_TABLE)
                except Exception as e:
                    typer.echo(f"Error: {e}")
            case _:
                raise Exception("apiVersion not supported")
    else:
        if ctx.invoked_subcommand is None:
            raise typer.Exit(code=typer.main.get_command(backupDb)(["--help"]))


@backupDb.command()
def target(
    table: Annotated[Optional[str], typer.Argument()],
    file: Annotated[Optional[str], typer.Argument()],
    db_target: Annotated[Optional[str], typer.Argument()] = None,
):
    fmt.print(
        f"Backing up table: [bold blue]{table}[/bold blue] to file: [bold blue]{file}[/bold blue]"
    )
    bak.backup_table(table, file, db_target)

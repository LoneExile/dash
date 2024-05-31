import os

# from typing import Optional

import typer
from internal.db.database import DbDatabase
from internal.db.table import DbTable
from internal.reader.reader_manager import ReaderManager, Mode
from pkg.config import cfg
from typing_extensions import Annotated
from internal.utils import Utils

inspector = typer.Typer(invoke_without_command=True)
db = DbDatabase()
tb = DbTable()
rm = ReaderManager()
utils = Utils()


@inspector.callback()
def main(
    target: Annotated[
        str,
        typer.Option(
            "--target",
            "-t",
            help="The database to inspect table.",
        ),
    ] = None,
    # size: Annotated[
    #     bool,
    #     typer.Option(
    #         "--size",
    #         "-s",
    #         help="The database to inspect table.",
    #     ),
    # ] = False,
    # test: Annotated[
    #     bool,
    #     typer.Option(
    #         "--wize",
    #         "-w",
    #         help="The database to inspect table.",
    #     ),
    # ] = False,
    book: Annotated[
        str,
        typer.Option(
            "--book",
            "-b",
            help="Inspect by Book.",
        ),
    ] = None,
):
    """Inspect the database."""
    if target is not None:
        tb.list_tables(target)
    elif book is not None:
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
                    rm.process_structure_v1(dir_struc, Mode.INSPECT)
                except Exception as e:
                    typer.echo(f"Error: {e}")
            case _:
                raise Exception("apiVersion not supported")
    else:
        db.inspect()

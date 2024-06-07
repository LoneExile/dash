import os

# from typing import Optional

import typer
from internal.db.database import DbDatabase
from internal.db.table import DbTable
from internal.reader.reader_manager import ReaderManager
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from pkg.config import cfg
from typing_extensions import Annotated
from internal.utils import Utils

inspector = typer.Typer(invoke_without_command=True)
db = DbDatabase()
tb = DbTable()
rm = ReaderManager()
utils = Utils()
v1 = ProcessStructureV1()


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
                    v1.process_structure_v1(dir_struc, ModeKeys.INSPECT)
                except Exception as e:
                    typer.echo(f"Error: {e}")
            case _:
                raise Exception("apiVersion not supported")
    else:
        db.inspect()

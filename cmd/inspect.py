import os

import typer
from internal.db.database import DbDatabase
from internal.db.table import DbTable
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from internal.reader.reader_manager import ReaderManager
from internal.utils import Utils
from pkg.config import cfg
from rich.console import Group
from rich.live import Live
from rich.progress import (
    FileSizeColumn,
    Progress,
    TextColumn,
)
from typing_extensions import Annotated

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
    bucket: Annotated[
        str,
        typer.Option(
            "--s3-bucket",
            help="Backup to S3 bucket.",
        ),
    ] = None,
    # dir_name: Annotated[
    #     str,
    #     typer.Option(
    #         "--name",
    #         "-n",
    #         help="Name of the backup.",
    #     ),
    # ] = None,
):
    """Inspect the database."""
    if target is not None:
        tb.list_tables(target)
    elif book is not None:
        path = cfg.Books.Location + book
        print(path)
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
                    progress = Progress(
                        TextColumn("[progress.description]{task.description}"),
                        FileSizeColumn(),
                    )
                    v1.appendix = rm.appendix
                    v1.progress = progress
                    v1.appendix_file_path = appendix_dir[0]
                    v1.s3_bucket = bucket
                    if book is not None:
                        v1.dir_name = book
                    v1.book = book
                    with Live((Group(progress))):
                        v1.process_structure_v1(dir_struc, ModeKeys.INSPECT)
                except Exception as e:
                    typer.echo(f"Error: {e}")
            case _:
                raise Exception("apiVersion not supported")
    else:
        db.inspect()

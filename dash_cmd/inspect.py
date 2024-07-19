import os
import traceback
from datetime import datetime

import typer
from internal.db.database import DbDatabase
from internal.db.table import DbTable
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from internal.reader.reader_manager import ReaderManager
from internal.utils import Utils
from pkg.config import cfg
from rich.console import Console, Group
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
    database: Annotated[
        str,
        typer.Option(
            "--database",
            "-d",
            # "-t",
            help="The database to inspect table.",
        ),
    ] = None,
    schema: Annotated[
        str,
        typer.Option(
            "--schema",
            "-s",
            help="The schema to inspect table.",
        ),
    ] = "public",
    book: Annotated[
        str,
        typer.Option(
            "--book",
            "-b",
            help="Inspect by Book.",
        ),
    ] = None,
    backup_dir: Annotated[
        str,
        typer.Option(
            "--backup-dir",
            "-B",
            help="Backup directory.",
        ),
    ] = None,
    bucket: Annotated[
        str,
        typer.Option(
            "--s3-bucket",
            help="Backup to S3 bucket.",
        ),
    ] = None,
    start_date: Annotated[
        str,
        typer.Option(
            "--start-date",
            help="Start date for backup.",
        ),
    ] = None,
    end_date: Annotated[
        str,
        typer.Option(
            "--end-date",
            help="End date for backup.",
        ),
    ] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
):
    """Inspect the database."""

    # if database is not None and book is not None:
    #     raise typer.BadParameter("Cannot use --database and --book together.")

    if database is not None:
        tb.list_tables(database, schema)

    # Code is unreachable  81:9:11Pyright
    if book is not None or backup_dir is not None:
        if backup_dir is not None:
            dir = backup_dir
            path = cfg.Postgres.PgBackupDir + backup_dir
        else:
            dir = book
            path = cfg.Books.Location + book
        is_path = os.path.exists(path)
        if not is_path:
            raise typer.BadParameter(f"Path does not exist: {path}")
        dir_struc = rm.read_directory(path)
        appendix_dir = utils.find_file("appendix.yaml", path)
        rm.appendix = rm.load_appendix(appendix_dir[0])

        if "apiVersion" not in rm.appendix:
            raise Exception("apiVersion not found in appendix.yaml")

        if rm.appendix.get("chapters") and rm.appendix.get("hook") is not None:
            is_hook = True
            hook_path = path
        else:
            is_hook = False
            hook_path = None

        if start_date is not None:
            is_date = True
        elif rm.appendix.get("date") and rm.appendix["date"]["start"]:
            is_date = True
            start_date = rm.appendix["date"]["start"]
            end_date = rm.appendix["date"]["end"]
            print(f"Start Date: {start_date}")
            print(f"End Date: {end_date}")
        else:
            is_date = False

        print(f"is_date: {is_date}")
        print(f"start_date: {start_date}")
        print(f"end_date: {end_date}")

        match rm.appendix["apiVersion"]:
            case "v1":
                try:
                    console = Console()
                    status = console.status("Not started")
                    progress = Progress(
                        TextColumn("[progress.description]{task.description}"),
                        FileSizeColumn(),
                    )
                    v1.appendix = rm.appendix
                    v1.progress = progress
                    v1.status = status
                    v1.appendix_file_path = appendix_dir[0]
                    v1.s3_bucket = bucket
                    v1.is_hook = is_hook
                    v1.hook_path = hook_path
                    if dir is not None:
                        v1.dir_name = dir
                    v1.dir = dir
                    v1.start_date = start_date
                    v1.end_date = end_date
                    v1.is_date = is_date
                    v1.end_date = end_date
                    with Live((Group(status, progress))):
                        status.update("[bold green]Status = Started[/bold green]")
                        v1.process_structure_v1(dir_struc, ModeKeys.INSPECT)
                        status.update("[bold green]Status = Completed[/bold green]")
                        status.stop()
                except Exception as e:
                    traceback.print_exc()
                    typer.echo(f"Error: {e}")
            case _:
                raise Exception("apiVersion not supported")
    else:
        db.inspect()

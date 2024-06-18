import os
from typing import Optional

import typer
from internal.db.database import DbDatabase
from internal.db.table import DbTable
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from internal.reader.reader_manager import ReaderManager
from internal.utils import Utils

from pkg.aws.s3 import S3
from pkg.config import cfg
from pkg.database.postgres.pg_backup import DbBackup
from pkg.term.formatter.rich import TermFormatter
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    FileSizeColumn,
    Progress,
    TextColumn,
)
from typing_extensions import Annotated

backupDb = typer.Typer(invoke_without_command=True)
bak = DbBackup()
fmt = TermFormatter()

db = DbDatabase()
tb = DbTable()
rm = ReaderManager()
utils = Utils()
v1 = ProcessStructureV1()
s3 = S3()


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
    bucket: Annotated[
        str,
        typer.Option(
            "--s3-bucket",
            help="Backup to S3 bucket.",
        ),
    ] = None,
    dir_name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help="Name of the backup.",
        ),
    ] = None,
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

        if bucket is not None:
            s3.check_bucket_exists(bucket)

        if (
            rm.appendix.get("chapters")
            and rm.appendix["chapters"].get("hook") is not None
        ):
            is_hook = True
        else:
            is_hook = False

        match rm.appendix["apiVersion"]:
            case "v1":
                try:
                    console = Console()
                    status = console.status("Not started")
                    progress = Progress(
                        TextColumn("[progress.description]{task.description}"),
                        # SpinnerColumn(),
                        FileSizeColumn(),
                    )

                    v1.clean = clean
                    v1.appendix = rm.appendix
                    v1.book = book
                    v1.s3_bucket = bucket
                    v1.status = status
                    v1.progress = progress
                    v1.dir_name = dir_name
                    v1.appendix_file_path = appendix_dir[0]
                    v1.is_hook = is_hook

                    with Live(Panel(Group(status, progress))):
                        status.update("[bold green]Status = Started[/bold green]")
                        v1.process_structure_v1(dir_struc, ModeKeys.BACKUP_CREATE_TABLE)
                        status.update("[bold green]Status = Completed[/bold green]")
                        status.stop()

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

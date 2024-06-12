import os
from typing import Optional

import typer
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from internal.reader.reader_manager import ReaderManager
from internal.utils import Utils
from pkg.database.postgres.pg_restore import DbRestore
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

restoreDb = typer.Typer(invoke_without_command=True)
rst = DbRestore()
fmt = TermFormatter()
rm = ReaderManager()
utils = Utils()
v1 = ProcessStructureV1()


@restoreDb.callback()
def main(
    ctx: typer.Context,
    bucket: Annotated[
        str,
        typer.Option(
            "--s3-bucket",
            help="Restore from S3 bucket.",
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
    """Restore database."""
    if dir_name is not None:
        fmt.print(f"Restoring up database: [bold blue]{dir_name}[/bold blue]")
        path = dir_name
        is_path = os.path.exists(path)
        if not is_path:
            raise typer.BadParameter(f"Path does not exist: {path}")
        dir_struc = rm.read_directory(path)
        appendix_dir = utils.find_file("appendix.yaml", path)
        rm.appendix = rm.load_appendix(appendix_dir[0])
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
                    v1.appendix = rm.appendix
                    v1.progress = progress
                    v1.status = status
                    v1.console = console
                    v1.dir_name = dir_name
                    v1.appendix_file_path = appendix_dir[0]
                    v1.s3_bucket = bucket
                    with Live(Panel(Group(status, progress))):
                        status.update("[bold green]Status = Started[/bold green]")
                        v1.process_structure_v1(dir_struc, ModeKeys.RESORE_TABLE)
                        status.update("[bold green]Status = Completed[/bold green]")
                        status.stop()
                except Exception as e:
                    typer.echo(f"Error: {e}")
            case _:
                raise Exception("apiVersion not supported")
    elif ctx.invoked_subcommand is None:
        raise typer.Exit(code=typer.main.get_command(restoreDb)(["--help"]))


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

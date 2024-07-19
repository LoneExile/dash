import os
from typing import Optional

import typer
import yaml
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from internal.reader.reader_manager import ReaderManager
from internal.utils import Utils
from pkg.aws.s3 import S3
from pkg.database.postgres.pg_restore import DbRestore
from pkg.term.formatter.rich import TermFormatter
from rich.console import Console, Group
from rich.live import Live

# from rich.panel import Panel
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
s3 = S3()


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
    no_confirm: Annotated[
        bool,
        typer.Option(
            "--yes",
            "--nc",
            "-y",
            help="Do not ask for confirmation.",
        ),
    ] = False,
):
    """Restore database."""
    if not no_confirm:
        fmt.print("[bold red]Are you sure you want to proceed?[/bold red]")
    confirm = no_confirm or input("(y/N): ").lower() in ("y", "yes")
    if not confirm:
        raise typer.Abort()

    if bucket is not None:
        isBucket = s3.check_bucket_exists(bucket)

        if isBucket:
            s3_contents = s3.list_bucket_contents(bucket, dir_name)

            if s3_contents:
                appendix = [item for item in s3_contents if "appendix.yaml" in item]
                rm.appendix = yaml.safe_load(s3.read_file(bucket, appendix[0]))

                if rm.appendix.get("chapters") and rm.appendix.get("hook") is not None:
                    is_hook = True
                else:
                    is_hook = False

                if "apiVersion" not in rm.appendix:
                    raise Exception("apiVersion not found in appendix.yaml")
                v1.appendix = rm.appendix
                v1.dir_name = dir_name
                v1.s3_bucket = bucket
                v1.is_hook = is_hook
                v1.restore_s3_v1(s3_contents)

    else:
        if dir_name is not None:
            fmt.print(f"Restoring up database: [bold blue]{dir_name}[/bold blue]")
            path = dir_name
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
                        v1.is_hook = is_hook
                        v1.hook_path = hook_path
                        v1.is_date = False
                        with Live((Group(status, progress))):
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

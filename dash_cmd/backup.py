import os
import traceback
from datetime import datetime
from typing import Optional

import typer
from rich.console import Console, Group
from rich.live import Live

# from rich.panel import Panel
from rich.progress import (
    FileSizeColumn,
    Progress,
    TextColumn,
)
from typing_extensions import Annotated, List

from internal.db.pg.database import DbDatabase
from internal.db.pg.table import DbTable
from internal.db.sqlite.database import DbDatabaseSqlite

# from internal.db.sqlite
from internal.reader.process_structure_v1 import ModeKeys, ProcessStructureV1
from internal.reader.process_structure_v2 import ModeKeysV2, ProcessStructureV2
from internal.reader.reader_manager import ReaderManager
from internal.utils import Utils
from pkg.aws.s3 import S3
from pkg.config import cfg
from pkg.database.postgres.pg_backup import DbBackup
from pkg.term.formatter.rich import TermFormatter

backupDb = typer.Typer(invoke_without_command=True)
bak = DbBackup()
fmt = TermFormatter()

db = DbDatabase()
sqlite = DbDatabaseSqlite()
tb = DbTable()
rm = ReaderManager()
utils = Utils()
v1 = ProcessStructureV1()
v2 = ProcessStructureV2()
s3 = S3()


@backupDb.callback()
def main(
    ctx: typer.Context,
    book: Annotated[
        str,
        typer.Option(
            "--book",
            "-b",
            help="Backup by Book.",
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
            "-d",
            help="Name of the backup directory.",
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
    ] = None,
    custom: Annotated[
        List[str],
        typer.Option(
            "--custom",
            help="Custom query for backup.",
        ),
    ] = None,
):
    """Backup database."""

    print(f"custom: {custom}")

    # if custom:
    #     custom_dict = {}
    #     for item in custom:
    #         key, value = item.split("=", 1)
    #         custom_dict[key] = value

    #     print("Custom options:")
    #     for key, value in custom_dict.items():
    #         print(f"  {key}: {value}")

    # raise typer.Abort()
    ################################
    if not no_confirm:
        fmt.print("[bold red]Are you sure you want to proceed?[/bold red]")
    confirm = no_confirm or input("(y/N): ").lower() in ("y", "yes")
    if not confirm:
        raise typer.Abort()

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
            s3.check_bucket_exists(bucket, True)

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
                    v1.hook_path = hook_path
                    v1.is_date = is_date
                    v1.start_date = start_date
                    if end_date is not None:
                        v1.end_date = end_date
                    else:
                        v1.end_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                    with Live((Group(status, progress))):
                        status.update("[bold green]Status = Started[/bold green]")
                        v1.process_structure_v1(dir_struc, ModeKeys.BACKUP_CREATE_TABLE)
                        status.update("[bold green]Status = Completed[/bold green]")
                        status.stop()

                except Exception as e:
                    typer.echo(f"Error: {e}")
            case "v2":
                try:
                    console = Console()
                    status = console.status("Not started")
                    progress = Progress(
                        TextColumn("[progress.description]{task.description}"),
                        # SpinnerColumn(),
                        FileSizeColumn(),
                    )
                    backup_dir = os.path.join(cfg.Postgres.PgBackupDir, dir_name)
                    os.makedirs(backup_dir, exist_ok=True)
                    sqlite_index = os.path.join(
                        cfg.Postgres.PgBackupDir, dir_name, "index.db"
                    )
                    if os.path.exists(sqlite_index):
                        os.remove(sqlite_index)
                    conn = sqlite.init(
                        sqlite_index,
                    )
                    cur = conn.cursor()
                    appendix_dir = utils.find_file("table_appendix.sql", path)
                    if len(appendix_dir) >= 0:
                        table_appendix = appendix_dir[0]
                        custom_appendix = ","
                        if custom:
                            custom_dict = {}
                            for item in custom:
                                key, value = item.split("=", 1)
                                custom_dict[key] = value
                            for i, (key, value) in enumerate(custom_dict.items()):
                                if i == len(custom_dict) - 1:
                                    custom_appendix += f"{key}"
                                else:
                                    custom_appendix += f"{key} text,"

                        sqlite.run_query_template(
                            cur,
                            table_appendix,
                            CUSTOM_APPENDIX=custom_appendix,
                        )

                    v2.sqlite = cur
                    v2.clean = clean
                    v2.appendix = rm.appendix
                    v2.book = book
                    v2.s3_bucket = bucket
                    v2.status = status
                    v2.progress = progress
                    v2.dir_name = dir_name
                    v2.appendix_file_path = appendix_dir[0]
                    v2.is_hook = is_hook
                    v2.hook_path = hook_path
                    v2.is_date = is_date
                    v2.start_date = start_date
                    v2.custom = custom
                    v2.end_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    if end_date is not None:
                        v2.end_date = end_date

                    with Live((Group(status, progress))):
                        status.update("[bold green]Status = Started[/bold green]")
                        v2.process_structure_v2(dir_struc, ModeKeysV2.BACKUP_CREATE_TABLE)
                        status.update("[bold green]Status = Completed[/bold green]")
                        status.stop()

                except Exception as e:
                    traceback.print_exc()
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

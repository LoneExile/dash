import typer
import os

# from typing import Optional
from typing_extensions import Annotated

# import yaml

from internal.reader.reader_manager import ReaderManager
from pkg.config import cfg

readBook = typer.Typer(invoke_without_command=True)
rm = ReaderManager()

book = cfg.Books.Play


@readBook.callback()
def main(
    ctx: typer.Context,
    target: Annotated[
        str,
        typer.Option(
            "--target",
            "-t",
            help="The book to read.",
        ),
    ] = book,
):
    """Read book."""
    if ctx.invoked_subcommand is None:
        path = cfg.Books.Location + target
        is_path = os.path.exists(path)
        if not is_path:
            raise typer.BadParameter(f"Path does not exist: {path}")

        dir_struc = rm.read_directory(path)
        # yaml_output = rm.output_as_yaml(dir_struc)
        # print(yaml_output)
        # print(f"Reading book: {target}")
        rm.process_structure(dir_struc)
        print(f"Completed backup book: {target}")


# @readBook.command()
# def index():
#     path = cfg.Books.Location + cfg.Books.Play
#     dir_struc = rm.read_directory(path)
#     rm.process_structure(dir_struc)

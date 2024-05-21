import typer
# from typing import Optional
# from typing_extensions import Annotated
# import yaml

from internal.reader.reader_manager import ReaderManager
from pkg.config import cfg

readBook = typer.Typer(invoke_without_command=True)
rm = ReaderManager()


@readBook.callback()
def main(ctx: typer.Context):
    """Read book."""
    if ctx.invoked_subcommand is None:
        path = cfg.Books.Location + cfg.Books.Play
        dir_struc = rm.read_directory(path)
        yaml_output = rm.output_as_yaml(dir_struc)
        print(yaml_output)


@readBook.command()
def index():
    path = cfg.Books.Location + cfg.Books.Play
    dir_struc = rm.read_directory(path)
    rm.process_structure(dir_struc)

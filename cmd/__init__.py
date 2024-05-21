import typer

from . import backup, inspect, restore, read

cmd = typer.Typer(invoke_without_command=True)

cmd.add_typer(backup.backupDb, name="backup")
cmd.add_typer(restore.restoreDb, name="restore")
cmd.add_typer(inspect.inspector, name="inspect")
cmd.add_typer(read.readBook, name="read")


@cmd.callback()
def main(ctx: typer.Context):
    """Handle the main entry point with no specific commands."""
    if ctx.invoked_subcommand is None:
        raise typer.Exit(code=typer.main.get_command(cmd)(["--help"]))

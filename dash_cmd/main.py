import typer

from . import backup, inspect, restore


def app():
    app = typer.Typer(invoke_without_command=True)

    app.add_typer(backup.backupDb, name="backup")
    app.add_typer(restore.restoreDb, name="restore")
    app.add_typer(inspect.inspector, name="inspect")

    app()

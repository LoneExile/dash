import typer

from . import backup, inspector, restore

cmd = typer.Typer()
cmd.add_typer(backup.backupDb, name="backup")
cmd.add_typer(restore.restoreDb, name="restore")


@cmd.command()
def inspect():
    inspector.inspect()

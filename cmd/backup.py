import typer

backupDb = typer.Typer(invoke_without_command=True)


@backupDb.callback()
def main(ctx: typer.Context):
    """Backup database."""
    if ctx.invoked_subcommand is None:
        raise typer.Exit(code=typer.main.get_command(backupDb)(["--help"]))


@backupDb.command()
def all():
    print("Backing up all item")


@backupDb.command()
def target(item: str):
    print(f"Backing up item: {item}")


if __name__ == "__main__":
    backupDb()

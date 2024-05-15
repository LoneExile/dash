import typer

restoreDb = typer.Typer(invoke_without_command=True)


@restoreDb.callback()
def main(ctx: typer.Context):
    """Restore database."""
    if ctx.invoked_subcommand is None:
        raise typer.Exit(code=typer.main.get_command(restoreDb)(["--help"]))


@restoreDb.command()
def all():
    print("Backing up all item")


@restoreDb.command()
def target(item: str):
    print(f"Backing up item: {item}")


if __name__ == "__main__":
    restoreDb()

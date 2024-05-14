import typer

restoreDb = typer.Typer()


@restoreDb.command()
def all():
    print("Backing up all item")


@restoreDb.command()
def target(item: str):
    print(f"Backing up item: {item}")


if __name__ == "__main__":
    restoreDb()

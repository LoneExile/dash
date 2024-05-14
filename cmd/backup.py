import typer

backupDb = typer.Typer()


@backupDb.command()
def all():
    print("Backing up all item")


@backupDb.command()
def target(item: str):
    print(f"Backing up item: {item}")


if __name__ == "__main__":
    backupDb()

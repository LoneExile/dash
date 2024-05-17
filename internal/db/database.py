from internal.db import Db


class DbDatabase(Db):
    def __init__(self):
        super().__init__()

    def inspect(self):
        try:
            db_list = self.dbm.list_databases()
            self.fmt.print_table(db_list, ["Database Name", "Size"])
            total_db_size = self.utils.convert_bytes_to_gb(
                self.dbm.fetch_total_database_size()
            )

            self.fmt.print(
                f"Total DB Size: [bold green]{total_db_size} GB[/bold green]"
            )
        finally:
            self.dbm.close_connection()

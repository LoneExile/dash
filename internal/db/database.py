from internal.db import Db


class DbDatabase(Db):
    def __init__(self):
        super().__init__()

    def inspect(self, db_target=None):
        try:
            columns, db_list = self.dbm.list_databases(db_target)
            self.fmt.print_table(db_list, columns)
            total_db_size = self.utils.convert_bytes_to_gb(
                self.dbm.fetch_total_database_size()
            )

            self.fmt.print(
                f"Total DB Size: [bold green]{total_db_size} GB[/bold green]"
            )
        finally:
            self.dbm.close_connection()

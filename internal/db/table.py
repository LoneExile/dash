from internal.db import Db


class DbTable(Db):
    def __init__(self):
        super().__init__()

    def list_tables(self, db_target: str):
        try:
            table_list = self.dbm.list_tables(db_target)
            self.fmt.print_table(table_list, ["Table Name", "Size"])

            total_table_size = str(self.dbm.sum_table_sizes(db_target))
            self.fmt.print(
                f"Total Table Size: [bold green]{total_table_size} [/bold green]"
            )
        finally:
            self.dbm.close_connection()

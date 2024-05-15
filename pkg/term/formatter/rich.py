from rich.console import Console
from rich.table import Table


class TermFormatter:
    def __init__(self):
        self.console = Console()

    def print(self, text):
        """Prints a text with a newline."""
        self.console.print(text)

    def print_table(self, data, columns, style="dim", header_style="bold magenta"):
        """Prints a table with dynamic columns and data.

        Args:
            data (list of list): A list of rows, where each row is a list of values corresponding to columns.
            columns (list of str): A list of column headers.
            style (str, optional): Style of the data. Defaults to "dim".
            header_style (str, optional): Style of the header. Defaults to "bold magenta".
        """
        self.table = Table(show_header=True, header_style=header_style)
        for column in columns:
            self.table.add_column(column, style=style)
        for row in data:
            self.table.add_row(*row)
        self.console.print(self.table)

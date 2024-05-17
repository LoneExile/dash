from rich.console import Console
from rich.table import Table

import logging
from rich.logging import RichHandler


class TermFormatter:
    def __init__(self):
        self.console = Console()

    def print(self, text):
        """Prints a text with a newline."""
        self.console.print(text)

    def log(self, text, log_locals=False):
        """Prints a Log message with a newline."""
        self.console.log(text, log_locals=log_locals)

    def logging(self, level=logging.INFO):
        """Configures logging to use RichHandler.

        Args:
            level (int, optional): Logging level. Defaults to logging.INFO.
        """
        logging.basicConfig(
            level=level,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler(rich_tracebacks=True)],
        )

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
            self.table.add_row(*(str(item) for item in row))
        self.console.print(self.table)

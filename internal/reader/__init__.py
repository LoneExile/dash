from internal.utils import Utils
from pkg.config import cfg
from pkg.config.config_manager import ConfigManager
from pkg.database.postgres.pg_backup import DbBackup
from pkg.database.postgres.postgres_manager import PostgresManager
from pkg.term.formatter.rich import TermFormatter
import datetime


class Reader:
    def __init__(self):
        self.books_loc = cfg.Books.Location
        self.playbook = cfg.Books.Play
        self.cfg = cfg
        self.current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.bak = DbBackup()
        self.utils = Utils()
        self.config = ConfigManager()
        self.pg = PostgresManager()
        self.fmt = TermFormatter()

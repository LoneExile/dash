import psycopg
from pkg.config import cfg
from pkg.database.database_interface import DatabaseInterface
from pkg.term.formatter.rich import TermFormatter


class Postgres(DatabaseInterface):
    def __init__(self):
        self.host = cfg.Postgres.PgHost
        self.database_name = cfg.Postgres.PgDatabase
        self.port = cfg.Postgres.PgPort
        self.user = cfg.Postgres.PgUsername
        self.password = str(cfg.Postgres.PgPassword)
        self.backup_dir = cfg.Postgres.PgBackupDir
        self.name_escaping = cfg.Postgres.PgNameEscaping
        self.dsn = f"dbname={self.database_name} user={self.user} password={self.password} host={self.host} port={self.port}"
        self.conn = None
        self.conn = psycopg.connect(self.dsn)
        self.fmt = TermFormatter()

        try:
            self.Play = cfg.Play
        except AttributeError:
            self.Play = "default"

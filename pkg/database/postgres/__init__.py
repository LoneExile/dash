import psycopg
from pkg.config import cfg
from pkg.database.database_interface import DatabaseInterface
from pkg.term.formatter.rich import TermFormatter


class Postgres(DatabaseInterface):
    def __init__(self):
        self.host = cfg.postgres.PgHost
        self.database_name = cfg.postgres.PgDatabase
        self.port = cfg.postgres.PgPort
        self.user = cfg.postgres.PgUsername
        self.password = str(cfg.postgres.PgPassword)
        self.backup_dir = cfg.postgres.PgBackupDir
        self.name_escaping = cfg.postgres.PgNameEscaping
        self.dsn = f"dbname={self.database_name} user={self.user} password={self.password} host={self.host} port={self.port}"
        self.conn = None
        self.conn = psycopg.connect(self.dsn)
        self.fmt = TermFormatter()

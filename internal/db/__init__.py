from internal.utils import Utils
from pkg.config import cfg
from pkg.term.formatter.rich import TermFormatter


class Db:
    def __init__(self):
        self.fmt = TermFormatter()
        self.utils = Utils()

        if cfg.Database.Type == "postgres":
            module_name = "pkg.database.postgres.postgres_manager"
            class_name = "PostgresManager"
        else:
            raise ValueError("Unsupported database type")
        self.dbm = self.utils.dynamic_import(module_name, class_name)()

        try:
            self.Play = cfg.Play
        except AttributeError:
            self.Play = "default"

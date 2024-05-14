from internal.config import cfg


def inspect():
    print(f"Inspecting items {cfg.postgres.PgHost}")

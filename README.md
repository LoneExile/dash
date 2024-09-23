# -

## Prerequisites

- [Python 3.8+](https://www.python.org/downloads/)
- [pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html)
- [pg_restore](https://www.postgresql.org/docs/16/app-pgrestore.html)
- [psql](https://www.postgresql.org/docs/16/app-psql.html)

### Ubuntu

```bash
sudo apt-get update && sudo apt-get upgrade
sudo apt-get install postgresql-client
```

### Install dependencies

#### PIP

```bash
pip install -r requirements.txt
```

#### Poetry

```bash
curl -sSL https://install.python-poetry.org | python3 -
poetry shell
poetry install
```

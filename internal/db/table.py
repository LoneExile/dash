import subprocess

from internal.db import Db
from pkg.aws.s3 import S3
from pkg.config import cfg
from pkg.database.postgres.helpers import Helpers
from pkg.aws import Aws
from smart_open import open

s3 = S3()
aws = Aws()


class DbTable(Db):
    def __init__(self):
        super().__init__()
        self.name_escaping = cfg.Postgres.PgNameEscaping
        self.helpers = Helpers()

    def list_tables(self, db_target: str, schema: str = "public"):
        print(f"Listing tables in {db_target} schema {schema}")
        try:
            if self.dbm.conn is None:
                self.dbm.init_connection(db_target, schema)

            table_list = self.dbm.list_tables(db_target, schema)
            self.fmt.print_table(table_list, ["Table Name", "Size"])

            total_table_size = str(self.dbm.sum_table_sizes(db_target, schema))
            self.fmt.print(
                f"Total Table Size: [bold green]{total_table_size} [/bold green]"
            )
        finally:
            self.dbm.close_connection()

    def backup_table_s3(
        self, table_name, aws_key=None, db_target=None, aws_bucket=None
    ):
        chunk_size = 8 * 1024

        try:
            if self.name_escaping:
                table_name = self.helpers.escape_name(table_name)

            if self.dbm.conn is None:
                self.dbm.init_connection(db_target)

            pg_dump_path = self.dbm.get_pg_dump_path()

            command = (
                f"{pg_dump_path} --host {self.dbm.host} --port {self.dbm.port} "
                f"--username {self.dbm.user} --format plain --column-inserts "
                f"--verbose --table {table_name} "
                f"--create --if-exists {db_target} -c"
            )

            session = s3.session

            url = f"s3://{aws_bucket}/{aws_key}"

            with subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={"PGPASSWORD": self.dbm.password},
                shell=True,
            ) as process:
                task_id = self.progress.add_task(
                    f"Uploading [blue]{self.table_name_original}[/blue]", start=False
                )
                total_bytes_written = 0

                with open(
                    url, "wb", transport_params={"client": session.client("s3")}
                ) as fout:
                    while True:
                        output = process.stdout.read(chunk_size)
                        if not output:
                            break
                        fout.write(output.encode("utf-8"))
                        total_bytes_written += len(output)
                        self.progress.update(task_id, advance=len(output))

                _, stderr = process.communicate()
                if process.returncode != 0:
                    raise Exception(f"pg_dump failed: {stderr}")

                self.progress.update(
                    task_id,
                    completed=total_bytes_written,  # , visible=False
                    description=f"Uploaded [blue]{self.table_name_original}[/blue]",
                )

        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")

    def restore_table_from_s3(self, aws_key, db_target, aws_bucket):
        session = s3.session
        s3_client = session.client("s3")
        s3_url = f"s3://{aws_bucket}/{aws_key}"

        try:
            if self.dbm.conn is None:
                self.dbm.init_connection(db_target)

            pg_restore_path = "psql"
            command = (
                f"{pg_restore_path} --host {self.dbm.host} --port {self.dbm.port} "
                f"--username {self.dbm.user} --dbname {db_target}"
            )

            with open(
                s3_url, "rb", transport_params={"client": s3_client}
            ) as fin, subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                text=True,
                env={"PGPASSWORD": self.dbm.password},
                shell=True,
            ) as process:
                for chunk in fin:
                    process.stdin.write(chunk.decode("utf-8"))

                process.stdin.close()
                process.wait()

                if process.returncode != 0:
                    raise Exception(
                        f"psql restore failed with return code {process.returncode}"
                    )

        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")

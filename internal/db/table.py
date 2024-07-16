import subprocess
import os

from internal.db import Db
from pkg.aws.s3 import S3
from pkg.config import cfg
from pkg.database.postgres.helpers import Helpers
from pkg.aws import Aws
from smart_open import open
from botocore.exceptions import BotoCoreError, ClientError

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
                f"{pg_dump_path} -Fc --host {self.dbm.host} --port {self.dbm.port} "
                f"--username {self.dbm.user} "
                f"--verbose --table {table_name} "
                f"--create --if-exists {db_target} -c"
            )

            session = s3.session

            url = f"s3://{aws_bucket}/{aws_key}"

            with subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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
                        fout.write(output)
                        total_bytes_written += len(output)
                        self.progress.update(task_id, advance=len(output))

                _, stderr = process.communicate()
                if process.returncode != 0:
                    raise Exception(f"pg_dump failed: {stderr}")

                self.progress.update(
                    task_id,
                    completed=total_bytes_written,
                    description=f"Uploaded [blue]{self.table_name_original}[/blue]",
                )

        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")
            os.exit(1)

    # def restore_table_from_s3(self, s3_url, db_target):
    #     session = s3.session
    #     s3_client = session.client("s3")

    #     try:
    #         if self.dbm.conn is None:
    #             self.dbm.init_connection(db_target)

    #         pg_restore_path = self.dbm.get_pg_restore_path()

    #         command = (
    #             f"{pg_restore_path} --host {self.dbm.host} --port {self.dbm.port} "
    #             f"--username {self.dbm.user} --dbname {db_target} --disable-triggers --verbose"
    #         )

    #         with open(
    #             s3_url, "rb", transport_params={"client": s3_client}
    #         ) as fin, subprocess.Popen(
    #             command,
    #             stdin=subprocess.PIPE,
    #             env={"PGPASSWORD": self.dbm.password},
    #             shell=True,
    #         ) as process:
    #             i = 0
    #             for chunk in fin:
    #                 process.stdin.write(chunk)
    #                 i += 1
    #             print(i)

    #             process.stdin.close()
    #             process.wait()

    #             if process.returncode != 0:
    #                 raise Exception(
    #                     f"psql restore failed with return code {process.returncode}"
    #                 )

    #     except Exception as e:
    #         self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")
    def restore_table_from_s3(self, s3_url, db_target):
        session = s3.session
        s3_client = session.client("s3")

        try:
            if self.dbm.conn is None:
                self.dbm.init_connection(db_target)

            pg_restore_path = self.dbm.get_pg_restore_path()

            command = (
                f"{pg_restore_path} --host {self.dbm.host} --port {self.dbm.port} "
                f"--username {self.dbm.user} --dbname {db_target} --disable-triggers --verbose"
            )

            with open(
                s3_url, "rb", transport_params={"client": s3_client}
            ) as fin, subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                env={"PGPASSWORD": self.dbm.password},
                shell=True,
            ) as process:
                try:
                    for chunk in fin:
                        process.stdin.write(chunk)
                    process.stdin.close()
                    process.wait()

                    if process.returncode != 0:
                        raise Exception(
                            f"psql restore failed with return code {process.returncode}"
                        )
                except (BotoCoreError, ClientError) as s3_error:
                    process.terminate()
                    raise Exception(f"Error reading from S3: {s3_error}")
                except Exception as write_error:
                    process.terminate()
                    raise Exception(f"Error during restore process: {write_error}")
                finally:
                    process.stdin.close()
                    process.wait()

        except Exception as e:
            self.fmt.print(f"An error occurred: [bold red]{e}[/bold red]")

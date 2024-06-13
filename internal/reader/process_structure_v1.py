import os
from enum import Enum
from time import sleep
from pkg.aws.s3 import S3

import yaml
from internal.db.table import DbTable
from internal.reader import Reader

tb = DbTable()
s3 = S3()


class ModeKeys(Enum):
    INSPECT = 1
    BACKUP_CREATE_TABLE = 2
    RESORE_TABLE = 3


class DbBackupKeys(Enum):
    BACKUP_TABLE_NAME = "BACKUP_TABLE_NAME"
    BACKUP_TARGET_FILE = "backup.sql"
    CLEAN_TARGET_FILE = "delete.sql"


class DbInspectionKeys(Enum):
    INSPECTION_TARGET_FILE = "size.sql"


class AppendixKeys(Enum):
    CHAPTERS = "chapters"
    QUERIES = "queries"
    TYPE = "type"
    NAME = "name"
    DB = "db"


class ProcessStructureV1(Reader):
    def __init__(self):
        super().__init__()
        self.result = []

    def process_structure_v1(
        self,
        structure,
        mode=ModeKeys.INSPECT,
        base_path="",
        depth=0,
        indexer=None,
        sum=None,
        curr_order_list=None,
    ):
        if indexer is None:
            indexer = []
        sum = sum or [0]

        dirs, files = self._separate_dirs_files(structure)
        tb.progress = self.progress

        if curr_order_list is None:
            curr_order_list = []

        self._process_files(files, base_path, mode, indexer, sum)

        for dir_name, sub_structure in self._order_directories(
            dirs, curr_order_list
        ).items():
            if not dir_name.startswith("_"):
                current_path = os.path.join(base_path, dir_name)
                new_curr_order_list = self._get_order_list(dir_name)
                self.process_structure_v1(
                    sub_structure,
                    mode,
                    current_path,
                    depth + 1,
                    indexer,
                    sum,
                    new_curr_order_list,
                )

        if mode == ModeKeys.INSPECT and base_path == "":
            sleep(0.01)
            self._print_results(self.result, sum)

        if mode == ModeKeys.BACKUP_CREATE_TABLE and base_path == "":
            if self.s3_bucket is None:
                self._create_appendix_file(
                    self.cfg.Postgres.PgBackupDir + self.dir_name
                )
            else:
                self._create_appendix_file_s3()

    def _separate_dirs_files(self, structure):
        dirs = {}
        files = {}
        for k, v in structure.items():
            if isinstance(v, dict):
                dirs[k] = v
            else:
                files[k] = v
        return dirs, files

    def _get_order_list(self, dir_name):
        return [
            item[AppendixKeys.NAME.value]
            for item in self.appendix[AppendixKeys.CHAPTERS.value]
            .get(dir_name, {})
            .get(AppendixKeys.QUERIES.value, [])
        ]

    def _order_directories(self, sub_structure, curr_order_list):
        return {
            k: v
            for k, v in sorted(
                sub_structure.items(),
                key=lambda x: (
                    curr_order_list.index(x[0])
                    if x[0] in curr_order_list
                    else len(curr_order_list)
                ),
            )
        }

    def _process_files(self, files, base_path, mode, indexer, sum):
        desired_order = ["size.sql", "select.sql", "backup.sql", "delete.sql"]
        ordered_files = {k: files[k] for k in desired_order if k in files}
        ordered_files.update({k: v for k, v in files.items() if k not in ordered_files})

        for file_name in ordered_files.keys():
            current_path = os.path.join(base_path, file_name)
            current_dir = os.path.basename(base_path)
            upper_dir = os.path.basename(os.path.dirname(base_path))

            if file_name.endswith(".sql") and not file_name.startswith("_"):
                if file_name == "index.sql":
                    indexer[:] = self.pg.get_data_list(
                        current_path,
                        self.appendix[AppendixKeys.CHAPTERS.value][current_dir][
                            AppendixKeys.DB.value
                        ],
                    )
                curr_order = self.appendix[AppendixKeys.CHAPTERS.value].get(
                    upper_dir, []
                )
                if curr_order:
                    table_list = [
                        item[AppendixKeys.NAME.value]
                        for item in self.appendix[AppendixKeys.CHAPTERS.value][
                            upper_dir
                        ][AppendixKeys.QUERIES.value]
                        if item[AppendixKeys.TYPE.value] == "table"
                    ]
                    upper_upper_dir = os.path.basename(
                        os.path.dirname(os.path.dirname(base_path))
                    )
                    sql_file_path = self._get_sql_file_path(
                        current_dir, upper_dir, upper_upper_dir, mode
                    )
                    self._handle_sql_file(
                        mode,
                        file_name,
                        current_path,
                        current_dir,
                        upper_dir,
                        table_list,
                        indexer,
                        sql_file_path,
                        sum,
                    )

    def _get_sql_file_path(self, current_dir, upper_dir, upper_upper_dir, mode=None):
        if self.s3_bucket:
            return os.path.join(
                self.dir_name
                + "/"
                + upper_upper_dir
                + "/"
                + upper_dir
                + "/"
                + current_dir
                + "/"
            )
        elif mode == ModeKeys.RESORE_TABLE or mode == ModeKeys.BACKUP_CREATE_TABLE:
            return os.path.join(
                self.cfg.Postgres.PgBackupDir
                + self.dir_name
                + "/"
                + upper_upper_dir
                + "/"
                + upper_dir
                + "/"
                + current_dir
                + "/"
            )
        elif mode == ModeKeys.INSPECT:
            return os.path.join(
                self.cfg.Books.Location
                + self.dir_name
                + "/"
                + upper_upper_dir
                + "/"
                + upper_dir
                + "/"
                + current_dir
                + "/"
            )

    def _handle_sql_file(
        self,
        mode,
        file_name,
        current_path,
        current_dir,
        upper_dir,
        table_list,
        indexer,
        sql_file_path,
        sum,
    ):
        db = self.appendix[AppendixKeys.CHAPTERS.value][upper_dir][
            AppendixKeys.DB.value
        ]
        if (
            mode == ModeKeys.INSPECT
            and file_name == DbInspectionKeys.INSPECTION_TARGET_FILE.value
        ):
            self._inspect_size_sql(current_path, db, current_dir, upper_dir, sum)
        elif mode == ModeKeys.INSPECT and current_dir in table_list:
            self._inspect_table_size(current_path, indexer, db, sum)
        elif mode == ModeKeys.BACKUP_CREATE_TABLE:
            self._backup_table(
                file_name,
                current_path,
                current_dir,
                db,
                sql_file_path,
                indexer,
                table_list,
            )
        elif mode == ModeKeys.RESORE_TABLE:
            if current_dir in table_list:
                print(f"Table: {file_name}")
                self.pg.run_query_psql(current_path, db)
            else:
                temp_table = "temp_backup_" + current_dir
                print(f"Partial: {file_name} {current_dir}")
                self.pg.run_query_psql(current_path, db)
                self.pg.insert_data_from_table(temp_table, current_dir, db)
                self.pg.drop_table(temp_table, db)

    def _inspect_size_sql(self, current_path, db, current_dir, upper_dir, sum):
        value = self.pg.get_data_single(current_path, db)
        if value:
            sum[0] += value
            value = self.utils.get_pretty_size(value)
        else:
            value = self.utils.get_pretty_size(0)
        self.result.append((current_dir, value, upper_dir, "partial"))

    def _inspect_table_size(self, current_path, indexer, db, sum):
        for index in indexer:
            list_table = self.pg.get_data_single_by_id(current_path, index, db)
            if list_table:
                for table in list_table:
                    value = self.pg.get_table_size(table, db)
                    if value:
                        sum[0] += value
                    self.result.append(
                        (table, self.utils.get_pretty_size(value), "", "table")
                    )

    def _backup_table(
        self,
        file_name,
        current_path,
        current_dir,
        db,
        sql_file_path,
        indexer,
        table_list,
    ):
        if file_name == DbBackupKeys.BACKUP_TARGET_FILE.value:
            self._create_backup_table(current_path, current_dir, db, sql_file_path)
        elif current_dir in table_list:
            for index in indexer:
                list_table = self.pg.get_data_single_by_id(current_path, index, db)
                if list_table:
                    for table in list_table:
                        self._backup_specific_table(table, db, sql_file_path)
        if self.clean and file_name == DbBackupKeys.CLEAN_TARGET_FILE.value:
            self.pg.run_query(current_path, db)

    def _create_backup_table(self, current_path, current_dir, db, sql_file_path):
        db_backup_table_name = "temp_backup_" + current_dir
        try:
            self.pg.run_query_template(
                current_path, db, BACKUP_TABLE_NAME=db_backup_table_name
            )
            if self.s3_bucket:
                tb.table_name_original = current_dir
                self.status.update(
                    "[bold magenta1]Status = Uploading to S3[/bold magenta1]"
                )
                tb.backup_table_s3(
                    db_backup_table_name,
                    sql_file_path + current_dir + ".sql",
                    db,
                    self.s3_bucket,
                )
            else:
                print(f"Backup partial: {current_dir}")
                self.bak.backup_table(
                    db_backup_table_name, current_dir, db, sql_file_path
                )
        except Exception as e:
            self.fmt.print(f"Error: {e}")
        finally:
            self.pg.drop_table(db_backup_table_name, db)

    def _backup_specific_table(self, table, db, sql_file_path):
        if self.s3_bucket:
            tb.table_name_original = table
            self.status.update(
                "[bold magenta1]Status = Uploading to S3[/bold magenta1]"
            )
            tb.backup_table_s3(
                table, sql_file_path + table + ".sql", db, self.s3_bucket
            )
        else:
            print(f"Backup table: {table}")
            self.bak.backup_table(table, table, db, sql_file_path)

        if self.clean:
            self.pg.drop_table(table, db)

    def _create_appendix_file(self, backup_dir):
        appendix_file_path = os.path.join(backup_dir, "appendix.yaml")
        with open(appendix_file_path, "w") as file:
            yaml.dump(self.appendix, file)

    def _create_appendix_file_s3(self):
        try:
            s3_client = s3.session.client("s3")
            s3_key = os.path.join(self.dir_name, "appendix.yaml")

            with open(self.appendix_file_path, "rb") as file:
                s3_client.upload_fileobj(file, self.s3_bucket, s3_key)
        except Exception as e:
            print(f"An error occurred while uploading to S3: {e}")

    def _print_results(self, sql_results, sum):
        self.fmt.print_table(sql_results, ["Table Name", "Size", "Chapter", "type"])
        self.fmt.print(
            f"Total Size: [bold green]{self.utils.get_pretty_size(sum[0])}[/bold green]"
        )
        self.fmt.print(f"Total Tables: [bold green]{len(sql_results)}[/bold green]")

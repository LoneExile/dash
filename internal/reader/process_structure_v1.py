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
    HOOK = 4


class DbQueryKeys(Enum):
    BACKUP_TABLE_NAME = "BACKUP_TABLE_NAME"
    BACKUP_TABLE_PREFIX = "temp_backup_"
    BACKUP_TARGET_FILE = "backup.sql"
    CLEAN_TARGET_FILE = "delete.sql"
    INSPECTION_TARGET_FILE = "size.sql"
    SELECT_TARGET_FILE = "select.sql"
    INDEX_TARGET_FILE = "index.sql"
    TYPE_TABLE = "table"
    TYPE_PARTIAL = "partial"


class AppendixKeys(Enum):
    FILE = "appendix.yaml"
    CHAPTERS = "chapters"
    QUERIES = "queries"
    TYPE = "type"
    NAME = "name"
    DB = "db"
    HOOK = "hook"


class ProcessStructureV1(Reader):
    def __init__(self):
        super().__init__()
        self.result = []
        self.running_chapter = []

    def restore_s3_v1(self, s3_contents):
        chapters = list(self.appendix.get(AppendixKeys.CHAPTERS.value, {}).keys())
        for chapter in chapters:
            db = self.appendix[AppendixKeys.CHAPTERS.value][chapter][
                AppendixKeys.DB.value
            ]
            if self.is_hook:
                hook = self.appendix[AppendixKeys.HOOK.value]
                hook_steps = [item[AppendixKeys.NAME.value] for item in hook]
                hook_steps.reverse()
                for step in hook_steps:
                    hook_path = os.path.join(
                        self.dir_name, AppendixKeys.HOOK.value, chapter, step
                    )
                    filtered_list = list(filter(lambda x: hook_path in x, s3_contents))
                    for item in filtered_list:
                        if item.endswith(".sql") or item.endswith(".dump"):
                            prefix = "s3://" + os.path.join(self.s3_bucket, item)
                            print(f"Prefix hook: {prefix}")
                            temp_table = (
                                DbQueryKeys.BACKUP_TABLE_PREFIX.value
                                + chapter
                                + "_"
                                + AppendixKeys.HOOK.value
                                + "_"
                                + step
                            )
                            tb.restore_table_from_s3(prefix, db)
                            self.pg.insert_data_from_table(temp_table, step, db)
                            self.pg.drop_table(temp_table, db)

            step = self.appendix[AppendixKeys.CHAPTERS.value][chapter][
                AppendixKeys.QUERIES.value
            ]
            for item in step:
                if item[AppendixKeys.TYPE.value] == DbQueryKeys.TYPE_TABLE.value:
                    prefix = os.path.join(
                        self.dir_name,
                        AppendixKeys.CHAPTERS.value,
                        chapter,
                        item[AppendixKeys.NAME.value],
                    )
                    filtered_list = list(filter(lambda x: prefix in x, s3_contents))
                    for item in filtered_list:
                        if item.endswith(".sql") or item.endswith(".dump"):
                            prefix = "s3://" + os.path.join(self.s3_bucket, item)
                            print(f"Prefix table: {prefix}")
                            tb.restore_table_from_s3(prefix, db)
                elif item[AppendixKeys.TYPE.value] == DbQueryKeys.TYPE_PARTIAL.value:
                    prefix = "s3://" + os.path.join(
                        self.s3_bucket,
                        self.dir_name,
                        AppendixKeys.CHAPTERS.value,
                        chapter,
                        item[AppendixKeys.NAME.value],
                        item[AppendixKeys.NAME.value] + ".dump",
                    )
                    print(f"Prefix: {prefix}")

                    temp_table = (
                        DbQueryKeys.BACKUP_TABLE_PREFIX.value
                        + chapter
                        + "_"
                        + item[AppendixKeys.NAME.value]
                    )
                    tb.restore_table_from_s3(prefix, db)
                    self.pg.insert_data_from_table(
                        temp_table, item[AppendixKeys.NAME.value], db
                    )
                    self.pg.drop_table(temp_table, db)

    def process_structure_v1(
        self,
        structure,
        mode=ModeKeys.INSPECT,
        base_path="",
        depth=0,
        indexer=None,
        sum=None,
    ):
        if indexer is None:
            indexer = []
        sum = sum or [0]

        dirs, files = self._separate_dirs_files(structure)
        tb.progress = self.progress

        # if self.current_chapter != "":
        #     self.step = self.appendix[AppendixKeys.CHAPTERS.value][self.current_chapter][
        #         AppendixKeys.QUERIES.value
        #     ]
        self._process_files(files, base_path, mode, indexer, sum)

        for dir_name, sub_structure in self._order_directories(
            dirs, self.running_chapter
        ).items():
            if (
                not dir_name.startswith("_")
                and AppendixKeys.HOOK.value not in base_path
                ## TODO: validate if the appendix is not empty
                # and self.appendix.get(AppendixKeys.CHAPTERS.value).get(dir_name, {}) != {}
            ):
                current_path = os.path.join(base_path, dir_name)
                new_curr_order_list = self._get_order_list(dir_name)
                if len(new_curr_order_list) > 0:
                    self.running_chapter = new_curr_order_list
                    self.current_chapter = os.path.basename(current_path)
                self.process_structure_v1(
                    sub_structure,
                    mode,
                    current_path,
                    depth + 1,
                    indexer,
                    sum,
                )
                if self.running_chapter[-1] == dir_name and self.is_hook:
                    self._process_hooks(mode, indexer, sum)

                if (
                    self.running_chapter[-1] == dir_name
                    and mode is not ModeKeys.RESORE_TABLE
                ):
                    self.pg.close_connection()

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
        # desired_order = ["size.sql", "select.sql", "backup.sql", "delete.sql"]
        desired_order = [
            DbQueryKeys.INSPECTION_TARGET_FILE.value,
            DbQueryKeys.SELECT_TARGET_FILE.value,
            DbQueryKeys.BACKUP_TARGET_FILE.value,
            DbQueryKeys.CLEAN_TARGET_FILE.value,
        ]
        ordered_files = {k: files[k] for k in desired_order if k in files}
        ordered_files.update({k: v for k, v in files.items() if k not in ordered_files})

        for file_name in ordered_files.keys():
            current_path = os.path.join(base_path, file_name)
            current_dir = os.path.basename(base_path)
            upper_dir = os.path.basename(os.path.dirname(base_path))

            if (
                file_name.endswith(".sql") or file_name.endswith(".dump")
            ) and not file_name.startswith("_"):
                if self.is_date:
                    is_where_clause_date = ""
                else:
                    is_where_clause_date = "--"
                if file_name == DbQueryKeys.INDEX_TARGET_FILE.value:
                    indexer[:] = self.pg.get_data_list(
                        current_path,
                        self.appendix[AppendixKeys.CHAPTERS.value][current_dir][
                            AppendixKeys.DB.value
                        ],
                        IS_WHERE_DATE=is_where_clause_date,
                        START_DATE=self.start_date,
                        END_DATE=self.end_date,
                    )
                curr_order = self.appendix[AppendixKeys.CHAPTERS.value].get(
                    upper_dir, []
                )

                if curr_order:
                    query_names = [
                        query[AppendixKeys.NAME.value]
                        for query in curr_order[AppendixKeys.QUERIES.value]
                    ]
                    if current_dir in query_names:
                        table_list = [
                            item[AppendixKeys.NAME.value]
                            for item in self.appendix[AppendixKeys.CHAPTERS.value][
                                upper_dir
                            ][AppendixKeys.QUERIES.value]
                            if item[AppendixKeys.TYPE.value]
                            == DbQueryKeys.TYPE_TABLE.value
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
            return os.path.join(self.dir_name, upper_upper_dir, upper_dir, current_dir)
        elif mode == ModeKeys.RESORE_TABLE or mode == ModeKeys.BACKUP_CREATE_TABLE:
            return os.path.join(
                self.cfg.Postgres.PgBackupDir,
                self.dir_name,
                upper_upper_dir,
                upper_dir,
                current_dir,
            )
        elif mode == ModeKeys.INSPECT:
            return os.path.join(
                self.cfg.Books.Location,
                self.dir_name,
                upper_upper_dir,
                upper_dir,
                current_dir,
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
            and file_name == DbQueryKeys.INSPECTION_TARGET_FILE.value
        ):
            self._inspect_size_sql(
                current_path, db, current_dir, upper_dir, sum, indexer
            )
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
                # self.pg.run_query_psql(current_path, db)
                self.pg.run_query_pg_restore(current_path, db)
            else:
                temp_table = (
                    DbQueryKeys.BACKUP_TABLE_PREFIX.value
                    + self.current_chapter
                    + "_"
                    + current_dir
                )
                print(f"Partial: {current_dir}")
                # self.pg.run_query_psql(current_path, db)
                self.pg.run_query_pg_restore(current_path, db)
                self.pg.insert_data_from_table(temp_table, current_dir, db)
                self.pg.drop_table(temp_table, db)

    def _inspect_size_sql(self, current_path, db, current_dir, upper_dir, sum, indexer):
        id_list = self._id_list(indexer)
        self.status.update(f"[bold green] Getting {current_dir}[/bold green]\n")
        if len(indexer) != 0:
            value = self.pg.get_data__by_list_template(
                current_path, db, ID_LIST=id_list, CURRENT_DIR=current_dir
            )
            if value:
                sum[0] += value
                value = self.utils.get_pretty_size(value)
            else:
                value = self.utils.get_pretty_size(0)
            self.result.append(
                (current_dir, value, upper_dir, DbQueryKeys.TYPE_PARTIAL.value)
            )

    def _inspect_table_size(self, current_path, indexer, db, sum):
        for index in indexer:
            list_table = self.pg.get_data_single_by_id(current_path, index, db)
            if list_table:
                for table in list_table:
                    value = self.pg.get_table_size(table, db)
                    if value:
                        sum[0] += value
                    self.result.append(
                        (
                            table,
                            self.utils.get_pretty_size(value),
                            self.current_chapter,
                            DbQueryKeys.TYPE_TABLE.value,
                        )
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
        if file_name == DbQueryKeys.BACKUP_TARGET_FILE.value:
            self._create_backup_table(
                current_path, current_dir, db, sql_file_path, indexer
            )
        elif current_dir in table_list:
            for index in indexer:
                list_table = self.pg.get_data_single_by_id(current_path, index, db)
                if list_table:
                    for table in list_table:
                        self._backup_specific_table(table, db, sql_file_path)
        if self.clean and file_name == DbQueryKeys.CLEAN_TARGET_FILE.value:
            # self.pg.run_query(current_path, db)
            id_list = self._id_list(indexer)
            if len(indexer) != 0:
                self.pg.run_query_template(current_path, db, ID_LIST=id_list)

    def _create_backup_table(
        self, current_path, current_dir, db, sql_file_path, indexer
    ):
        id_list = self._id_list(indexer)
        if len(indexer) != 0:
            db_backup_table_name = (
                DbQueryKeys.BACKUP_TABLE_PREFIX.value
                + self.current_chapter
                + "_"
                + current_dir
            )
            try:
                self.pg.run_query_template(
                    current_path,
                    db,
                    BACKUP_TABLE_NAME=db_backup_table_name,
                    ID_LIST=id_list,
                )
                if self.s3_bucket:
                    tb.table_name_original = current_dir
                    self.status.update(
                        "[bold magenta1]Status = Uploading to S3[/bold magenta1]"
                    )
                    tb.backup_table_s3(
                        db_backup_table_name,
                        os.path.join(
                            sql_file_path, current_dir + ".dump"
                        ),  # TODO: Fix this
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
                table, os.path.join(sql_file_path, table + ".dump"), db, self.s3_bucket
            )
        else:
            print(f"Backup table: {table}")
            self.bak.backup_table(table, table, db, sql_file_path)

        if self.clean:
            self.pg.drop_table(table, db)

    def _create_appendix_file(self, backup_dir):
        appendix_file_path = os.path.join(backup_dir, AppendixKeys.FILE.value)
        with open(appendix_file_path, "w") as file:
            yaml.dump(self.appendix, file)

    def _create_appendix_file_s3(self):
        try:
            s3_client = s3.session.client("s3")
            s3_key = os.path.join(self.dir_name, AppendixKeys.FILE.value)

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

    def _id_list(self, indexer):
        id_list = ""
        for index in indexer:
            if indexer[-1] == index:
                id_list += "'" + str(index) + "'"
            else:
                id_list += "'" + str(index) + "',"
        return id_list

    def _process_hooks(self, mode, indexer, sum):
        hook_base_path = os.path.join(self.hook_path, AppendixKeys.HOOK.value)

        hooks = self.appendix.get(AppendixKeys.HOOK.value, [])
        db = (
            self.appendix[AppendixKeys.CHAPTERS.value]
            .get(self.current_chapter, {})
            .get(AppendixKeys.DB.value)
        )

        id_list = self._id_list(indexer)
        if mode == ModeKeys.RESORE_TABLE:
            hooks.reverse()

        for hook in hooks:
            db_backup_table_name = (
                DbQueryKeys.BACKUP_TABLE_PREFIX.value
                + self.current_chapter
                + "_"
                + AppendixKeys.HOOK.value
                + "_"
                + hook.get(AppendixKeys.NAME.value)
            )
            if (
                hook.get(AppendixKeys.TYPE.value) == DbQueryKeys.TYPE_PARTIAL.value
                and mode == ModeKeys.INSPECT
                # and len(indexer) != 0
            ):
                inspect_path = os.path.join(
                    hook_base_path,
                    hook.get(AppendixKeys.NAME.value),
                    DbQueryKeys.INSPECTION_TARGET_FILE.value,
                )

                if len(indexer) != 0:
                    value = self.pg.get_data__by_list_template(
                        inspect_path, db, ID_LIST=id_list
                    )
                    if value:
                        sum[0] += value
                        value = self.utils.get_pretty_size(value)
                    else:
                        value = self.utils.get_pretty_size(0)
                    self.result.append(
                        (
                            hook.get(AppendixKeys.NAME.value),
                            value,
                            self.current_chapter,
                            os.path.join(
                                AppendixKeys.HOOK.value, DbQueryKeys.TYPE_PARTIAL.value
                            ),
                        )
                    )
                else:
                    self.result.append(
                        (
                            hook.get(AppendixKeys.NAME.value),
                            self.utils.get_pretty_size(0),
                            self.current_chapter,
                            os.path.join(
                                AppendixKeys.HOOK.value, DbQueryKeys.TYPE_PARTIAL.value
                            ),
                        )
                    )
            elif (
                hook.get(AppendixKeys.TYPE.value) == DbQueryKeys.TYPE_PARTIAL.value
                and mode == ModeKeys.BACKUP_CREATE_TABLE
            ):
                backup_path = os.path.join(
                    hook_base_path,
                    hook.get(AppendixKeys.NAME.value),
                    DbQueryKeys.BACKUP_TARGET_FILE.value,
                )
                delete_path = os.path.join(
                    hook_base_path,
                    hook.get(AppendixKeys.NAME.value),
                    DbQueryKeys.CLEAN_TARGET_FILE.value,
                )

                target_backup_path = os.path.join(
                    self.cfg.Postgres.PgBackupDir + self.dir_name,
                    AppendixKeys.HOOK.value,
                    self.current_chapter,
                    hook.get(AppendixKeys.NAME.value),
                )
                if len(indexer) != 0:
                    try:
                        self.pg.run_query_template(
                            backup_path,
                            db,
                            BACKUP_TABLE_NAME=db_backup_table_name,
                            ID_LIST=id_list,
                        )
                        if self.s3_bucket:
                            tb.table_name_original = hook.get(AppendixKeys.NAME.value)
                            self.status.update(
                                "[bold magenta1]Status = Uploading to S3[/bold magenta1]"
                            )
                            tb.backup_table_s3(
                                db_backup_table_name,
                                os.path.join(
                                    self.dir_name,
                                    AppendixKeys.HOOK.value,
                                    self.current_chapter,
                                    hook.get(AppendixKeys.NAME.value),
                                    hook.get(AppendixKeys.NAME.value) + ".dump",
                                ),
                                db,
                                self.s3_bucket,
                            )
                        else:
                            print(
                                f"Backup partial: {hook.get(AppendixKeys.NAME.value)}"
                            )
                            self.bak.backup_table(
                                db_backup_table_name,
                                hook.get(AppendixKeys.NAME.value),
                                db,
                                target_backup_path,
                            )
                    except Exception as e:
                        self.fmt.print(f"Error: {e}")
                    finally:
                        if self.clean:
                            self.pg.run_query_template(delete_path, db, ID_LIST=id_list)
                        self.pg.drop_table(db_backup_table_name, db)
            elif (
                hook.get(AppendixKeys.TYPE.value) == DbQueryKeys.TYPE_PARTIAL.value
                and mode == ModeKeys.RESORE_TABLE
            ):
                target_backup_file = os.path.join(
                    self.dir_name,
                    AppendixKeys.HOOK.value,
                    self.current_chapter,
                    hook.get(AppendixKeys.NAME.value),
                    hook.get(AppendixKeys.NAME.value) + ".dump",
                )
                print(f"Partial(Hook): {hook.get(AppendixKeys.NAME.value)}")

                # self.pg.run_query_psql(target_backup_file, db)
                self.pg.run_query_pg_restore(target_backup_file, db)
                self.pg.insert_data_from_table(
                    db_backup_table_name, hook.get(AppendixKeys.NAME.value), db
                )
                self.pg.drop_table(db_backup_table_name, db)

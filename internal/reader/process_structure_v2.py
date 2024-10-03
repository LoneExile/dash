import os

# import shutil
from enum import Enum
from time import sleep
from pkg.aws.s3 import S3
from internal.db.sqlite.database import DbDatabaseSqlite

import yaml
from internal.db.pg.table import DbTable
from internal.reader import Reader
from internal.utils import Utils
from pkg.config import cfg
from pkg.database.postgres.helpers import Helpers

sqlite = DbDatabaseSqlite()
tb = DbTable()
s3 = S3()
utils = Utils()


class ModeKeysV2(Enum):
    INSPECT = 1
    BACKUP_CREATE_TABLE = 2
    RESORE_TABLE = 3
    HOOK = 4


class DbQueryKeys(Enum):
    BACKUP_TABLE_NAME = "BACKUP_TABLE_NAME"
    BACKUP_TABLE_PREFIX = "t_"
    BACKUP_TARGET_FILE = "backup.sql"
    CLEAN_TARGET_FILE = "delete.sql"
    INSPECTION_TARGET_FILE = "size.sql"
    SELECT_TARGET_FILE = "select.sql"
    INDEX_TARGET_FILE = "index.sql"
    TYPE_TABLE = "table"
    TYPE_PARTIAL = "partial"


class AppendixKeys(Enum):
    DIR = "appendix"
    FILE = "appendix.yaml"
    INDEX = "index.db"
    CHAPTERS = "chapters"
    QUERIES = "queries"
    TYPE = "type"
    NAME = "name"
    DB = "db"
    HOOK = "hook"


class ProcessStructureV2(Reader):
    def __init__(self):
        super().__init__()
        self.start_date = None
        self.end_date = None
        self.result = []
        self.running_chapter = []
        self.indexer_date = []
        self.selected_list = []
        self.name_escaping = cfg.Postgres.PgNameEscaping
        self.helpers = Helpers()

    def restore_v2(self):
        get_chapters = self.appendix.get(AppendixKeys.CHAPTERS.value, {})
        chapters = list(get_chapters.keys())

        for chapter in chapters:
            chapter_item = get_chapters.get(chapter, {})
            db = chapter_item.get(AppendixKeys.DB.value)
            chapter_queries = chapter_item.get(AppendixKeys.QUERIES.value, [])
            queries_file = []
            for query in chapter_queries:
                if query[AppendixKeys.TYPE.value] != DbQueryKeys.TYPE_TABLE.value:
                    queries_file.append(query[AppendixKeys.NAME.value])
            restore_order = []
            if self.is_hook:
                hooks = self.appendix.get(AppendixKeys.HOOK.value, [])
                for hook in hooks:
                    restore_order.append(hook[AppendixKeys.NAME.value])
                restore_order.reverse()
                for query in queries_file:
                    restore_order.append(query)
            else:
                for query in queries_file:
                    restore_order.append(query)

            # s3.client.download_fileobj(bucket, dir_name + "/appendix.yaml", file)
            if self.s3_bucket:
                print("Restoring s3 v2", chapter)
                print(f"Order: {restore_order}")
                conn = sqlite.init(os.path.join(cfg.Restore.Temp, "index.db"))
                cur = conn.cursor()
                # if self.start_date is None and self.appendix.get("date") is not None:
                #     self.start_date = self.appendix["date"]["start"]
                #     self.end_date = self.appendix["date"]["end"]
                if self.start_date:
                    print("start_date: ", self.start_date)
                    cur.execute(
                        "SELECT * FROM appendix WHERE chapters = ? and created_at BETWEEN ? AND ?",
                        (chapter, self.start_date, self.end_date),
                    )
                elif len(self.selected_list) > 0:
                    print("selected_list: ", self.selected_list)
                    if self.selected_list:
                        placeholders = ",".join("?" for _ in self.selected_list)
                        query = f"SELECT * FROM appendix WHERE chapters = ? and id IN ({placeholders})"
                        params = (chapter, *self.selected_list)
                    else:
                        query = "SELECT * FROM appendix WHERE chapters = ?"
                        params = (chapter,)
                    cur.execute(query, params)
                elif self.custom:
                    print("custom: ", self.custom)
                    custom_query = ""
                    custom_dict = {}
                    for item in self.custom:
                        key, value = item.split("=", 1)
                        custom_dict[key] = value
                    for key, value in custom_dict.items():
                        custom_query += f"AND {key} = '{value}' "
                    query = f"SELECT * FROM appendix WHERE chapters = ? {custom_query}"
                    params = (chapter,)
                    cur.execute(query, params)
                else:
                    print("chapter: ", chapter)
                    cur.execute("SELECT * FROM appendix WHERE chapters = ?", (chapter,))
                rows = cur.fetchall()
                for row in rows:
                    # print(f"rows: {row}")
                    # print(
                    #     f"s3 bucket and prefix: {self.s3_bucket} {os.path.join(row[1])}"
                    # )
                    list_files_in_dir = []
                    get_list_files_in_dir = s3.client.list_objects_v2(
                        Bucket=self.s3_bucket,
                        Prefix=row[1],
                    )
                    if "Contents" in get_list_files_in_dir:
                        for obj in get_list_files_in_dir["Contents"]:
                            list_files_in_dir.append(obj["Key"])
                    else:
                        print("Folder is empty.")

                    # os.path.basename(obj["Key"])
                    # print(f"list_files_in_dir: {list_files_in_dir}")
                    for i in range(len(restore_order)):
                        file_name = restore_order[i] + ".dump"
                        print(f"restoring: {row[0]}: {file_name}")
                        db_backup_table_name = (
                            DbQueryKeys.BACKUP_TABLE_PREFIX.value + restore_order[i]
                        )
                        # if file_name in list_files_in_dir:
                        #     print("file_name: ", file_name)

                        for file in list_files_in_dir:
                            if file_name == os.path.basename(file):
                                prefix = "s3://" + os.path.join(self.s3_bucket, file)
                                print(f"file: {prefix}")
                                tb.restore_table_from_s3(prefix, db)
                                self.pg.insert_data_from_table(
                                    db_backup_table_name, restore_order[i], db
                                )
                                self.pg.drop_table(db_backup_table_name, db)

                                list_files_in_dir.remove(file)
                    for left_over in list_files_in_dir:
                        prefix = "s3://" + os.path.join(self.s3_bucket, left_over)
                        print(f"left_over: {prefix}")
                        tb.restore_table_from_s3(prefix, db)

            else:
                conn = sqlite.init(os.path.join(self.dir_name, "index.db"))
                cur = conn.cursor()
                if self.start_date:
                    cur.execute(
                        "SELECT * FROM appendix WHERE chapters = ? and created_at BETWEEN ? AND ?",
                        (chapter, self.start_date, self.end_date),
                    )
                elif self.selected_list:
                    if self.selected_list:
                        placeholders = ",".join("?" for _ in self.selected_list)
                        query = f"SELECT * FROM appendix WHERE chapters = ? and id IN ({placeholders})"
                        params = (chapter, *self.selected_list)
                    else:
                        query = "SELECT * FROM appendix WHERE chapters = ?"
                        params = (chapter,)
                    cur.execute(query, params)
                else:
                    cur.execute("SELECT * FROM appendix WHERE chapters = ?", (chapter,))

                rows = cur.fetchall()
                for row in rows:
                    list_files_in_dir = os.listdir(row[1])
                    for order in restore_order:
                        file_name = order + ".dump"
                        print(f"restoring: {row[0]}: {file_name}")
                        file_loc = os.path.join(
                            row[1],
                            file_name,
                        )
                        db_backup_table_name = (
                            DbQueryKeys.BACKUP_TABLE_PREFIX.value + order
                        )
                        if os.path.exists(file_loc) and file_name in list_files_in_dir:
                            self.pg.run_query_pg_restore(file_loc, db)
                            self.pg.insert_data_from_table(
                                db_backup_table_name, order, db
                            )
                            self.pg.drop_table(db_backup_table_name, db)
                            list_files_in_dir.remove(file_name)
                    for left_over in list_files_in_dir:
                        file_loc = os.path.join(
                            row[1],
                            left_over,
                        )
                        self.pg.run_query_pg_restore(file_loc, db)

    def process_structure_v2(
        self,
        structure,
        mode=ModeKeysV2.INSPECT,
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
                self.process_structure_v2(
                    sub_structure,
                    mode,
                    current_path,
                    depth + 1,
                    indexer,
                    sum,
                )
                if (
                    self.running_chapter
                    and self.running_chapter[-1] == dir_name
                    and self.is_hook
                ):
                    for index in indexer:
                        self._process_hooks(mode, [index], sum)
                    if (
                        mode == ModeKeysV2.BACKUP_CREATE_TABLE
                        and len(self.indexer_date) > 0
                        and len(indexer) == len(self.indexer_date)
                    ):
                        appendix_insert_sql = utils.find_file(
                            "insert_appendix.sql", cfg.Books.Location + self.book
                        )
                        for x in range(len(indexer)):
                            print(f"indexer_date: {indexer[x]}::{self.indexer_date[x]}")

                            loc = os.path.join(
                                self.cfg.Postgres.PgBackupDir,
                                self.dir_name,
                                AppendixKeys.CHAPTERS.value,
                                self.current_chapter,
                                indexer[x],
                            )
                            loc_s3 = os.path.join(
                                self.dir_name,
                                AppendixKeys.CHAPTERS.value,
                                self.current_chapter,
                                indexer[x],
                            )

                            custom_appendix_value = ","
                            custom_appendix_key = ","
                            if self.custom:
                                custom_dict = {}
                                for item in self.custom:
                                    key, value = item.split("=", 1)
                                    custom_dict[key] = value
                                for i, (key, value) in enumerate(custom_dict.items()):
                                    if i == len(custom_dict) - 1:
                                        custom_appendix_value += f"'{value}'"
                                        custom_appendix_key += f"{key}"
                                    else:
                                        custom_appendix_value += f"'{value}',"
                                        custom_appendix_key += f"{key},"
                                # print(
                                #     f"custom_appendix: {custom_appendix_value} :: {appendix_insert_sql[0]}"
                                # )
                            if self.s3_bucket:
                                sqlite.run_query_template(
                                    self.sqlite,
                                    appendix_insert_sql[0],
                                    ID_APPENDIX=indexer[x],
                                    PREFIX_APPENDIX=loc_s3,
                                    CREATED_AT_APPENDIX=self.indexer_date[x],
                                    CHAPTERS_APPENDIX=self.current_chapter,
                                    CUSTOM_APPENDIX_VALUE=custom_appendix_value,
                                    CUSTOM_APPENDIX_KEY=custom_appendix_key,
                                )
                            else:
                                sqlite.run_query_template(
                                    self.sqlite,
                                    appendix_insert_sql[0],
                                    ID_APPENDIX=indexer[x],
                                    PREFIX_APPENDIX=loc,
                                    CREATED_AT_APPENDIX=self.indexer_date[x],
                                    CHAPTERS_APPENDIX=self.current_chapter,
                                    CUSTOM_APPENDIX_VALUE=custom_appendix_value,
                                    CUSTOM_APPENDIX_KEY=custom_appendix_key,
                                )

                if (
                    self.running_chapter
                    and self.running_chapter[-1] == dir_name
                    and mode is not ModeKeysV2.RESORE_TABLE
                ):
                    self.pg.close_connection()

        if mode == ModeKeysV2.INSPECT and base_path == "":
            sleep(0.01)
            self._print_results(self.result, sum)

        if mode == ModeKeysV2.BACKUP_CREATE_TABLE and base_path == "":
            if self.s3_bucket is None:
                self._create_appendix_file(
                    os.path.join(self.cfg.Postgres.PgBackupDir + self.dir_name)
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
                if self.custom:
                    custom_dict = {}
                    custom_query = "WHERE "
                    first_item = True
                    for item in self.custom:
                        key, value = item.split("=", 1)
                        custom_dict[key] = value

                    # print("Custom options:")
                    for key, value in custom_dict.items():
                        if self.name_escaping:
                            key = '"' + key + '"'
                        # print(f"  {key}: {value}")
                        if first_item:
                            custom_query += f"{key} = '{value}' "
                            first_item = False
                        else:
                            custom_query += f"AND {key} = '{value}' "
                else:
                    custom_query = ""
                # print(f"custom_query: {custom_query}")
                if file_name == DbQueryKeys.INDEX_TARGET_FILE.value:
                    indexer[:], self.indexer_date = self.pg.get_data_list_v2(
                        current_path,
                        self.appendix[AppendixKeys.CHAPTERS.value][current_dir][
                            AppendixKeys.DB.value
                        ],
                        IS_WHERE_DATE=is_where_clause_date,
                        START_DATE=self.start_date,
                        END_DATE=self.end_date,
                        CUSTOM_WHERE=custom_query,
                    )
                    # print(f"indexer: {indexer}")
                    # print(f"indexer_date: {self.indexer_date}")
                    upper_upper_dir = os.path.basename(
                        os.path.dirname(os.path.dirname(base_path))
                    )
                curr_order = self.appendix[AppendixKeys.CHAPTERS.value].get(upper_dir, [])

                if curr_order:
                    query_names = [
                        query[AppendixKeys.NAME.value]
                        for query in curr_order[AppendixKeys.QUERIES.value]
                    ]
                    # print(f"Running Chapter: {self.running_chapter}")
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
        elif mode == ModeKeysV2.RESORE_TABLE or mode == ModeKeysV2.BACKUP_CREATE_TABLE:

            return os.path.join(
                self.cfg.Postgres.PgBackupDir,
                self.dir_name,
                upper_upper_dir,
                upper_dir,
                current_dir,
            )
        elif mode == ModeKeysV2.INSPECT:
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
        # print(f"Running Chapter: {self.running_chapter}")
        db = self.appendix[AppendixKeys.CHAPTERS.value][upper_dir][AppendixKeys.DB.value]
        if (
            mode == ModeKeysV2.INSPECT
            and file_name == DbQueryKeys.INSPECTION_TARGET_FILE.value
        ):
            self._inspect_size_sql(current_path, db, current_dir, upper_dir, sum, indexer)
        elif (
            mode == ModeKeysV2.INSPECT
            and current_dir in table_list
            and (file_name == DbQueryKeys.SELECT_TARGET_FILE.value)
        ):
            self._inspect_table_size(current_path, indexer, db, sum)
        elif mode == ModeKeysV2.BACKUP_CREATE_TABLE:
            self._backup_table(
                file_name,
                current_path,
                current_dir,
                db,
                sql_file_path,
                indexer,
                table_list,
            )
        elif mode == ModeKeysV2.RESORE_TABLE and os.path.basename(current_path).endswith(
            ".dump"
        ):
            if current_dir in table_list:
                print(f"Table: {os.path.basename(current_path)}")
                # self.pg.run_query_pg_restore(current_path, db)
            else:
                temp_table = DbQueryKeys.BACKUP_TABLE_PREFIX.value + current_dir
                print(f"Partial: {os.path.basename(current_path)}")
                # self.pg.run_query_pg_restore(current_path, db)
                # self.pg.insert_data_from_table(temp_table, current_dir, db)
                # self.pg.drop_table(temp_table, db)

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
            for index in indexer:
                self._create_backup_table(
                    current_path, current_dir, db, sql_file_path, [index]
                )
        elif current_dir in table_list:
            for index in indexer:
                list_table = self.pg.get_data_single_by_id(current_path, index, db)
                save_path = os.path.dirname(sql_file_path)
                print(
                    f"os.path.basename(sql_file_path): {os.path.basename(sql_file_path)}"
                )
                if list_table:
                    for table in list_table:
                        self._backup_specific_table(
                            table,
                            db,
                            os.path.join(
                                save_path,
                                [index][0],
                                # os.path.basename(sql_file_path), ## more depth
                            ),
                        )
        if self.clean and file_name == DbQueryKeys.CLEAN_TARGET_FILE.value:
            print(f"Cleaning: {current_dir}")
            id_list = self._id_list(indexer)
            if len(indexer) != 0:
                print(f"Cleaning: {id_list}")
                self.pg.run_query_template(
                    current_path, db, ID_LIST=id_list, CURRENT_DIR=current_dir
                )

    def _create_backup_table(self, current_path, current_dir, db, sql_file_path, indexer):
        ## TODO: create a backup each id
        if len(indexer) != 0:
            id_list = self._id_list(indexer)
            print(f"Backup table: {id_list}")
            db_backup_table_name = DbQueryKeys.BACKUP_TABLE_PREFIX.value + current_dir
            try:
                self.pg.run_query_template(
                    current_path,
                    db,
                    BACKUP_TABLE_NAME=db_backup_table_name,
                    ID_LIST=id_list,
                    CURRENT_DIR=current_dir,
                )
                if self.s3_bucket:
                    tb.table_name_original = current_dir
                    self.status.update(
                        "[bold magenta1]Status = Uploading to S3[/bold magenta1]"
                    )
                    print(
                        f"sql_file_path: {os.path.join(os.path.dirname(sql_file_path), indexer[0], current_dir + '.dump')}"
                    )
                    tb.backup_table_s3(
                        db_backup_table_name,
                        os.path.join(
                            # sql_file_path, current_dir + ".dump"
                            os.path.join(
                                os.path.dirname(sql_file_path),
                                indexer[0],
                                current_dir + ".dump",
                            )
                        ),
                        db,
                        self.s3_bucket,
                    )
                else:
                    # print(f"Backup partial: {current_dir}")
                    save_path = os.path.dirname(sql_file_path)
                    print(f"sql_file_path: {os.path.join(save_path,indexer[0])}")
                    self.bak.backup_table(
                        db_backup_table_name,
                        current_dir,
                        db,
                        os.path.join(save_path, indexer[0]),
                    )
            except Exception as e:
                self.fmt.print(f"Error: {e}")
            finally:
                self.pg.drop_table(db_backup_table_name, db)

    def _backup_specific_table(self, table, db, sql_file_path, current_path=None):
        if self.s3_bucket:
            tb.table_name_original = table
            self.status.update("[bold magenta1]Status = Uploading to S3[/bold magenta1]")
            print(f"table:sql_file_path: {sql_file_path}")
            tb.backup_table_s3(
                table, os.path.join(sql_file_path, table + ".dump"), db, self.s3_bucket
            )
        else:
            print(f"Backup table: {table}")
            self.bak.backup_table(table, table, db, sql_file_path)

        if self.clean:
            self.pg.drop_table(table, db)

    def _create_appendix_file(self, backup_dir):
        appendix_file_path = os.path.join(
            backup_dir,
            AppendixKeys.FILE.value,
        )
        with open(appendix_file_path, "w") as file:
            self.appendix["date"] = {"start": self.start_date, "end": self.end_date}
            yaml.dump(self.appendix, file)

    def _create_appendix_file_s3(self):
        try:
            s3_client = s3.session.client("s3")
            s3_key = os.path.join(self.dir_name, AppendixKeys.FILE.value)
            appendix_file = os.path.join(
                self.cfg.Books.Location + self.book,
                AppendixKeys.DIR.value,
                AppendixKeys.FILE.value,
            )
            with open(appendix_file, "rb") as file:
                s3_client.upload_fileobj(file, self.s3_bucket, s3_key)
            index_file = os.path.join(
                self.cfg.Postgres.PgBackupDir, self.dir_name, AppendixKeys.INDEX.value
            )
            s3_key = os.path.join(self.dir_name, AppendixKeys.INDEX.value)
            with open(index_file, "rb") as file:
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
        if mode == ModeKeysV2.RESORE_TABLE:
            hooks.reverse()

        for hook in hooks:
            db_backup_table_name = DbQueryKeys.BACKUP_TABLE_PREFIX.value + hook.get(
                AppendixKeys.NAME.value
            )
            if (
                hook.get(AppendixKeys.TYPE.value) == DbQueryKeys.TYPE_PARTIAL.value
                and mode == ModeKeysV2.INSPECT
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
                and mode == ModeKeysV2.BACKUP_CREATE_TABLE
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
                    AppendixKeys.CHAPTERS.value,
                    self.current_chapter,
                    indexer[0],
                )

                if len(indexer) != 0:
                    try:
                        self.pg.run_query_template(
                            backup_path,
                            db,
                            BACKUP_TABLE_NAME=db_backup_table_name,
                            ID_LIST=id_list,
                            CURRENT_DIR=hook.get(AppendixKeys.NAME.value),
                        )
                        if self.s3_bucket:
                            tb.table_name_original = hook.get(AppendixKeys.NAME.value)
                            self.status.update(
                                "[bold magenta1]Status = Uploading to S3[/bold magenta1]"
                            )
                            print("target_backup_path", target_backup_path)
                            tb.backup_table_s3(
                                db_backup_table_name,
                                os.path.join(
                                    self.dir_name,
                                    AppendixKeys.CHAPTERS.value,
                                    # AppendixKeys.HOOK.value,
                                    self.current_chapter,
                                    # hook.get(AppendixKeys.NAME.value),
                                    indexer[0],
                                    hook.get(AppendixKeys.NAME.value) + ".dump",
                                ),
                                db,
                                self.s3_bucket,
                            )
                        else:
                            print(f"Backup partial: {hook.get(AppendixKeys.NAME.value)}")
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
                            self.pg.run_query_template(
                                delete_path,
                                db,
                                ID_LIST=id_list,
                                CURRENT_DIR=hook.get(AppendixKeys.NAME.value),
                            )
                        self.pg.drop_table(db_backup_table_name, db)

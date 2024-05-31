import json
import os
from enum import Enum

import yaml
from internal.reader import Reader


class Mode(Enum):
    INSPECT = 1


class ReaderManager(Reader):
    def __init__(self):
        super().__init__()

    def load_appendix(self, appendix_path):
        with open(appendix_path, "r") as file:
            return yaml.safe_load(file)

    def read_directory(self, path):
        directory_structure = {}

        for root, dirs, files in os.walk(path):
            path_parts = root.split(os.sep)
            current_level = directory_structure

            for part in path_parts:
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]

            for dir_name in dirs:
                if dir_name not in current_level:
                    current_level[dir_name] = {}

            for file_name in files:
                current_level[file_name] = None

        return directory_structure

    def output_as_json(self, structure):
        return json.dumps(structure, indent=2)

    def output_as_yaml(self, structure):
        return yaml.dump(structure, default_flow_style=False)

    # for chapter in chapters:
    #     sub_structure = structure[chapter]
    #     base_path = os.path.join("", chapter)
    #     appendix = {"chapters": {chapter: appendix["chapters"][chapter]}}
    #     self.process_structure_v1(sub_structure, mode, base_path, sql_results, depth, indexer)

    def process_structure_v1(
        self,
        structure,
        mode=Mode.INSPECT,
        base_path="",
        sql_results=None,
        depth=0,
        indexer=None,
        sum=None,
    ):
        if sql_results is None:
            sql_results = []
        if indexer is None:
            indexer = []
        if sum is None:
            sum = [0]
        # print(f"D: {depth} - {base_path}")

        dirs = {k: v for k, v in structure.items() if isinstance(v, dict)}
        files = {k: v for k, v in structure.items() if not isinstance(v, dict)}

        for file_name, _ in files.items():
            current_path = os.path.join(base_path, file_name)
            current_dir = os.path.basename(base_path)
            upper_dir = os.path.basename(os.path.dirname(base_path))

            if file_name.endswith(".sql") and not file_name.startswith("_"):
                if file_name == "index.sql":
                    indexer = self.pg.get_data_list(
                        current_path, self.appendix["chapters"][current_dir]["db"]
                    )
                curr_order = self.appendix["chapters"].get(upper_dir, [])
                if curr_order != []:
                    table_list = [
                        item["name"]
                        for item in self.appendix["chapters"][upper_dir]["queries"]
                        if item["type"] == "table"
                    ]
                    curr_order_list = [item["name"] for item in curr_order["queries"]]
                    if current_dir in curr_order_list:
                        db = self.appendix["chapters"][upper_dir]["db"]
                        if mode == Mode.INSPECT and file_name == "size.sql":
                            value = self.pg.get_data_single(current_path, db)
                            if value:
                                sum[0] += value
                                value = self.utils.get_pretty_size(value)
                            else:
                                value = self.utils.get_pretty_size(0)
                            sql_results.append(
                                (
                                    current_dir,
                                    value,
                                    upper_dir,
                                )
                            )
                        elif mode == Mode.INSPECT and current_dir in table_list:
                            for index in indexer:
                                list_table = self.pg.get_data_single_by_id(
                                    current_path, index, db
                                )
                                if list_table and list_table != []:
                                    for table in list_table:
                                        value = self.pg.get_table_size(table)
                                        if value:
                                            sum[0] += value
                                        sql_results.append(
                                            (
                                                table,
                                                self.utils.get_pretty_size(value),
                                                upper_dir,
                                            )
                                        )

        for dir_name, sub_structure in dirs.items():
            if dir_name.startswith("_"):
                continue
            current_path = os.path.join(base_path, dir_name)

            dir_key = os.path.basename(current_path)
            if dir_key in self.appendix["chapters"]:
                order = self.appendix["chapters"].get(dir_key, [])["queries"]

                ordered_dirs = {}
                for item in order:
                    if item["name"] in sub_structure:
                        ordered_dirs[item["name"]] = sub_structure[item["name"]]

                order_list = [item["name"] for item in order]
                unordered_dirs = {}
                for k in sub_structure:
                    if k not in order_list:
                        unordered_dirs[k] = sub_structure[k]
                combined_dirs = {**ordered_dirs, **unordered_dirs}
                self.process_structure_v1(
                    combined_dirs,
                    mode,
                    current_path,
                    sql_results,
                    depth + 1,
                    indexer,
                    sum,
                )
            else:
                self.process_structure_v1(
                    sub_structure,
                    mode,
                    current_path,
                    sql_results,
                    depth + 1,
                    indexer,
                    sum,
                )

        if mode == Mode.INSPECT and base_path == "":
            self.fmt.print_table(sql_results, ["Table Name", "Size", "Chapter"])
            self.fmt.print(
                f"Total Size: [bold green]{self.utils.get_pretty_size(sum[0])}[/bold green]"
            )
            self.fmt.print(f"Total Tables: [bold green]{len(sql_results)}[/bold green]")

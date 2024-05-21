import json
import os

import yaml

from internal.reader import Reader


class ReaderManager(Reader):
    def __init__(self):
        super().__init__()
        self.appendix = self.load_appendix("./books/demo/appendix.yaml")

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

    def process_structure(self, structure, base_path=""):
        dirs = {k: v for k, v in structure.items() if isinstance(v, dict)}
        files = {k: v for k, v in structure.items() if not isinstance(v, dict)}

        for file_name, _ in files.items():
            current_path = os.path.join(base_path, file_name)
            current_dir = os.path.basename(base_path)
            upper_dir = os.path.basename(os.path.dirname(base_path))

            if file_name.endswith(".sql"):
                curr_order = self.appendix[upper_dir].get("order", [])
                if current_dir in curr_order:
                    print(f"Executing SQL file: {current_path}")
            elif file_name.endswith(".yaml"):
                print(f"Reading YAML file: {current_path}")

        for dir_name, sub_structure in dirs.items():
            current_path = os.path.join(base_path, dir_name)

            dir_key = os.path.basename(current_path)
            if dir_key in self.appendix:
                order = self.appendix[dir_key].get("order", [])
                # db = self.appendix[dir_key].get("db")
                # print(f"DB: {db}")
                ordered_dirs = {
                    k: sub_structure[k] for k in order if k in sub_structure
                }
                unordered_dirs = {
                    k: sub_structure[k] for k in sub_structure if k not in order
                }
                combined_dirs = {**ordered_dirs, **unordered_dirs}
                self.process_structure(combined_dirs, current_path)
            else:
                self.process_structure(sub_structure, current_path)

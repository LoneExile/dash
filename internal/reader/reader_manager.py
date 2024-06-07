import json
import os

import yaml
from internal.reader import Reader


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

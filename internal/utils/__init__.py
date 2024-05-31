import datetime
import importlib
import os


class Utils:
    def __init__(self):
        pass

    @staticmethod
    def get_current_time():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def dynamic_import(module_name, class_name):
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    @staticmethod
    def find_file(file_name, search_path):
        result = []
        for root, _, files in os.walk(search_path):
            for file in files:
                if file == file_name:
                    result.append(os.path.join(root, file))
        return result

    @staticmethod
    def get_pretty_size(total_bytes):
        suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
        i = 0
        while total_bytes >= 1024 and i < len(suffixes) - 1:
            total_bytes /= 1024.0
            i += 1
        f = ("%.2f" % total_bytes).rstrip("0").rstrip(".")
        return "%s %s" % (f, suffixes[i])

    @staticmethod
    def convert_bytes_to_gb(size):
        return round(size / 1024 / 1024 / 1024, 2)

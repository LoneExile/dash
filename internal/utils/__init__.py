import importlib
import datetime


class Utils:
    def __init__(self):
        pass

    @staticmethod
    def get_current_time():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def convert_bytes_to_gb(size):
        return round(size / 1024 / 1024 / 1024, 2)

    @staticmethod
    def dynamic_import(module_name, class_name):
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

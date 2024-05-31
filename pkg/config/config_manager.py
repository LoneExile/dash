from pkg.config.parser import config_from_json, config_from_yaml


class ConfigManager:
    def __init__(self):
        self.config = None
        pass

    def load_config(self, config_path=None):
        if config_path.endswith(".json"):
            self.config = config_from_json(config_path, read_from_file=True)
        elif config_path.endswith(".yaml"):
            self.config = config_from_yaml(config_path, read_from_file=True)
        else:
            raise ValueError("Unsupported file format")

        return self.config

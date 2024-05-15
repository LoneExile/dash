import os
from pkg.config.parser import config_from_json, config_from_yaml

json_config_path = "localConfig.json"
yaml_config_path = "localConfig.yaml"

if os.path.exists(json_config_path):
    cfg = config_from_json(json_config_path, read_from_file=True)
else:
    cfg = config_from_yaml(yaml_config_path, read_from_file=True)

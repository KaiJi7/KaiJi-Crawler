from functools import lru_cache
import yaml

DEFAULT_CONFIG_PATH = "configs/config.yaml"


@lru_cache(maxsize=1)
def get_config(config_path=DEFAULT_CONFIG_PATH) -> dict:
    with open(config_path) as config:
        return yaml.load(config, Loader=yaml.FullLoader)

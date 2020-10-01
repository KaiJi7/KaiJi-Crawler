import logging
import os

import yaml

from util.singleton import Singleton


class Util(metaclass=Singleton):
    def __init__(self):
        with open("configs/config.yaml") as config:
            self.config = yaml.load(config, Loader=yaml.FullLoader)

    def load_environment_variable(self):
        logging.info("start load environment variables and overwrite configs file")
        with open("configs/config.yaml") as config:
            config = yaml.load(config, Loader=yaml.FullLoader)

            config["mongo"]["host"] = (
                os.environ.get("DB_HOST") or config["mongoDb"]["host"]
            )
            config["mongo"]["port"] = (
                os.environ.get("DB_PORT") or config["mongoDb"]["host"]
            )
            config["mongo"]["user"] = (
                os.environ.get("DB_USER") or config["mongoDb"]["user"]
            )
            config["mongo"]["password"] = (
                os.environ.get("DB_PASSWORD") or config["mongoDb"]["password"]
            )

        # overwrite configs by environment variable
        with open("configs/configs.yml", "w") as new_config:
            yaml.dump(config, new_config)

        self.config = config
        logging.debug("finish update configs file")
        return

    @classmethod
    def get_config(cls):
        return Util().config

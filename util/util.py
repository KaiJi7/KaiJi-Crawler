import os

import pymysql
import yaml
from pymongo import MongoClient
from sqlalchemy import create_engine

from config.logger import get_logger
from util.singleton import Singleton


class Util(metaclass=Singleton):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        with open("config/configuration.yml") as config:
            self.config = yaml.load(config, Loader=yaml.FullLoader)

    def load_environment_variable(self):
        self.logger.info("start load environment variables and overwrite config file")
        with open("config/configuration.yml") as config:
            config = yaml.load(config, Loader=yaml.FullLoader)

            config["DB"]["host"] = (
                os.environ.get("DB_HOST")
                if os.environ.get("DB_HOST")
                else config["DB"]["host"]
            )

            config["DB"]["port"] = (
                int(os.environ.get("DB_PORT"))
                if os.environ.get("DB_PORT")
                else config["DB"]["port"]
            )

            config["DB"]["user"] = (
                os.environ.get("DB_USERNAME")
                if os.environ.get("DB_USERNAME")
                else config["DB"]["user"]
            )

            config["DB"]["password"] = (
                os.environ.get("DB_PASSWORD")
                if os.environ.get("DB_PASSWORD")
                else config["DB"]["password"]
            )

        # overwrite config by environment variable
        with open("config/configuration.yml", "w") as new_config:
            yaml.dump(config, new_config)

        self.config = config
        self.logger.debug("finish update config file")
        return

    @classmethod
    def get_config(cls):
        return Util().config

    @classmethod
    def get_db_connection(cls):
        user = Util().config["DB"]["user"]
        password = Util().config["DB"]["password"]
        host = Util().config["DB"]["host"]
        port = Util().config["DB"]["port"]
        return pymysql.connect(
            host=host,
            user=user,
            passwd=password,
            port=port,
            db=Util().config["DB"]["schema"],
            charset="utf8",
        )

    @classmethod
    def get_db_engine(cls):
        user = Util().config["DB"]["user"]
        password = Util().config["DB"]["password"]
        host = Util().config["DB"]["host"]
        port = Util().config["DB"]["port"]
        return create_engine(
            "mysql+pymysql://{}:{}@{}:{}".format(user, password, host, port)
        )

    @classmethod
    def get_mongo_client(cls):
        return MongoClient(
            host=Util().config["mongoDb"]["host"],
            port=Util().config["mongoDb"]["port"],
            username=Util().config["mongoDb"]["username"],
            password=Util().config["mongoDb"]["password"],
            authMechanism="SCRAM-SHA-1",
        )

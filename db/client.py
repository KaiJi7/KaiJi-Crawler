from util.singleton import Singleton
from mongoengine import connect
from util.util import Util
import logging


class Client(metaclass=Singleton):
    def __init__(self):
        config = Util.get_config()
        connect(
            db=config["mongoDb"]["db"],
            host=config["mongoDb"]["host"],
            port=config["mongoDb"]["port"],
            username=config["mongoDb"]["username"],
            password=config["mongoDb"]["password"],
            authentication_source="admin",
        )

        logging.debug('db initialized')

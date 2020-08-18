import logging
import sys

from mongoengine import connect
from mongoengine.errors import DoesNotExist

from db.collection.sports import SportsData
from util.singleton import Singleton
from util.util import Util


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

        logging.debug("db initialized")

    @classmethod
    def latest_record(cls, game_type):
        logging.debug(f"getting latest record: {game_type}")
        try:
            res = SportsData.objects(game_type=game_type).order_by("-game_time")
            return res[0] if res else None
        except DoesNotExist as e:
            logging.warning(e)
            raise e
        except Exception as e:
            logging.error(f"unknown error: {e}")
            sys.exit(-1)

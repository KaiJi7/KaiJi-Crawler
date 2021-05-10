import logging
import sys

from mongoengine import connect
import pymongo
from pymongo import results
from mongoengine.errors import DoesNotExist

from src.db.collection.sports_data import SportsData
from src.db.collection.game import Game
from src.util.singleton import Singleton
from src.util.util import Util
from src.db.collection.gambling import Gambling
from src.db.collection.betting import Betting


class Client(metaclass=Singleton):
    def __init__(self):
        config = Util.get_config()
        connect(
            db=config["mongo"]["db"],
            host=config["mongo"]["host"],
            port=config["mongo"]["port"],
            username=config["mongo"]["username"],
            password=config["mongo"]["password"],
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


class NewClient(metaclass=Singleton):
    def __init__(self):
        config = Util.get_config()
        self._client = pymongo.MongoClient(
            host=config["mongo"]["host"],
            port=config["mongo"]["port"],
            username=config["mongo"]["username"],
            password=config["mongo"]["password"],
        )

        self._db = self._client[config["mongo"]["db"]]
        self._col_game = self._db["game"]
        self._col_gambling = self._db["gambling"]
        self._col_betting = self._db["betting"]

    def insert_game(self, game: Game) -> results:
        return self._col_game.insert_one(game.get_data())

    def insert_gambling(self, gambling: Gambling) -> results:
        return self._col_gambling.insert_one(gambling.get_data())

    def insert_betting(self, betting: Betting) -> results:
        return self._col_betting.insert_one(betting.get_data())


if __name__ == '__main__':
    p = NewClient()
    p.insert_game(Game())

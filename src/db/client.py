import pymongo
from bson.objectid import ObjectId
from pymongo import results

from src.db.collection.betting import Betting
from src.db.collection.gambling import Gambling
from src.db.collection.game import Game
from src.util.singleton import Singleton
from src.util.util import Util


class Client(metaclass=Singleton):
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

    def latest_record(self, game_type: str) -> Game:
        f = {
            "game_type": game_type
        }

        res = list(self._col_game.find(f).sort("start_time", -1).limit(1))
        if res:
            return Game().from_dict(res[0])
        else:
            return None

    def insert_game(self, game: Game) -> results:
        return self._col_game.insert_one(game.get_data())

    def insert_gambling(self, gambling: Gambling) -> results:
        return self._col_gambling.insert_one(gambling.get_data())

    def insert_betting(self, betting: Betting) -> results:
        return self._col_betting.insert_one(betting.get_data())

    def get_gambling(self, gambling_id: ObjectId) -> Gambling:
        data = self._col_gambling.find_one(gambling_id)
        return Gambling().from_dict(data)

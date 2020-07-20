from config.logger import get_logger
from util.singleton import Singleton
from util.util import Util


class MongoUtil(metaclass=Singleton):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util.get_config()
        self.client = Util.get_mongo_client()

    @classmethod
    def get_latest_record(cls, game_type):
        record = MongoUtil().client["gambling_simulation"]["sports_data"].find_one(
            {"game_type": game_type}, sort=[("game_time", -1)]
        )
        return MongoUtil().client["gambling_simulation"]["sports_data"].find_one(
            {"game_type": game_type}, sort=[("game_time", -1)]
        )

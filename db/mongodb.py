from config.logger import get_logger
from util.util import Util


def init_mongo():
    logger = get_logger("mongo")
    logger.debug("start init mongo")
    mongo_client = Util.get_mongo_client()["gambling_simulation"]

    if "battle_data" not in mongo_client.list_collection_names():
        logger.debug("collection battle_data not exist, create it")
        collection = mongo_client["battle_data"]
        collection.create_index(
            [
                ("gambler_id", 1),
                ("strategy", 1),
                ("decision.game_type", 1),
                ("decision.game_date", 1),
                ("decision.gamble_id", 1),
            ],
            unique=True,
        )

    if "sports_data" not in mongo_client.list_collection_names():
        logger.debug("collection sports_data not exist, create it")
        collection = mongo_client["sports_data"]
        collection.create_index(
            [("game_time", 1), ("gamble_id", 1), ("game_type", 1)], unique=True,
        )

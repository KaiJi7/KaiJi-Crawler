import copy
import logging
from datetime import datetime

from bson.objectid import ObjectId

default_datetime = datetime(2000, 1, 1)

DATA_SOURCE_WILD_MEMBER = "wild_member"

BET_GUEST = "guest"
BET_HOST = "host"
BET_OVER = "over"
BET_UNDER = "under"

template = {
    "gambling_id": ObjectId(),  # mongodb object id
    "source": "",
    "bet": [
        # {
        #     "side": None,
        #     "quantity": -1,
        # }
    ],
}


class Betting:

    def __init__(self, gambling_id: ObjectId):
        self._data = copy.deepcopy(template)
        self._data["gambling_id"] = gambling_id

    def set_bet(self, side: str, quantity: int) -> bool:
        for bet in self._data["bet"]:
            if bet["side"] == side:
                bet["quantity"] = quantity
                return True

        logging.info(f"side {side} not exist in bet, set new bet side")

        self._data["bet"].append({
            "side": side,
            "quantity": quantity
        })
        return True

    def set_source(self, source: str) -> bool:
        self._data["source"] = source
        return True

    def get_data(self) -> dict:
        return self._data

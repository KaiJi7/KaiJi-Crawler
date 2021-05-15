import copy
import logging
from datetime import datetime

default_datetime = datetime(2000, 1, 1)

GAMBLING_TYPE_UNKNOWN = "unknown"
GAMBLING_TYPE_ORIGINAL = "original"
GAMBLING_TYPE_SPREAD_POINT = "spread_point"
GAMBLING_TYPE_TOTAL_SCORE = "total_score"

template = {
    "type": GAMBLING_TYPE_UNKNOWN,
    "game_id": None,  # mongodb object id
    "odds": [
        # {
        #     "bet": None,
        #     "odds": 0.0,
        # }
    ],
    "properties": [
        # {
        #     "name": None,
        #     "value": None,
        # }
    ]
}


class Gambling:

    def __init__(self, game_id=None):
        self._data = copy.deepcopy(template)
        self._data["game_id"] = game_id

    def set_type(self, gambling_type: str) -> bool:
        # TODO: check type availability first
        self._data["type"] = gambling_type
        return True

    def set_odds(self, bet: str, odds: float) -> bool:
        self._data["odds"].append(
            {
                "bet": bet,
                "odds": odds
            }
        )
        return True

    def set_property(self, name: str, value) -> bool:
        # if not any(d['main_color'] == 'red' for d in a):

        if any(p["name"] == name for p in self._data["properties"]):
            logging.warning(f"property {name} already exist")
            return False
        self._data["properties"].append(
            {
                "name": name,
                "value": value,
            }
        )
        return True

    def get_data(self) -> dict:
        return self._data

    def from_dict(self, data: dict):
        pass
        # self.__dict__["_data"].update(data)

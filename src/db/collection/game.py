from datetime import datetime

default_datetime = datetime(2000, 1, 1)
template = {
    # "game_id": None,
    "guest": {
        "name": "",
        "score": -1,
    },
    "host": {
        "name": "",
        "score": -1,
    },
    "start_time": default_datetime,
    "start_time_local": default_datetime,
    "location": ""
}


class Game:
    _data = template

    def set_guest_name(self, name: str) -> bool:
        self._data["guest"]["name"] = name
        return True

    def set_guest_score(self, score: int) -> bool:
        self._data["guest"]["score"] = score
        return True

    def set_host_name(self, name: str) -> bool:
        self._data["host"]["name"] = name
        return True

    def set_host_score(self, score: int) -> bool:
        self._data["host"]["score"] = score
        return True

    def set_start_time(self, time: datetime) -> bool:
        self._data["start_time"] = time
        return True

    def set_start_time_local(self, time: datetime) -> bool:
        self._data["start_time_local"] = time
        return True

    def set_location(self, location: str) -> bool:
        self._data["location"] = location
        return True

    def get_data(self) -> dict:
        return self._data

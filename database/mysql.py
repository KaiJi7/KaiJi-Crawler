from configs.logger import get_logger
from util.singleton import Singleton
from util.util import Util


class MySqlUtil(metaclass=Singleton):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util.get_config()
        self.client = Util.get_db_connection()

    @classmethod
    def get_latest_record(cls, game_season):
        game_latest_record = {}
        cursor = MySqlUtil().client.cursor()
        for game_type in game_season.keys():
            cursor.execute(
                f"SELECT game_date FROM game_data where game_type = '{game_type}' ORDER BY game_date DESC, gamble_id DESC LIMIT 1"
            )
            latest_record = cursor.fetchone()
            if latest_record:
                game_latest_record[game_type] = latest_record[0]
            else:
                game_latest_record[game_type] = 0
        return game_latest_record

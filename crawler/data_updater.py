import yaml

from config.logger import get_logger
from crawler.common import get_crawl_range, write_to_db
from crawler.crawler import Crawler
from util.util import Util


class DataUpdater:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util.get_config()
        self.db = Util.get_db_connection()
        self.engine = Util.get_db_engine()

        with open("config/game_season.yml") as config:
            self.game_season = yaml.load(config, Loader=yaml.FullLoader)

    def update_db(self, db_type):
        self.logger.info(f"start update db: {db_type}")
        self.__init__()
        for game_type, crawl_ranges in get_crawl_range(
            self.get_latest_record()
        ).items():
            self.logger.debug("start update game type: {}".format(game_type))
            for crawl_range in crawl_ranges:
                self.sql_crawl_and_update(game_type, crawl_range)

    def sql_crawl_and_update(self, game_type, crawl_range):
        self.logger.debug(
            f"start crawler with date range: {crawl_range.begin}-{crawl_range.end}"
        )
        for daily_data in Crawler(game_type=game_type).start_crawler(
            start_date=str(crawl_range.begin), end_date=str(crawl_range.end)
        ):
            write_to_db(self.engine, daily_data["game_data"], "game_data")
            for prediction_group, prediction_data in daily_data[
                "prediction_data"
            ].items():
                write_to_db(
                    self.engine, prediction_data, f"prediction_data_{prediction_group}"
                )

    def mongo_crawl_and_update(self, game_type, crawl_range):
        self.logger.debug(
            f"start crawler with date range: {crawl_range.begin}-{crawl_range.end}"
        )
        for daily_data in Crawler(game_type=game_type).start_crawler(
            start_date=str(crawl_range.begin), end_date=str(crawl_range.end)
        ):
            write_to_db(self.engine, daily_data["game_data"], "game_data")
            for prediction_group, prediction_data in daily_data[
                "prediction_data"
            ].items():
                write_to_db(
                    self.engine, prediction_data, f"prediction_data_{prediction_group}"
                )

    def get_latest_record(self):
        self.logger.info("start get latest record")
        game_latest_record = {}
        cursor = self.db.cursor()
        for game_type in self.game_season.keys():
            self.logger.debug(
                "getting latest record of game type: {}".format(game_type)
            )
            cursor.execute(
                f"SELECT game_date FROM game_data where game_type = '{game_type}' ORDER BY game_date DESC, gamble_id DESC LIMIT 1"
            )
            latest_record = cursor.fetchone()
            if latest_record:
                game_latest_record[game_type] = latest_record[0]
            else:
                self.logger.warn("no latest record of game type: {}".format(game_type))
                game_latest_record[game_type] = 0
        return game_latest_record

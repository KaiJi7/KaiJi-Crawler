import datetime
import functools

import yaml
from flatten_dict import flatten

from config.constant import database as db_constant
from config.logger import get_logger
from crawler.crawler import Crawler
from util.util import Util


class DataUpdater(object):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util().get_config()
        self.db = Util().get_db_connection()

        with open("config/game_season.yml") as config:
            self.game_season = yaml.load(config, Loader=yaml.FullLoader)

    def update_db(self):
        self.logger.info("start update db")
        self.__init__()
        for game_type, crawl_ranges in self.get_crawl_range().items():
            self.logger.debug("start update game type: {}".format(game_type))
            for crawl_range in crawl_ranges:
                self.logger.debug(
                    "start crawler with date range: {}-{}".format(
                        crawl_range.begin, crawl_range.end
                    )
                )
                Crawler(game_type=game_type).start_crawler(
                    start_date=str(crawl_range.begin), end_date=str(crawl_range.end)
                )

    @functools.lru_cache(maxsize=1)
    def get_latest_record(self):
        self.logger.info("start get latest record")
        game_latest_record = {}
        cursor = self.db.cursor()
        for game_type in self.game_season.keys():
            self.logger.debug(
                "getting latest record of game type: {}".format(game_type)
            )
            cursor.execute(
                "SELECT {} FROM {} where {} = '{}' ORDER BY {} DESC LIMIT 1".format(
                    db_constant.game_date,
                    db_constant.game_data,
                    db_constant.game_type,
                    game_type,
                    db_constant.row_id,
                )
            )
            latest_record = cursor.fetchone()
            if latest_record:
                game_latest_record[game_type] = latest_record[0]
            else:
                self.logger.warn("no latest record of game type: {}".format(game_type))
                game_latest_record[game_type] = 0
        return game_latest_record

    def get_crawl_range(self):
        self.logger.info("start get crawl range")
        crawl_range = {}
        for game_type, season_info in self.game_season.items():
            self.logger.debug(
                "start date crawl range of game type: {}".format(game_type)
            )
            # get latest game date of the game_type
            latest_game = min(
                max(flatten(season_info).values()),
                int(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")),
            )
            if (
                game_type in self.get_latest_record()
                and self.get_latest_record()[game_type] < latest_game
            ):
                self.logger.debug(
                    "game data was out of date, the latest record in db is: {}, "
                    "the latest game is: {}".format(
                        self.get_latest_record()[game_type], latest_game
                    )
                )
                crawl_range[game_type] = self.date_overlap(
                    self.get_latest_record()[game_type], latest_game, game_type
                )

        return crawl_range

    # get date overlap between date_range(latest_record, latest_game) and game date
    def date_overlap(self, latest_record, latest_game, game_type):
        self.logger.info("start get date overlap of game type: {}".format(game_type))
        overlap = []

        for season in self.game_season[game_type].keys():
            if latest_record < self.game_season[game_type][season]["start"]:
                self.logger.debug(
                    "start date of the season: {} after latest record: {}".format(
                        season, latest_record
                    )
                )
                overlap.append(
                    CrawlRange(
                        min(self.game_season[game_type][season]["start"], latest_game),
                        min(self.game_season[game_type][season]["end"], latest_game),
                    )
                )
            elif latest_record < self.game_season[game_type][season]["end"]:
                self.logger.debug(
                    "end date of the season: {} after latest record: {}".format(
                        season, latest_record
                    )
                )
                overlap.append(
                    CrawlRange(
                        latest_record,
                        min(self.game_season[game_type][season]["end"], latest_game),
                    )
                )
            else:
                self.logger.debug(
                    "the latest record after the season, no overlap of this season"
                )
        return overlap


class CrawlRange:
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end

import datetime
import functools
import threading

import yaml
from dateutil.relativedelta import relativedelta
from flatten_dict import flatten

from configs.constant import database as db_constant
from configs.logger import get_logger
from crawler.crawler import Crawler
from util.util import Util


class MongoDbDataUpdater:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util.get_config()
        self.db = Util.get_db_connection()
        self.engine = Util.get_db_engine()

        with open("configs/game_season.yml") as config:
            self.game_season = yaml.load(config, Loader=yaml.FullLoader)

    def update_db(self):
        self.logger.info("start update db")
        self.__init__()
        for game_type, crawl_ranges in self.get_crawl_range().items():
            self.logger.debug("start update game type: {}".format(game_type))
            for crawl_range in crawl_ranges:
                # have to consider the limit of rps
                threads = []
                t = threading.Thread(
                    target=self.do_crawl_and_update, args=(game_type, crawl_range,)
                )
                threads.append(t)
                t.start()

                # self.logger.debug(
                #     f"start crawler with date range: {crawl_range.begin}-{crawl_range.end}"
                # )
                # for daily_data in Crawler(game_type=game_type).start_crawler(
                #     start_date=str(crawl_range.begin), end_date=str(crawl_range.end)
                # ):
                #     self.write_to_db(daily_data["game_data"], "game_data")
                #     for prediction_group, prediction_data in daily_data[
                #         "prediction_data"
                #     ].items():
                #         self.write_to_db(
                #             prediction_data, f"prediction_data_{prediction_group}"
                #         )

    def do_crawl_and_update(self, game_type, crawl_range):
        self.logger.debug(
            f"start crawler with date range: {crawl_range.begin}-{crawl_range.end}"
        )
        for daily_data in Crawler(game_type=game_type).start_crawler(
            start_date=str(crawl_range.begin), end_date=str(crawl_range.end)
        ):
            self.write_to_db(daily_data["game_data"], "game_data")
            for prediction_group, prediction_data in daily_data[
                "prediction_data"
            ].items():
                self.write_to_db(prediction_data, f"prediction_data_{prediction_group}")

    def write_to_db(self, df, table_name):
        self.logger.info("start write game data to db: {}".format(table_name))
        df.to_sql(
            con=self.engine,
            name=table_name,
            index=False,
            if_exists="append",
            schema=self.config["DB"]["schema"],
        )
        self.logger.info("finished write game data to db")
        return

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

        # the available data since 1 year from now only
        valid_date_since = int(
            datetime.datetime.strftime(
                datetime.datetime.now() - relativedelta(years=1), "%Y%m%d"
            )
        )

        for season in self.game_season[game_type].keys():
            latest_record += 1
            if 1 < latest_record < self.game_season[game_type][season]["start"]:
                self.logger.debug(
                    f"start date of the season: {season} after latest record: {latest_record}"
                )
                begin = min(
                    max(
                        self.game_season[game_type][season]["start"], valid_date_since,
                    ),
                    latest_game,
                )
                end = min(
                    max(self.game_season[game_type][season]["end"], valid_date_since,),
                    latest_game,
                )
                if begin < end:
                    overlap.append(CrawlRange(begin, end))
            elif (
                1 < latest_record < self.game_season[game_type][season]["end"]
                and latest_record < latest_game
            ):
                self.logger.debug(
                    f"end date of the season: {season} after latest record: {latest_record}"
                )
                end = min(
                    max(self.game_season[game_type][season]["end"], valid_date_since,),
                    latest_game,
                )
                if latest_record < end:
                    overlap.append(CrawlRange(latest_record, end,))
            else:
                self.logger.debug(
                    "the latest record after the season, no overlap of this season"
                )
        return overlap


class CrawlRange:
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end

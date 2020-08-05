import datetime

import pandas as pd
import pymysql
import yaml
from dateutil.relativedelta import relativedelta
from flatten_dict import flatten

from configs.logger import get_logger
from util.util import Util

logger = get_logger("common")


class CrawlRange:
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end


def get_crawl_range(latest_records):
    crawl_range = {}

    with open("configs/game_season.yml") as config:
        game_season = yaml.load(config, Loader=yaml.FullLoader)

    for game_type, season_info in game_season.items():
        logger.debug(f"start date crawl range of game type: {game_type}")
        # get latest game date of the game_type
        latest_game = min(
            max(flatten(season_info).values()),
            int(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")),
        )
        if game_type in latest_records and latest_records[game_type] < latest_game:
            logger.debug(
                f"game data was out of date, the latest record in db is: {latest_records[game_type]}, "
                "the latest game is: {latest_game}"
            )

            crawl_range[game_type] = date_overlap(
                latest_records[game_type], latest_game, game_type, game_season
            )

    return crawl_range


# get date overlap between date_range(latest_record, latest_game) and game date
def date_overlap(latest_record, latest_game, game_type, game_season):
    logger.info("start get date overlap of game type: {}".format(game_type))
    overlap = []

    # the available data since 1 year from now only
    valid_date_since = int(
        datetime.datetime.strftime(
            datetime.datetime.now() - relativedelta(years=1), "%Y%m%d"
        )
    )
    latest_record += 1
    for season in game_season[game_type].keys():
        if 1 < latest_record < game_season[game_type][season]["start"]:
            logger.debug(
                f"start date of the season: {season} after latest record: {latest_record}"
            )
            begin = min(
                max(game_season[game_type][season]["start"], valid_date_since,),
                latest_game,
            )
            end = min(
                max(game_season[game_type][season]["end"], valid_date_since,),
                latest_game,
            )
            if begin < end:
                overlap.append(CrawlRange(begin, end))
        elif (
            1 < latest_record < game_season[game_type][season]["end"]
            and latest_record < latest_game
        ):
            logger.debug(
                f"end date of the season: {season} after latest record: {latest_record}"
            )
            end = min(
                max(game_season[game_type][season]["end"], valid_date_since,),
                latest_game,
            )
            if latest_record < end:
                overlap.append(CrawlRange(latest_record, end,))
        else:
            logger.debug(
                "the latest record after the season, no overlap of this season"
            )
    return overlap


def write_to_db(engine, data, table_name):
    logger.info("start write game data to db: {}".format(table_name))
    try:
        pd.DataFrame.from_dict(data).to_sql(
            con=engine,
            name=table_name,
            index=False,
            if_exists="append",
            schema=Util.get_config()["DB"]["schema"],
        )
    except pymysql.err.Error as e:
        logger.error(f"fail to write db: {e}")
        raise e

    logger.info("finished write game data to db")
    return

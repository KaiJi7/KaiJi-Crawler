import logging
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from crawler.crawler import Crawler
from db.client import Client
from util.util import Util


class Commander:
    def __init__(self):
        self.config = Util.get_config()

    def start(self):
        for game_type, crawl_range in self.crawl_range().items():
            crawler = Crawler(game_type)
            for date in pd.date_range(
                start=crawl_range["begin"], end=crawl_range["end"]
            ):
                logging.debug(f"command: crawling {game_type} at {date}")
                crawler.run(self.format_date(date))

                # random sleep
                time.sleep(
                    abs(np.random.normal(self.config["commander"]["queryPeriod"]))
                )

    def format_date(self, date: datetime) -> str:
        return datetime.strftime(date, self.config["dateFormat"])

    def crawl_range(self):
        # TODO: skip empty range
        crawl_range = {}
        end_day = datetime.today()
        for game_type in self.config["commander"]["gameTypes"]:
            latest_record = Client.latest_record(game_type)
            if latest_record:
                begin_day = latest_record.game_time + timedelta(days=1)
            else:
                available_interval = self.config["commander"]["availableInterval"]
                begin_day = datetime.today() - timedelta(days=available_interval)
                logging.info(
                    f"no latest record exist for {game_type}, start from the earliest available"
                )

            crawl_range[game_type] = {"begin": begin_day, "end": end_day}

        logging.debug(f"crawl range: {crawl_range}")
        return crawl_range

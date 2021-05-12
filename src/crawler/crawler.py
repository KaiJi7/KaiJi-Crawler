import logging
import sys

import requests
from bs4 import BeautifulSoup

from mongoengine.errors import NotUniqueError

from src.crawler.common import game_type_map
from src.crawler.document_builder import DocumentBuilder
from src.crawler.row_parser import RowParser
from src.util.util import Util
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.parser.game import parse_game
from src.parser.gambling import parse_gambling
from src.parser.betting import parse_betting
from src.db.client import NewClient


class Crawler:
    def __init__(self, game_type):
        self.game_type = game_type

    def run(self, date):
        logging.debug(f"start crawling: {date}")
        try:
            content = self.get_content(self.get_url(date))
            soup = BeautifulSoup(content.text, "html.parser")
            db = NewClient()
        except Exception as e:
            logging.error(f"fail to get and parse content: {e}")
            return

        # iterate for every 2 rows, the first for the guest and game info, the second for the host
        for guest_row, host_row in zip(
                *[iter(soup.find("tbody").findAll("tr", {"class": "game-set"}))] * 2
        ):
            try:

                game = parse_game(date, guest_row, host_row)
                res = db.insert_game(game)

                gambling = parse_gambling(res.inserted_id, guest_row, host_row)
                for g in gambling:
                    res = db.insert_gambling(g)
                    betting = parse_betting(res.inserted_id, g, guest_row, host_row)
                    db.insert_betting(betting)
            except Exception as e:
                logging.error(f"unknown error: {e}")

        logging.debug(f"crawl job done: {date}")
        return

    def judge_total_point(self, scores, threshold):
        if not threshold:
            return None
        total_point = scores["guest"] + scores["host"]
        return (
            "under"
            if total_point < threshold
            else "over"
            if total_point > threshold
            else None
        )

    def judge_spread_point(self, scores, guest_sp, host_sp):
        try:
            return (
                "guest"
                if scores["host"] + host_sp < scores["guest"]
                else "host"
                if scores["guest"] + guest_sp < scores["host"]
                else None
            )
        except Exception as e:
            logging.error(e)
            return None

    def judge_original(self, scores):
        return (
            "guest"
            if scores["host"] < scores["guest"]
            else "host"
            if scores["guest"] < scores["host"]
            else None
        )

    def total_point_major_pred(self, result, under, over):
        if (result == "under" and over["population"] < under["population"]) or (
                result == "over" and under["population"] < over["population"]
        ):
            return True
        else:
            return False

    def point_major_predict(self, result, guest, host):
        if (result == "guest" and host["population"] < guest["population"]) or (
                result == "host" and guest["population"] < host["population"]
        ):
            return True
        else:
            return False

    def get_url(self, date, group_type=0):
        return Util.get_config()["crawler"]["urlPattern"].format(
            game_type=game_type_map[self.game_type],
            game_date=date,
            group_type=group_type,
        )

    @staticmethod
    def get_content(url: str):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[408, 502, 503, 504])
        s.mount('http://', HTTPAdapter(max_retries=retries))
        return s.get(url, timeout=5)

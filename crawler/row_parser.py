import logging
import re
from datetime import datetime

import pytz

from crawler.common import team_name_mapping
from util.util import Util


class RowParser:
    @classmethod
    def gamble_id(cls, row_content):
        return row_content.find("td", "td-gameinfo").find("h3").text

    @classmethod
    def game_time(cls, date, row_content):
        game_time = row_content.find("td", "td-gameinfo").find("h4").text
        return datetime.strptime(f"{date} {game_time}", "%Y%m%d %p %I:%M").astimezone(
            pytz.timezone(Util.get_config()["timezone"])
        )

    @classmethod
    def team_name(cls, row_content):
        guest = (
            row_content.find("td", {"class": "td-teaminfo"})
            .find_all("tr")[0]
            .find("a")
            .text.strip()
        )
        host = (
            row_content.find("td", {"class": "td-teaminfo"})
            .find_all("tr")[-1]
            .find("a")
            .text.strip()
        )
        guest_abbreviate = team_name_mapping.get(guest, guest)
        host_abbreviate = team_name_mapping.get(host, host)

        return {"guest": guest_abbreviate, "host": host_abbreviate}

    @classmethod
    def scores(cls, row_content):
        guest = int(
            row_content.find("td", {"class": "td-teaminfo"})
            .find_all("li")[0]
            .text.strip()
        )
        host = int(
            row_content.find("td", {"class": "td-teaminfo"})
            .find_all("li")[-1]
            .text.strip()
        )
        return {"guest": guest, "host": host}

    @classmethod
    def total_point_threshold(cls, row_content):
        try:
            threshold = (
                row_content.find("td", {"class": "td-bank-bet02"})
                .find("span", {"class": "data-wrap"})
                .find("strong")
                .text
            )
            return float(threshold)
        except Exception as e:
            logging.error(e)
            return None

    @classmethod
    def total_point_response(cls, row_content):
        try:
            response = (
                row_content.find("td", {"class": "td-bank-bet02"})
                .find("span", {"class": "data-wrap"})
                .find("span")
                .text[2:]
            )
            return float(response)
        except Exception as e:
            logging.error(e)
            return None

    @classmethod
    def total_point_prediction(cls, row_content):
        date = (
            row_content.find("td", {"class": "td-bank-bet02"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        return {
            "percentage": float(percentage),
            "population": int(population),
        }

    @classmethod
    def spread_point(cls, row_content):
        try:
            sp = (
                row_content.find("td", {"class": "td-bank-bet01"})
                .find("span", {"class": "data-wrap"})
                .find("strong")
                .text
            )
            return float(sp)
        except Exception as e:
            logging.error(e)
            return None

    @classmethod
    def spread_point_response(cls, row_content):
        try:
            spr = (
                row_content.find("td", {"class": "td-bank-bet01"})
                .find("span", {"class": "data-wrap"})
                .find("span")
                .text[2:]
            )
            return float(spr)
        except Exception as e:
            logging.error(e)
            return None

    @classmethod
    def spread_point_prediction(cls, row_content):
        date = (
            row_content.find("td", {"class": "td-bank-bet01"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        return {
            "percentage": float(percentage),
            "population": int(population),
        }

    @classmethod
    def origin_response(cls, row_content):
        local_origin_guest_response_ratio = row_content.find(
            "td", {"class": "td-bank-bet03"}
        ).text.strip()

        data = re.findall(r"\d+\.\d+", local_origin_guest_response_ratio)
        if len(data) != 1:
            logging.warning(f"unexpected data length: {data}")
            return None
        return float(data[0])

    @classmethod
    def original_prediction(cls, row_content):
        date = (
            row_content.find("td", {"class": "td-bank-bet03"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        return {
            "percentage": float(percentage),
            "population": int(population),
        }

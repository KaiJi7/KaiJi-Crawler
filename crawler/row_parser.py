from datetime import datetime
from configs.constant.crawler import team_name_mapping
from db.collection.sports import TeamInfo
import re


class RowParser:
    @classmethod
    def gamble_id(cls, row_content):
        return row_content.find("td", "td-gameinfo").find("h3").text

    @classmethod
    def game_time(cls, row_content):
        game_time = row_content.find("td", "td-gameinfo").find("h4").text
        return datetime.strptime(game_time, "%p %H:%M")

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
        # TODO: test to get the float directly
        total_point = row_content.find(
            "td", {"class": "td-bank-bet02"}
        ).text.strip()
        # filter out float
        data = re.findall(r"\d+\.\d+", total_point)
        return data if len(data) == 2 else (0, 0)

    @classmethod
    def total_point_response(cls, row_content):
        # TODO: test to get the float directly
        total_point = row_content.find(
            "td", {"class": "td-bank-bet02"}
        ).text.strip()
        # filter out float
        data = re.findall(r"\d+\.\d+", total_point)
        return data if len(data) == 2 else (0, 0)

    @classmethod
    def spread_point(cls, row_content):
        # TODO: test to get the float directly
        # get local point spread info and response ratio
        local_host_spread_point = row_content.find(
            "td", {"class": "td-bank-bet01"}
        ).text.strip()
        # filter out float
        data = re.findall(r"[+-]?\d+\.\d+", local_host_spread_point)
        return data if len(data) == 2 else (0, 0)

    @classmethod
    def origin_response(cls, row_content):
        local_origin_guest_response_ratio = row_content.find(
            "td", {"class": "td-bank-bet03"}
        ).text.strip()
        return re.findall(r"\d+\.\d+", local_origin_guest_response_ratio)
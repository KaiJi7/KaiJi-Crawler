import datetime
import re
from collections import defaultdict

import pandas as pd
import requests
from bs4 import BeautifulSoup

from configs.constant.crawler import prediction_groups, team_name_mapping
from configs.logger import get_logger
from configs.constant.global_constant import game_type_map
from util.util import Util
from crawler.row_parser import RowParser
from crawler.document_builder import DocumentBuilder
from db.collection.sports import SportsData
from mongoengine.errors import NotUniqueError

import logging


class Crawler:
    def __init__(self, game_type):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util.get_config()
        self.data = None
        self.game_type = game_type
        self.game_info = defaultdict(list)
        self.prediction_info_all_member = defaultdict(list)
        self.prediction_info_more_than_sixty = defaultdict(list)
        self.prediction_info_all_prefer = defaultdict(list)
        self.prediction_info_top_100 = defaultdict(list)

        self.prediction = {
            "all_member": self.prediction_info_all_member,
            "more_than_sixty": self.prediction_info_more_than_sixty,
            "all_prefer": self.prediction_info_all_prefer,
            "top_100": self.prediction_info_top_100,
        }
        self.engine = Util.get_db_engine()

    def start_crawler(self, start_date, end_date):
        total_crawled_game = 0
        start_date = datetime.datetime.strptime(
            start_date, self.config["crawler"]["dateFormat"]
        )
        end_date = datetime.datetime.strptime(
            end_date, self.config["crawler"]["dateFormat"]
        )

        for date in pd.date_range(start=start_date, end=end_date):
            date = datetime.datetime.strftime(
                date, self.config["crawler"]["dateFormat"]
            )

            # for each prediction group
            for prediction_group in prediction_groups.keys():
                res = requests.get(
                    self.get_url(date, prediction_groups[prediction_group])
                )
                soup = BeautifulSoup(res.text, "html.parser")

                # get game info for only once
                if prediction_group == "all_member":
                    self.get_game_data(date, soup)
                    # self.write_to_db(
                    #     pd.DataFrame.from_dict(self.game_info), "game_data"
                    # )
                    # clean cache after write to db
                    total_crawled_game += len(self.game_info["gamble_id"])
                    # self.game_info = defaultdict(list)

                # get prediction info for each prediction group
                self.get_prediction_data(date, soup, prediction_group)
                # self.write_to_db(
                #     pd.DataFrame.from_dict(self.prediction[prediction_group]),
                #     "{}_{}".format("prediction_data", prediction_group),
                # )
                # clean cache after write to db
                # self.prediction[prediction_group] = defaultdict(list)

            yield {"game_data": self.game_info, "prediction_data": self.prediction}
            self.init_prediction_data()

        self.logger.info(
            "crawler task done, total crawled games: {}, days: {}".format(
                total_crawled_game, (end_date - start_date).days
            )
        )
        return

    def crawl(self, date):
        logging.debug(f"start crawling: {date}")
        res = requests.get(self.get_url(date))
        soup = BeautifulSoup(res.text, "html.parser")

        # iterate for every 2 rows, the first for the guest and game info, the second for the host
        for guest_row, host_row in zip(
            *[iter(soup.find("tbody").findAll("tr", {"class": "game-set"}))] * 2
        ):
            # parse data
            # game info
            gamble_id = RowParser.gamble_id(guest_row)
            game_time = RowParser.game_time(date, guest_row)
            team_name = RowParser.team_name(guest_row)
            scores = RowParser.scores(guest_row)

            # gambling info
            tpt, tpr_over = RowParser.total_point_threshold(guest_row)
            guest_sp, guest_spr = RowParser.spread_point(guest_row)
            guest_or = RowParser.origin_response(guest_row)
            _, tpr_under = RowParser.total_point_threshold(host_row)
            host_sp, host_spr = RowParser.spread_point(host_row)
            host_or = RowParser.origin_response(host_row)

            # prediction
            # TODO: confirm under and over
            under_tpp = RowParser.total_point_prediction(guest_row)
            over_tpp = RowParser.total_point_prediction(host_row)

            tpj = self.judge_total_point(scores, tpt)
            tpm = self.total_point_major_pred(tpj, under_tpp, over_tpp)

            guest_spp = RowParser.spread_point_prediction(guest_row)
            host_spp = RowParser.spread_point_prediction(host_row)
            spj = self.judge_spread_point(scores, guest_sp, host_sp)
            spm = self.point_major_predict(spj, guest_sp, host_sp)
            guest_op = RowParser.original_prediction(guest_row)
            host_op = RowParser.original_prediction(host_row)
            oj = self.judge_original(scores)
            om = self.point_major_predict(oj, guest_op, host_op)

            # builder
            # game info
            builder = DocumentBuilder()
            builder.gamble_id(gamble_id)
            builder.game_time(game_time)
            builder.sports_type(self.game_type)
            builder.guest_name(team_name["guest"])
            builder.host_name(team_name["host"])
            builder.guest_score(scores["guest"])
            builder.host_score(scores["host"])

            # gambling info
            builder.total_point_threshold(tpt)
            builder.total_point_resp_under(tpr_under)
            builder.total_point_resp_over(tpr_over)
            builder.total_point_pred_under(under_tpp)
            builder.total_point_pred_over(over_tpp)
            builder.total_point_major_predict(self.total_point_major_pred())
            builder.spread_point_guest(guest_sp)
            builder.spread_point_host(host_sp)
            builder.spread_point_resp_guest(guest_spr)
            builder.spread_point_resp_host(host_spr)
            builder.spread_point_pred_guest(guest_spp)
            builder.spread_point_pred_host(host_spp)
            builder.original_resp_guest(guest_or)
            builder.original_resp_host(host_or)
            builder.original_pred_guest(guest_op)
            builder.original_pred_host(host_op)

            # judge
            builder.total_point_judge(self.judge_total_point(scores, tpt))
            builder.spread_point_judge(
                self.judge_spread_point(scores, guest_sp, host_sp)
            )
            builder.original_judge(self.judge_original(scores))

            document = builder.to_document()

            try:
                document.save()
            except NotUniqueError as e:
                logging.warning(f"duplicated data: {e}")
            except Exception as e:
                logging.error(f"unknown error: {e}")

            logging.debug(f"crawled document: {document.to_mongo()}")
            return document

    def judge_total_point(self, scores, threshold):
        total_point = scores["guest"] + scores["host"]
        return (
            "under"
            if total_point < threshold
            else "over"
            if total_point > threshold
            else None
        )

    def judge_spread_point(self, scores, guest_sp, host_sp):
        return (
            "guest"
            if scores["host"] + host_sp < scores["guest"]
            else "host"
            if scores["guest"] + guest_sp < scores["host"]
            else None
        )

    def judge_original(self, scores):
        return (
            "guest"
            if scores["host"] < scores["guest"]
            else "host"
            if scores["guest"] < scores["host"]
            else None
        )

    def total_point_major_pred(self, result, under_pup, over_pup):
        if (result == "under" and over_pup < under_pup) or (
            result == "over" and under_pup < over_pup
        ):
            return True
        else:
            return False

    def point_major_predict(self, result, guest_pup, host_pup):
        if (result == "guest" and host_pup < guest_pup) or (
            result == "host" and guest_pup < host_pup
        ):
            return True
        else:
            return False

    def init_prediction_data(self):
        self.game_info = defaultdict(list)
        self.prediction = {
            "all_member": defaultdict(list),
            "more_than_sixty": defaultdict(list),
            "all_prefer": defaultdict(list),
            "top_100": defaultdict(list),
        }

    def get_game_data(self, date, soup):
        self.logger.debug("getting game data: {}".format(date))
        guest_row = True
        for row_content in soup.find("tbody").findAll("tr", {"class": "game-set"}):
            if guest_row:
                # assert self.check_data_consistent(self.game_info)
                self.append_game_id_and_type(row_content, date)
                self.append_game_time(row_content)
                self.append_team_name(row_content)
                self.append_score(row_content)
                self.append_total_point_info(row_content)
            self.append_point_spread_info(row_content, guest_row)
            self.append_response_ratio_info(row_content, guest_row)
            guest_row = not guest_row
        self.logger.info("finished crawl and parse game data: {}".format(date))
        return

    def get_prediction_data(self, date, soup, group):
        self.logger.info("start crawl and parse prediction data: {}".format(date))
        guest_row = True
        for row_content in soup.find("tbody").findAll("tr", {"class": "game-set"}):
            if guest_row:
                assert self.check_data_consistent(self.prediction[group])
                self.append_game_id_and_type(row_content, date, group)
            self.append_prediction_national_point_spread(row_content, guest_row, group)
            self.append_prediction_national_total_point(row_content, guest_row, group)
            self.append_prediction_local_point_spread(row_content, guest_row, group)
            self.append_prediction_local_total_point(row_content, guest_row, group)
            self.append_prediction_local_original(row_content, guest_row, group)
            guest_row = not guest_row
        self.logger.info("finished crawl and parse prediction data: {}".format(date))
        return

    def append_prediction_national_point_spread(self, row_content, guest_row, group):
        date = (
            row_content.find("td", {"class": "td-universal-bet01"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        if guest_row:
            self.prediction[group]["percentage_national_point_spread_guest"].append(
                percentage
            )
            self.prediction[group]["population_national_point_spread_guest"].append(
                population
            )
        else:
            self.prediction[group]["percentage_national_point_spread_host"].append(
                percentage
            )
            self.prediction[group]["population_national_point_spread_host"].append(
                population
            )
        return

    def append_prediction_national_total_point(self, row_content, guest_row, group):
        date = (
            row_content.find("td", {"class": "td-universal-bet02"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        if guest_row:
            self.prediction[group]["percentage_national_total_point_over"].append(
                percentage
            )
            self.prediction[group]["population_national_total_point_over"].append(
                population
            )
        else:
            self.prediction[group]["percentage_national_total_point_under"].append(
                percentage
            )
            self.prediction[group]["population_national_total_point_under"].append(
                population
            )
        return

    def append_prediction_local_point_spread(self, row_content, guest_row, group):
        date = (
            row_content.find("td", {"class": "td-bank-bet01"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        if guest_row:
            self.prediction[group]["percentage_local_point_spread_guest"].append(
                percentage
            )
            self.prediction[group]["population_local_point_spread_guest"].append(
                population
            )
        else:
            self.prediction[group]["percentage_local_point_spread_host"].append(
                percentage
            )
            self.prediction[group]["population_local_point_spread_host"].append(
                population
            )
        return

    def append_prediction_local_total_point(self, row_content, guest_row, group):
        date = (
            row_content.find("td", {"class": "td-bank-bet02"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        if guest_row:
            self.prediction[group]["percentage_local_total_point_over"].append(
                percentage
            )
            self.prediction[group]["population_local_total_point_over"].append(
                population
            )
        else:
            self.prediction[group]["percentage_local_total_point_under"].append(
                percentage
            )
            self.prediction[group]["population_local_total_point_under"].append(
                population
            )
        return

    def append_prediction_local_original(self, row_content, guest_row, group):
        date = (
            row_content.find("td", {"class": "td-bank-bet03"})
            .find_next("td")
            .text.strip()
        )
        date = re.findall(r"\d+", date)
        percentage, population = date if len(date) == 2 else (0, 0)
        if guest_row:
            self.prediction[group]["percentage_local_original_guest"].append(percentage)
            self.prediction[group]["population_local_original_guest"].append(population)
        else:
            self.prediction[group]["percentage_local_original_host"].append(percentage)
            self.prediction[group]["population_local_original_host"].append(population)
        return

    def append_game_id_and_type(self, row_content, date, group=None):
        gamble_id = row_content.find("td", "td-gameinfo").find("h3").text
        gamble_id = gamble_id if gamble_id else None
        # prediction-related table
        if group:
            self.logger.debug(
                "current column size of {}: {}".format(
                    "gamble_id", len(self.prediction[group]["gamble_id"])
                )
            )
            self.prediction[group]["game_date"].append(date)
            self.prediction[group]["gamble_id"].append(gamble_id)
            self.prediction[group]["game_type"].append(self.game_type)
        # game_data table
        else:
            self.logger.debug(
                "current column size of {}: {}".format(
                    "gamble_id", len(self.game_info["gamble_id"])
                )
            )
            self.game_info["game_date"].append(date)
            self.game_info["gamble_id"].append(gamble_id)
            self.game_info["game_type"].append(self.game_type)

        self.logger.info(
            "append gamble id: {}, game type: {}".format(gamble_id, self.game_type)
        )
        return

    def n_append_game_id_and_type(self, row_content):
        gamble_id = row_content.find("td", "td-gameinfo").find("h3").text

    def append_game_time(self, row_content):
        self.logger.info(
            "current column size of {}: {}".format(
                "play_time", len(self.game_info["play_time"])
            )
        )
        game_time = row_content.find("td", "td-gameinfo").find("h4").text
        apm, time = game_time.split()
        self.logger.info("append play time: {}".format(game_time))
        self.game_info["play_time"].append(time)
        self.game_info["am_pm"].append(apm)
        return

    def append_score(self, row_content):
        self.logger.info(
            "current column size of {}: {}".format(
                "guest_score", len(self.game_info["guest_score"])
            )
        )
        self.logger.info(
            "current column size of {}: {}".format(
                "host_score", len(self.game_info["host_score"])
            )
        )
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
        self.logger.debug("append guest: {}, host: {}".format(guest, host))
        self.game_info["guest_score"].append(guest)
        self.game_info["host_score"].append(host)
        return

    def append_team_name(self, row_content):
        self.logger.info(
            "current column size of {}: {}".format(
                "guest_id", len(self.game_info["guest"])
            )
        )
        self.logger.info(
            "current column size of {}: {}".format(
                "host_id", len(self.game_info["host"])
            )
        )

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

        guest_abbreviate = (
            team_name_mapping[guest] if guest in team_name_mapping.keys() else guest
        )
        host_abbreviate = (
            team_name_mapping[host] if host in team_name_mapping.keys() else host
        )

        self.logger.debug("append guest: {}, host: {}".format(guest, host))
        self.game_info["guest"].append(guest_abbreviate)
        self.game_info["host"].append(host_abbreviate)
        return

    def append_point_spread_info(self, row_content, guest_row):
        spread_info = row_content.find(
            "td", {"class": "td-universal-bet01"}
        ).text.strip()
        if len(spread_info) > 1:
            # get national point spread info
            self.logger.debug("the row contains national spread point info")
            national_spread_from = "chinese_mapping"[spread_info[0]]
            national_spread_point, hit_percentage = re.findall(r"\d+", spread_info)
            hit_result = "chinese_mapping"[re.findall(r"[輸贏]", spread_info)[0]]
            hit_percentage = (
                int(hit_percentage) + 100 if hit_result else int(hit_percentage)
            )
            national_spread_point = (
                -int(national_spread_point)
                if national_spread_from == "guest"
                else int(national_spread_point)
            )
            self.game_info["national_host_point_spread"].append(national_spread_point)
            self.game_info["win_if_meet_spread_point"].append(hit_result)
            self.game_info["response_ratio_if_hit_spread_point"].append(
                hit_percentage / 100
            )
        elif len(spread_info) == 0 and guest_row:
            # no this kind of gambling
            self.logger.info("no gambling information")
            self.game_info["national_host_point_spread"].append(None)
            self.game_info["win_if_meet_spread_point"].append(None)
            self.game_info["response_ratio_if_hit_spread_point"].append(None)

        if guest_row:
            (
                local_host_spread_point,
                spread_point_response_ratio,
            ) = self.get_spread_point_and_response(row_content)
            self.game_info["local_host_point_spread"].append(
                float(local_host_spread_point)
            )
            self.game_info["local_host_point_spread_response_ratio"].append(
                float(spread_point_response_ratio)
            )

            (
                local_total_point,
                total_point_under_response,
            ) = self.get_threshold_and_response(row_content)
            self.game_info["local_total_point_threshold"].append(
                float(local_total_point)
            )
            self.game_info["local_total_point_under_threshold_response_ratio"].append(
                float(total_point_under_response)
            )
        else:
            # add guest response ratio on point spread gamble
            _, spread_point_response_ratio = self.get_spread_point_and_response(
                row_content
            )
            self.game_info["local_guest_point_spread_response_ratio"].append(
                float(spread_point_response_ratio)
            )

            # add response ratio of total point over
            _, total_point_over_response = self.get_threshold_and_response(row_content)
            self.game_info["local_total_point_over_threshold_response_ratio"].append(
                float(total_point_over_response)
            )

        return

    def get_threshold_and_response(self, row_content):
        # get local total point info and response ratio
        local_total_point = row_content.find(
            "td", {"class": "td-bank-bet02"}
        ).text.strip()
        # filter out float
        data = re.findall(r"\d+\.\d+", local_total_point)
        return data if len(data) == 2 else (0, 0)

    def get_spread_point_and_response(self, row_content):
        # get local point spread info and response ratio
        local_host_spread_point = row_content.find(
            "td", {"class": "td-bank-bet01"}
        ).text.strip()
        # filter out float
        data = re.findall(r"[+-]?\d+\.\d+", local_host_spread_point)
        return data if len(data) == 2 else (0, 0)

    def append_total_point_info(self, row_content):
        # get national total point info
        national_total_point = row_content.find(
            "td", {"class": "td-universal-bet02"}
        ).text.strip()
        threshold = re.findall(r"\d+[\.\d+]?", national_total_point)
        national_total_point = threshold[0] if threshold else 0
        self.game_info["national_total_point_threshold"].append(
            float(national_total_point)
        )
        pass

    def append_response_ratio_info(self, row_content, guest_row):
        self.logger.info("append response ratio info")
        if guest_row:
            # get guest response ratio of no point spread at local
            local_origin_guest_response_ratio = row_content.find(
                "td", {"class": "td-bank-bet03"}
            ).text.strip()
            ratio = re.findall(r"\d+\.\d+", local_origin_guest_response_ratio)
            local_origin_guest_response_ratio = ratio[0] if ratio else 0
            self.game_info["local_origin_guest_response_ratio"].append(
                float(local_origin_guest_response_ratio)
            )
        else:
            # get host response ratio of no point spread at local
            local_origin_host_response_ratio = row_content.find(
                "td", {"class": "td-bank-bet03"}
            ).text.strip()
            ratio = re.findall(r"\d+\.\d+", local_origin_host_response_ratio)
            local_origin_host_response_ratio = ratio[0] if ratio else 0
            self.game_info["local_origin_host_response_ratio"].append(
                float(local_origin_host_response_ratio)
            )

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

    def check_data_consistent(self, table):
        length_list = [len(i) for i in table.values()]
        return (
            length_list.count(length_list[0]) == len(length_list)
            if length_list
            else True
        )

    def get_url(self, date, member_type=1):
        return self.config["crawler"]["urlPattern"].format(
            game_type=game_type_map[self.game_type],
            game_date=date,
            group_type=member_type,
        )

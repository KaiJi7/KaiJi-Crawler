import logging
import sys

import requests
from bs4 import BeautifulSoup
from mongoengine.errors import NotUniqueError

from crawler.common import game_type_map
from crawler.document_builder import DocumentBuilder
from crawler.row_parser import RowParser
from util.util import Util


class Crawler:
    def __init__(self, game_type):
        self.game_type = game_type

    def run(self, date):
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
            tpt = RowParser.total_point_threshold(guest_row)
            tpr_over = RowParser.total_point_response(guest_row)
            guest_sp = RowParser.spread_point(guest_row)
            guest_spr = RowParser.spread_point_response(guest_row)
            guest_or = RowParser.origin_response(guest_row)
            tpr_under = RowParser.total_point_response(host_row)
            host_sp = RowParser.spread_point(host_row)
            host_spr = RowParser.spread_point_response(host_row)
            host_or = RowParser.origin_response(host_row)

            # prediction
            over_tpp = RowParser.total_point_prediction(guest_row)
            under_tpp = RowParser.total_point_prediction(host_row)

            tpj = self.judge_total_point(scores, tpt)
            tpm = self.total_point_major_pred(tpj, under_tpp, over_tpp)

            guest_spp = RowParser.spread_point_prediction(guest_row)
            host_spp = RowParser.spread_point_prediction(host_row)
            spj = self.judge_spread_point(scores, guest_sp, host_sp)
            spm = self.point_major_predict(spj, guest_spp, host_spp)
            guest_op = RowParser.original_prediction(guest_row)
            host_op = RowParser.original_prediction(host_row)
            oj = self.judge_original(scores)
            om = self.point_major_predict(oj, guest_op, host_op)

            # builder
            # game info
            builder = DocumentBuilder()
            builder.gamble_id(gamble_id)
            builder.game_time(game_time)
            builder.game_type(self.game_type)
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
            builder.total_point_major_predict(tpm)
            builder.spread_point_guest(guest_sp)
            builder.spread_point_host(host_sp)
            builder.spread_point_resp_guest(guest_spr)
            builder.spread_point_resp_host(host_spr)
            builder.spread_point_pred_guest(guest_spp)
            builder.spread_point_pred_host(host_spp)
            builder.spread_point_major_predict(spm)
            builder.original_resp_guest(guest_or)
            builder.original_resp_host(host_or)
            builder.original_pred_guest(guest_op)
            builder.original_pred_host(host_op)
            builder.original_major_predict(om)

            # judge
            builder.total_point_judge(tpj)
            builder.spread_point_judge(spj)
            builder.original_judge(oj)

            document = builder.to_document()

            try:
                logging.debug(f"saving document: {document}")
                # TODO: cannot save chinese word
                document.save()
            except NotUniqueError as e:
                logging.warning(f"duplicated data: {e}")
                continue
            except Exception as e:
                logging.error(f"unknown error: {e}")
                sys.exit(-1)

            logging.debug(f"crawled document: {document.to_mongo()}")

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

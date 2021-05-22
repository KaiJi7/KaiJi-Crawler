import logging

from src.db.collection.betting import Betting, BET_GUEST, BET_HOST, BET_UNDER, BET_OVER, DATA_SOURCE_WILD_MEMBER
from src.db.collection.gambling import GAMBLING_TYPE_ORIGINAL, GAMBLING_TYPE_SPREAD_POINT, GAMBLING_TYPE_TOTAL_SCORE
from src.db.collection.gambling import Gambling
from src.parser.row_parser import RowParser


def parse_betting(gambling_id, gambling: Gambling, guest_row, host_row) -> Betting:
    gambling_type = gambling.get_data()["type"]
    betting = Betting(gambling_id)
    betting.set_source(DATA_SOURCE_WILD_MEMBER)
    if gambling_type == GAMBLING_TYPE_ORIGINAL:
        betting.set_bet(BET_GUEST, RowParser.original_prediction(guest_row)["population"])
        betting.set_bet(BET_HOST, RowParser.original_prediction(host_row)["population"])
        return betting

    if gambling_type == GAMBLING_TYPE_SPREAD_POINT:
        betting.set_bet(BET_GUEST, RowParser.spread_point_prediction(guest_row)["population"])
        betting.set_bet(BET_HOST, RowParser.spread_point_prediction(host_row)["population"])
        return betting

    if gambling_type == GAMBLING_TYPE_TOTAL_SCORE:
        betting.set_bet(BET_UNDER, RowParser.total_point_prediction(host_row)["population"])
        betting.set_bet(BET_OVER, RowParser.total_point_prediction(guest_row)["population"])
        return betting

    logging.warning(f"invalid gambling type: {gambling_type}")
    raise Exception(f"invalid gambling type: {gambling_type}")

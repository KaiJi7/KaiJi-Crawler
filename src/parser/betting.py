from typing import List
from src.db.collection.betting import Betting, BET_GUEST, BET_HOST, BET_UNDER, BET_OVER
from src.crawler.row_parser import RowParser


def parse_betting(gambling_id, guest_row, host_row) -> List[Betting]:
    ori = Betting(gambling_id)
    sp = Betting(gambling_id)
    tp = Betting(gambling_id)

    ori.set_bet(BET_GUEST, RowParser.original_prediction(guest_row)["population"])
    ori.set_bet(BET_HOST, RowParser.original_prediction(host_row)["population"])

    sp.set_bet(BET_GUEST, RowParser.spread_point_prediction(guest_row)["population"])
    sp.set_bet(BET_HOST, RowParser.spread_point_prediction(host_row)["population"])

    tp.set_bet(BET_UNDER, RowParser.total_point_prediction(host_row)["population"])
    tp.set_bet(BET_OVER, RowParser.total_point_prediction(guest_row)["population"])

    return [ori, sp, tp]

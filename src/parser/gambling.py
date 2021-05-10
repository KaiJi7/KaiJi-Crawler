from src.db.collection.gambling import Gambling
from src.crawler.row_parser import RowParser
from src.db.collection.gambling import BET_GUEST, BET_HOST, BET_OVER, BET_UNDER, GAMBLING_TYPE_ORIGINAL, \
    GAMBLING_TYPE_SPREAD_POINT, GAMBLING_TYPE_TOTAL_SCORE
from typing import List


def parse_gambling(game_id, guest_row, host_row) -> List[Gambling]:

    ori = Gambling(game_id)
    sp = Gambling(game_id)
    tp = Gambling(game_id)

    ori.set_type(GAMBLING_TYPE_ORIGINAL)
    ori.set_odds(BET_GUEST, RowParser.origin_response(guest_row))
    ori.set_odds(BET_HOST, RowParser.origin_response(host_row))

    sp.set_type(GAMBLING_TYPE_SPREAD_POINT)
    sp.set_odds(BET_GUEST, RowParser.spread_point_response(guest_row))
    sp.set_odds(BET_HOST, RowParser.spread_point_response(host_row))
    sp.set_property("guest_spread_point", RowParser.spread_point(guest_row))
    sp.set_property("host_spread_point", RowParser.spread_point(host_row))

    tp.set_type(GAMBLING_TYPE_TOTAL_SCORE)
    tp.set_odds(BET_UNDER, RowParser.total_point_response(host_row))
    tp.set_odds(BET_OVER, RowParser.total_point_response(guest_row))
    tp.set_property("threshold", RowParser.total_point_threshold(guest_row))

    return [ori, sp, tp]

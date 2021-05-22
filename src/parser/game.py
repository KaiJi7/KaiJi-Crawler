from src.db.collection.game import Game
from src.parser.row_parser import RowParser
from src.crawler.common import host_timezone
import pytz
import logging


def parse_game(game_type: str, date, guest_row, host_row) -> Game:
    game_time = RowParser.game_time(date, guest_row)
    team_name = RowParser.team_name(guest_row)
    scores = RowParser.scores(guest_row)

    g = Game()
    g.set_type(game_type)
    g.set_guest_name(team_name["guest"])
    g.set_host_name(team_name["host"])
    g.set_guest_score(scores["guest"])
    g.set_host_score(scores["host"])
    g.set_start_time(game_time)

    try:
        tz = pytz.timezone(host_timezone.get(team_name["host"], ""))
        g.set_start_time_local(tz.localize(game_time))
    except Exception as e:
        logging.warning(f"unknown to get timezone info of team: {team_name['host']}. {e}")

    return g

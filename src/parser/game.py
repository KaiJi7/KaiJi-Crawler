from src.crawler.row_parser import RowParser
from src.db.collection.game import Game


def parse_game(date, guest_row, host_row) -> Game:
    game_time = RowParser.game_time(date, guest_row)
    team_name = RowParser.team_name(guest_row)
    scores = RowParser.scores(guest_row)

    g = Game()
    g.set_guest_name(team_name["guest"])
    g.set_host_name(team_name["host"])
    g.set_guest_score(scores["guest"])
    g.set_host_score(scores["host"])
    g.set_start_time(game_time)

    # TODO: local start time map
    # TODO: location map based on host

    return g





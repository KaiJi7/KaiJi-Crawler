from db.collection.sports import template
from db.collection.sports import SportsData
import logging


class DocumentBuilder:
    def __init__(self):
        self.document = template.copy()
        logging.debug("document builder initialized")

    def to_document(self):
        return SportsData(**self.document)

    def game_id(self, data):
        self.document["game_id"] = data

    def gamble_id(self, data):
        self.document["gamble_id"] = data

    def game_time(self, data):
        self.document["game_time"] = data

    def game_type(self, data):
        self.document["game_type"] = data

    def guest_name(self, data):
        self.document["guest"]["name"] = data

    def guest_score(self, data):
        self.document["guest"]["score"] = data

    def host_name(self, data):
        self.document["host"]["name"] = data

    def host_score(self, data):
        self.document["host"]["score"] = data

    def total_point_threshold(self, data):
        self.document["gamble_info"]["total_point"]["threshold"] = data

    def total_point_resp_under(self, data):
        self.document["gamble_info"]["total_point"]["response"]["under"] = data

    def total_point_resp_over(self, data):
        self.document["gamble_info"]["total_point"]["response"]["over"] = data

    def total_point_pred_under(self, data):
        self.document["gamble_info"]["total_point"]["prediction"]["under"][
            "percentage"
        ] = data["percentage"]
        self.document["gamble_info"]["total_point"]["prediction"]["under"][
            "population"
        ] = data["population"]

    def total_point_pred_over(self, data):
        self.document["gamble_info"]["total_point"]["prediction"]["over"][
            "percentage"
        ] = data["percentage"]
        self.document["gamble_info"]["total_point"]["prediction"]["over"][
            "population"
        ] = data["population"]

    def total_point_major_predict(self, data):
        self.document["gamble_info"]["total_point"]["prediction"]["major"] = data

    def total_point_judge(self, data):
        self.document["gamble_info"]["total_point"]["judgement"] = data

    def spread_point_guest(self, data):
        self.document["gamble_info"]["spread_point"]["guest"] = data

    def spread_point_host(self, data):
        self.document["gamble_info"]["spread_point"]["host"] = data

    def spread_point_resp_guest(self, data):
        self.document["gamble_info"]["spread_point"]["response"]["guest"] = data

    def spread_point_resp_host(self, data):
        self.document["gamble_info"]["spread_point"]["response"]["host"] = data

    def spread_point_pred_guest(self, data):
        self.document["gamble_info"]["spread_point"]["prediction"]["guest"][
            "percentage"
        ] = data["percentage"]
        self.document["gamble_info"]["spread_point"]["prediction"]["guest"][
            "population"
        ] = data["population"]

    def spread_point_pred_host(self, data):
        self.document["gamble_info"]["spread_point"]["prediction"]["host"][
            "percentage"
        ] = data["percentage"]
        self.document["gamble_info"]["spread_point"]["prediction"]["host"][
            "population"
        ] = data["population"]

    def spread_point_major_predict(self, data):
        self.document["gamble_info"]["spread_point"]["prediction"]["major"] = data

    def spread_point_judge(self, data):
        self.document["gamble_info"]["spread_point"]["judgement"] = data

    def original_resp_guest(self, data):
        self.document["gamble_info"]["original"]["response"]["guest"] = data

    def original_resp_host(self, data):
        self.document["gamble_info"]["original"]["response"]["host"] = data

    def original_pred_guest(self, data):
        self.document["gamble_info"]["original"]["prediction"]["guest"][
            "percentage"
        ] = data["percentage"]
        self.document["gamble_info"]["original"]["prediction"]["guest"][
            "population"
        ] = data["population"]

    def original_pred_host(self, data):
        self.document["gamble_info"]["original"]["prediction"]["host"][
            "percentage"
        ] = data["percentage"]
        self.document["gamble_info"]["original"]["prediction"]["host"][
            "population"
        ] = data["population"]

    def original_major_predict(self, data):
        self.document["gamble_info"]["original"]["prediction"]["major"] = data

    def original_judge(self, data):
        self.document["gamble_info"]["original"]["judgement"] = data

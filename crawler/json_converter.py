import datetime
import json
import math

import numpy as np
import pandas as pd
from pymongo import MongoClient

from config.logger import get_logger
from util.util import Util
from util.singleton import Singleton


class JsonConverter(metaclass=Singleton):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

        self.config = Util().get_config()
        self.db = Util().get_db_connection()
        self.mongo_client = MongoClient(
            host=self.config["mongoDb"]["host"],
            port=self.config["mongoDb"]["port"],
            username=self.config["mongoDb"]["username"],
            password=self.config["mongoDb"]["password"],
            authMechanism="SCRAM-SHA-1",
        )["gambling_simulation"]["sports_data"]

    def to_json(self, daily_data):
        json_document = {}
        self.add_common_info(daily_data, json_document)
        self.add_score(daily_data, json_document)
        self.add_gamble_info(daily_data, json_document)

    def start(self):
        self.logger.info("start converter")

        for index, row in self.get_game_data(start_id=self.last_converted_id()).iterrows():
            row = self.join_row(row)
            self.logger.debug("wipe game id: {}".format(index))
            json_document = {}
            self.add_common_info(row, json_document, index)
            self.add_score(row, json_document)
            self.add_gamble_info(row, json_document)
            self.add_judgement_info(row, json_document)
            for group in self.get_prediction_groups():
                self.add_prediction_info(row, json_document, group)
                self.add_prediction_judgement_info(row, json_document, group)
            self.remove_nan_key(json_document)
            self.logger.debug("wiped document: {}".format(json_document))
            self.mongo_client.replace_one(json_document, json_document, upsert=True)

    def get_prediction_groups(self):
        self.logger.debug("getting prediction group")
        cursor = self.db.cursor()
        cursor.execute("""show tables like 'prediction_data%'""")
        return [group[0][16:] for group in cursor.fetchall()]

    def add_common_info(self, daily_data, json_document, index):
        self.logger.debug(f"add common info")

        # json_document["game_id"] = index
        json_document["game_time"] = datetime.datetime.strptime(
            f"{daily_data['game_data']['game_date']} {daily_data['game_data']['play_time']}",
            f"{self.config['crawler']['dateFormat']} %I:%M",
        ).isoformat("T", "minutes")
        json_document["gamble_id"] = daily_data['game_data'].get("gamble_id", None)
        json_document["game_type"] = daily_data['game_data']["game_type"]
        return json_document

    def add_score(self, daily_data, json_document):
        self.logger.debug("add score")
        json_document["guest"] = {"name": daily_data["game_data"]["guest"], "score": daily_data["game_data"]["guest_score"]}
        json_document["host"] = {"name": daily_data["game_data"]["host"], "score": daily_data["game_data"]["host_score"]}
        return json_document

    def add_gamble_info(self, daily_data, json_document):
        self.logger.debug("add gamble info")
        json_document["gamble_info"] = {}
        json_document["gamble_info"]["national"] = {
            "total_point": {"threshold": row["national_total_point_threshold"]},
            "spread_point": {
                "host": row["national_host_point_spread"],
                "response": {"on_hit": row["response_ratio_if_hit_spread_point"]},
            },
        }
        json_document["gamble_info"]["local"] = {
            "total_point": {
                "threshold": row["local_total_point_threshold"],
                "response": {
                    "under": row["local_total_point_under_threshold_response_ratio"],
                    "over": row["local_total_point_over_threshold_response_ratio"],
                },
            },
            "spread_point": {
                "host": row["local_host_point_spread"],
                "response": {
                    "host": row["local_host_point_spread_response_ratio"],
                    "guest": row["local_guest_point_spread_response_ratio"],
                },
            },
            "original": {
                "response": {
                    "guest": row["local_origin_guest_response_ratio"],
                    "host": row["local_origin_host_response_ratio"],
                }
            },
        }
        return json_document

    def add_judgement_info(self, daily_data, json_document):
        self.logger.debug("add judgement info")
        json_document["judgement"] = json_document.get("judgement", {})
        json_document["judgement"]["game"] = json_document["judgement"].get("game", {})
        json_document["judgement"]["game"]["national"] = {
            "total_point": "over"
            if bool(row["over_total_point_national"])
            else "under",
            "spread_point": "host"
            if row["host_win_point_spread_national"]
            else "guest",
        }
        json_document["judgement"]["game"]["local"] = {
            "total_point": "over" if bool(row["over_total_point_local"]) else "under",
            "spread_point": "host" if row["host_win_point_spread_local"] else "guest",
            "original": "host" if row["host_win_original"] else "guest",
        }

    def add_prediction_info(self, daily_data, json_document, group):
        self.logger.debug("add prediction info")
        json_document["prediction"] = json_document.get("prediction", [])
        json_document["prediction"].append(
            {
                "group": group,
                "national": {
                    "total_point": {
                        "over": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_national_total_point_over", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_national_total_point_over", group
                                )
                            ],
                        },
                        "under": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_national_total_point_under", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_national_total_point_under", group
                                )
                            ],
                        },
                    },
                    "spread_point": {
                        "guest": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_national_point_spread_guest", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_national_point_spread_guest", group
                                )
                            ],
                        },
                        "host": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_national_point_spread_host", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_national_point_spread_host", group
                                )
                            ],
                        },
                    },
                },
                "local": {
                    "total_point": {
                        "over": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_local_total_point_over", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_local_total_point_over", group
                                )
                            ],
                        },
                        "under": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_local_total_point_under", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_local_total_point_under", group
                                )
                            ],
                        },
                    },
                    "spread_point": {
                        "guest": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_local_point_spread_guest", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_local_point_spread_guest", group
                                )
                            ],
                        },
                        "host": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_local_point_spread_host", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_local_point_spread_host", group
                                )
                            ],
                        },
                    },
                    "original": {
                        "guest": {
                            "percentage": row[
                                "{}__{}".format(
                                    "percentage_local_original_guest", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "population_local_original_guest", group
                                )
                            ],
                        },
                        "host": {
                            "percentage": row[
                                "{}__{}".format("percentage_local_original_host", group)
                            ],
                            "population": row[
                                "{}__{}".format("population_local_original_host", group)
                            ],
                        },
                    },
                },
            }
        )

    def add_prediction_judgement_info(self, daily_data, json_document, group):
        self.logger.debug("add prediction judgement info")
        json_document["judgement"] = json_document.get("judgement", {})
        json_document["judgement"]["prediction"] = json_document["judgement"].get(
            "prediction", []
        )
        json_document["judgement"]["prediction"].append(
            {
                "group": group,
                "national": {
                    "total_point": {
                        "matched_info": {
                            "is_major": bool(
                                row[
                                    "{}__{}".format(
                                        "national_total_point_result", group
                                    )
                                ]
                            ),
                            "percentage": row[
                                "{}__{}".format(
                                    "national_total_point_percentage", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "national_total_point_population", group
                                )
                            ],
                        }
                    },
                    "spread_point": {
                        "matched_info": {
                            "is_major": bool(
                                row[
                                    "{}__{}".format(
                                        "national_point_spread_result", group
                                    )
                                ]
                            ),
                            "percentage": row[
                                "{}__{}".format(
                                    "national_point_spread_percentage", group
                                )
                            ],
                            "population": row[
                                "{}__{}".format(
                                    "national_point_spread_population", group
                                )
                            ],
                        }
                    },
                },
                "local": {
                    "total_point": {
                        "matched_info": {
                            "is_major": bool(
                                row["{}__{}".format("local_total_point_result", group)]
                            ),
                            "percentage": row[
                                "{}__{}".format("local_total_point_percentage", group)
                            ],
                            "population": row[
                                "{}__{}".format("local_total_point_population", group)
                            ],
                        }
                    },
                    "spread_point": {
                        "matched_info": {
                            "is_major": bool(
                                row["{}__{}".format("local_point_spread_result", group)]
                            ),
                            "percentage": row[
                                "{}__{}".format("local_point_spread_percentage", group)
                            ],
                            "population": row[
                                "{}__{}".format("local_point_spread_population", group)
                            ],
                        }
                    },
                    "original": {
                        "matched_info": {
                            "is_major": bool(
                                row["{}__{}".format("local_original_result", group)]
                            ),
                            "percentage": row[
                                "{}__{}".format("local_original_percentage", group)
                            ],
                            "population": row[
                                "{}__{}".format("local_original_population", group)
                            ],
                        }
                    },
                },
            }
        )

    def remove_nan_key(self, json_document):
        for k, v in list(json_document.items()):
            self.logger.debug("check: {}-{}".format(k, v))
            if isinstance(v, dict):
                self.remove_nan_key(v)
            elif isinstance(v, list):
                for e in v:
                    self.remove_nan_key(e)
            elif not pd.notnull(v):
                self.logger.debug("delete key-value: {}, {}".format(k, v))
                json_document.pop(k, None)

    def iter_table_as_df(self):
        self.logger.info("start iterate db")
        # cursor = self.db.cursor()
        # cursor.execute('SHOW TABLES')
        for table_name in constant.joined_tables:
            self.logger.debug("get table: {}".format(table_name))
            yield table_name, pd.read_sql(
                "SELECT * FROM %s LIMIT 10" % (table_name), con=self.db, index_col="id"
            )

    def get_game_data(self, start_id=0):
        sql = f"SELECT * FROM game_data WHERE game_data.id > {start_id}"
        return pd.read_sql(sql, con=self.db, index_col="id")

    def join_row(self, daily_data):
        sql_template = (
            lambda table_name: f"SELECT * FROM {table_name} WHERE game_date = {row['game_date']} AND gamble_id = {row['gamble_id']} AND game_type = '{row['game_type']}'"
        )

        def update_row(table, update_column=False):
            sql = sql_template(table)
            df = pd.read_sql(sql, con=self.db, index_col="id")
            if update_column:
                add_suffix(df)
            a = df.iloc[0]
            assert len(df) == 1

            for _, r in df.iterrows():
                b = r
            return row.combine(a, lambda x, y: x or y, fill_value=0)

        def add_suffix(df):
            columns = [(c, f"{c}__{group}") for c in df.iloc[:, 3:].columns.values]
            df.rename(columns=dict(columns), inplace=True)

        row = update_row("game_judgement")
        for group in ["all_member", "all_prefer", "top_100", "more_than_sixty"]:
            row = update_row(f"prediction_data_{group}", update_column=True)
            row = update_row(f"prediction_judgement_{group}", update_column=True)

        # due to numpy.int64 un-serializable for json, have to convert to normal python object first.
        return json.loads(row.to_json())

    def last_converted_id(self):
        last_id = self.mongo_client.find_one(
            {}, {"game_id": True, "_id": False}, sort=[("game_id", -1)]
        )
        if last_id:
            return last_id["game_id"]
        else:
            return 0

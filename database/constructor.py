from sqlalchemy import MetaData, Table, Column, Integer, String, Float, Index

from config.constant import global_constant
from config.logger import get_logger
from util.util import Util


class DbConstructor(object):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util().get_config()
        self.engine = Util().get_db_engine()

    def create_schema(self, force=False):
        self.logger.info("start create schema, type force: {}".format(force))
        if force:
            self.engine.execute(
                "DROP DATABASE IF EXISTS {}".format(
                    self.config[global_constant.DB][global_constant.schema]
                )
            )

        self.engine.execute(
            "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".format(
                self.config[global_constant.DB][global_constant.schema]
            )
        )
        return

    def create_tables(self):
        # self.engine.execute('USE {}'.format(self.config[string_constant.DB][string_constant.schema]))
        game_data = Table(
            "game_data",
            MetaData(),
            Column("id", Integer, primary_key=True, autoincrement="ignore_fk"),
            Column("game_date", Integer),
            Column("gamble_id", Integer),
            Column("game_type", String(16)),
            Column("play_time", String(10)),
            Column("am_pm", String(2)),
            Column("guest", String(64)),
            Column("host", String(64)),
            Column("guest_score", Integer),
            Column("host_score", Integer),
            Column("national_total_point_threshold", Float),
            Column("national_host_point_spread", Integer),
            Column("win_if_meet_spread_point", Integer),
            Column("response_ratio_if_hit_spread_point", Float),
            Column("local_host_point_spread", Float),
            Column("local_host_point_spread_response_ratio", Float),
            Column("local_guest_point_spread_response_ratio", Float),
            Column("local_total_point_threshold", Float),
            Column("local_total_point_under_threshold_response_ratio", Float),
            Column("local_total_point_over_threshold_response_ratio", Float),
            Column("local_origin_guest_response_ratio", Float),
            Column("local_origin_host_response_ratio", Float),
            Index("game_index", "game_date", "gamble_id", "game_type", unique=True),
            schema=self.config[global_constant.DB][global_constant.schema],
        )

        # prediction table template for each prediction group
        def template(table_name):
            return Table(
                "{}_{}".format("prediction_data", table_name),
                MetaData(),
                Column("id", Integer, primary_key=True, autoincrement="ignore_fk"),
                Column("game_date", Integer),
                Column("gamble_id", Integer),
                Column("game_type", String(16)),
                Column("percentage_national_point_spread_guest", Integer),
                Column("population_national_point_spread_guest", Integer),
                Column("percentage_national_total_point_over", Integer),
                Column("population_national_total_point_over", Integer),
                Column("percentage_local_point_spread_guest", Integer),
                Column("population_local_point_spread_guest", Integer),
                Column("percentage_local_total_point_over", Integer),
                Column("population_local_total_point_over", Integer),
                Column("percentage_local_original_guest", Integer),
                Column("population_local_original_guest", Integer),
                Column("percentage_national_point_spread_host", Integer),
                Column("population_national_point_spread_host", Integer),
                Column("percentage_national_total_point_under", Integer),
                Column("population_national_total_point_under", Integer),
                Column("percentage_local_point_spread_host", Integer),
                Column("population_local_point_spread_host", Integer),
                Column("percentage_local_total_point_under", Integer),
                Column("population_local_total_point_under", Integer),
                Column("percentage_local_original_host", Integer),
                Column("population_local_original_host", Integer),
                Index("game_index", "game_date", "gamble_id", "game_type", unique=True),
                schema=self.config[global_constant.DB][global_constant.schema],
            )

        # create each table
        self.create_if_not_exist(game_data)
        self.create_if_not_exist(game_data)
        self.create_if_not_exist(game_judgement)
        self.create_if_not_exist(template("all_member"))
        self.create_if_not_exist(template("more_than_sixty"))
        self.create_if_not_exist(template("all_prefer"))
        self.create_if_not_exist(template("top_100"))

        return

    def create_if_not_exist(self, table):
        if table.exists(bind=self.engine):
            self.logger.info("table {} already exist, skip creation".format(table.name))
            return
        self.logger.info("create table: {}".format(table.name))
        table.create(self.engine)

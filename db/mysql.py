from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    Index,
)

from config.constant import global_constant
from config.logger import get_logger
from util.util import Util


class MySqlDbConstructor:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util().get_config()
        self.engine = Util().get_db_engine()

    def create_schema(self, force=False):
        self.logger.info("start create schema, type force: {}".format(force))
        if force:
            self.engine.execute(
                "DROP DATABASE IF EXISTS {}".format(self.config["DB"]["schema"])
            )

        self.engine.execute(
            "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".format(
                self.config["DB"]["schema"]
            )
        )
        return

    def create_tables(self):
        game_judgement = Table(
            "game_judgement",
            MetaData(),
            Column("id", Integer, primary_key=True, autoincrement="ignore_fk"),
            Column("game_date", Integer),
            Column("gamble_id", Integer),
            Column("game_type", String(16)),
            Column("host_win_original", Integer),
            Column("host_win_point_spread_national", Integer),
            Column("host_win_point_spread_local", Integer),
            Column("over_total_point_national", Integer),
            Column("over_total_point_local", Integer),
            Index("game_index", "game_date", "gamble_id", "game_type", unique=True),
            schema=self.config[global_constant.DB][global_constant.schema],
        )

        prediction_judgement_summarize = Table(
            "prediction_judgement_summarize",
            MetaData(),
            Column("member_group", String(30)),
            Column("national_point_spread_win_ratio", Float),
            Column("national_point_spread_max_continuous_lose", Integer),
            Column("national_point_spread_number_of_valid_game", Integer),
            Column("national_total_point_win_ratio", Float),
            Column("national_total_point_max_continuous_lose", Integer),
            Column("national_total_point_number_of_valid_game", Integer),
            Column("local_point_spread_win_ratio", Float),
            Column("local_point_spread_max_continuous_lose", Integer),
            Column("local_point_spread_number_of_valid_game", Integer),
            Column("local_total_point_win_ratio", Float),
            Column("local_total_point_max_continuous_lose", Integer),
            Column("local_total_point_number_of_valid_game", Integer),
            Column("local_original_win_ratio", Float),
            Column("local_original_max_continuous_lose", Integer),
            Column("local_original_number_of_valid_game", Integer),
            # Index("game_index", "game_date", "gamble_id", "game_type", unique=True),
            schema=self.config["DB"]["schema"],
        )

        # prediction judge table template for each prediction group
        def prediction_judgement_template(table_name):
            return Table(
                "{}_{}".format("prediction_judgement", table_name),
                MetaData(),
                Column("id", Integer, primary_key=True, autoincrement="ignore_fk"),
                Column("game_date", Integer),
                Column("gamble_id", Integer),
                Column("game_type", String(16)),
                Column("national_point_spread_result", Integer),
                Column("national_point_spread_percentage", Integer),
                Column("national_point_spread_population", Integer),
                Column("national_total_point_result", Integer),
                Column("national_total_point_percentage", Integer),
                Column("national_total_point_population", Integer),
                Column("local_point_spread_result", Integer),
                Column("local_point_spread_percentage", Integer),
                Column("local_point_spread_population", Integer),
                Column("local_total_point_result", Integer),
                Column("local_total_point_percentage", Integer),
                Column("local_total_point_population", Integer),
                Column("local_original_result", Integer),
                Column("local_original_percentage", Integer),
                Column("local_original_population", Integer),
                Index("game_index", "game_date", "gamble_id", "game_type", unique=True),
                schema=self.config["DB"]["schema"],
            )

        # create each table
        self.create_if_not_exist(game_judgement)
        self.create_if_not_exist(prediction_judgement_summarize)

        self.create_if_not_exist(prediction_judgement_template("all_member"))
        self.create_if_not_exist(prediction_judgement_template("more_than_sixty"))
        self.create_if_not_exist(prediction_judgement_template("all_prefer"))
        self.create_if_not_exist(prediction_judgement_template("top_100"))

        return

    def create_if_not_exist(self, table):
        if table.exists(bind=self.engine):
            self.logger.info("table {} already exist, skip creation".format(table.name))
            return
        self.logger.info("create table: {}".format(table.name))
        table.create(self.engine)

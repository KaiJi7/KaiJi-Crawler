from sqlalchemy import MetaData, Table, Column, Integer, String, Float

from config.constant import database as db_constant
from config.constant import global_constant
from config.logger import get_logger
from util.util import Util


class DbConstructor(object):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.config = Util().get_config()
        self.engine = Util().get_db_engine()

    def create_schema(self, force=False):
        self.logger.info('start create schema, type force: {}'.format(force))
        if force:
            self.engine.execute(
                "DROP DATABASE IF EXISTS {}".format(self.config[global_constant.DB][global_constant.schema]))

        self.engine.execute(
            "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".format(
                self.config[global_constant.DB][global_constant.schema]))
        return

    def create_table(self):
        game_data = Table(db_constant.game_data, MetaData(),
                          Column(db_constant.row_id, Integer, primary_key=True, autoincrement='ignore_fk'),
                          Column(db_constant.game_date, Integer),
                          Column(db_constant.gamble_id, Integer),
                          Column(db_constant.game_type, String(16)),
                          Column(db_constant.play_time, String(10)),
                          Column(db_constant.am_pm, String(2)),
                          Column(db_constant.guest, String(10)),
                          Column(db_constant.host, String(10)),
                          Column(db_constant.guest_score, Integer),
                          Column(db_constant.host_score, Integer),
                          Column(db_constant.national_total_point_threshold, Float),
                          Column(db_constant.national_host_point_spread, Integer),
                          Column(db_constant.win_if_meet_spread_point, Integer),
                          Column(db_constant.response_ratio_if_hit_spread_point, Float),
                          Column(db_constant.local_host_point_spread, Float),
                          Column(db_constant.local_host_point_spread_response_ratio, Float),
                          Column(db_constant.local_total_point_threshold, Float),
                          Column(db_constant.local_total_point_threshold_response_ratio, Float),
                          Column(db_constant.local_origin_guest_response_ratio, Float),
                          Column(db_constant.local_origin_host_response_ratio, Float),
                          schema=self.config[global_constant.DB][global_constant.schema])

        self.create_if_not_exist(game_data)

        return

    def create_if_not_exist(self, table):
        if table.exists(bind=self.engine):
            self.logger.info('table {} already exist, skip creation'.format(table.name))
            return
        self.logger.info('create table: {}'.format(table.name))
        table.create(self.engine)

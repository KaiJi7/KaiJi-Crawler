import datetime
import time

import click
import schedule
import yaml
from dateutil.relativedelta import relativedelta

from config.constant import global_constant
from crawler.crawler import Crawler
from crawler.data_updater import DataUpdater
from database.constructor import DbConstructor
from util.util import Util


@click.group(chain=True)
def cli():
    pass


@click.command("create_db", help="Create DB.")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Drop schema before create.",
    show_default=True,
)
@click.option(
    "--create_schema",
    "-cs",
    is_flag=True,
    default=False,
    help="Create schema.",
    show_default=True,
)
@click.option(
    "--create_table",
    "-ct",
    is_flag=True,
    default=True,
    help="Create table.",
    show_default=True,
)
def task_create_db(force, create_schema, create_table):
    constructor = DbConstructor()
    if create_schema:
        constructor.create_schema(force=force)
    if create_table:
        constructor.create_tables()


@click.command("crawl_data", help="Start crawler to get sports gambling data.")
@click.option(
    "--start_date",
    "-sd",
    type=str,
    required=False,
    default=datetime.datetime.strftime(
        datetime.datetime.now() - relativedelta(days=7), "%Y%m%d"
    ),
    show_default=True,
    help="Start date of sports gambling, the format must follow the pattern: YYYYmmDD, ex: 20190130.",
)
@click.option(
    "--end_date",
    "-ed",
    type=str,
    required=False,
    default=datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d"),
    show_default=True,
    help="End date of sports gambling, the format must follow the pattern: YYYYmmDD, ex: 20190130.",
)
@click.option(
    "--game_type",
    "-gt",
    type=click.Choice(global_constant.game_type_map.keys()),
    default=global_constant.NBA,
    help="Target game type.",
    show_default=True,
)
def task_crawler(start_date, end_date, game_type):
    Crawler(game_type=game_type).start_crawler(start_date=start_date, end_date=end_date)


@click.command("update_db", help="Update game data based on game_season.yml")
@click.option(
    "--keep_update",
    "-k",
    is_flag=True,
    default=False,
    help="Keeping update.",
    show_default=True,
)
def task_update_db(keep_update):
    click.echo("update db")
    if keep_update:
        Util().load_environment_variable()
        schedule.every(Util().get_config()["data_updater"]["update_period"]).hours.do(
            DataUpdater().update_db
        )

        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        DataUpdater().update_db()


if __name__ == "__main__":
    cli.add_command(task_create_db)
    cli.add_command(task_crawler)
    cli.add_command(task_update_db)
    cli()

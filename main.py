import logging

from commander.commander import Commander
from db.client import Client
from util.util import Util


def init():
    # logger
    log_level = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARN,
        "ERROR": logging.ERROR,
    }
    logging.basicConfig(
        level=log_level[Util.get_config()["logging"]["level"]],
        format="%(asctime)s %(filename)s %(lineno)d %(name)s: %(levelname)s %(message)s",
    )
    logging.debug("logger initialized")

    # db
    Client()


def main():
    init()
    Commander().start()


if __name__ == "__main__":
    main()

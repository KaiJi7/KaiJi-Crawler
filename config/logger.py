import logging
import datetime
import yaml
import os

init = False


def get_logger(name):
    global init
    if not init:
        os.makedirs('log', exist_ok=True)
        with open('config/configuration.yml', 'r') as config:
            level = logging.getLevelName(yaml.load(config, Loader=yaml.FullLoader)['logging']['level'])
            logging.basicConfig(level=level,
                                format='%(asctime)s %(filename)s %(lineno)d %(name)s: %(levelname)s %(message)s',
                                datefmt='%y-%m-%d %H:%M:%S',
                                filename='log/{:%Y-%m-%d}.log'.format(datetime.datetime.now()))

        console = logging.StreamHandler()
        console.setLevel(level)

        formatter = logging.Formatter('%(asctime)s %(filename)s %(lineno)d %(name)s: %(levelname)s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger().addHandler(console)
        init = True

    logger = logging.getLogger(name)

    return logger

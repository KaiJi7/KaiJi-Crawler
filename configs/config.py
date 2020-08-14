from util.singleton import Singleton
import yaml
import logging


Config = None


# def init_config():
#     with open("configs/config.yml") as config:
#         global Config = yaml.load(config, Loader=yaml.FullLoader)
#         logging.debug('config initialized')
#
#
# class Config(metaclass=Singleton):
#     def __init__(self):
#         with open("configs/config.yml") as config:
#             self.config = yaml.load(config, Loader=yaml.FullLoader)
#
#         logging.debug('config initialized')
#
#     def get(self):
#         return self.config

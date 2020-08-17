import datetime

import pandas as pd
import pymysql
import yaml
from dateutil.relativedelta import relativedelta
from flatten_dict import flatten

from util.util import Util

team_name_mapping = {
    "密爾瓦基公鹿": "MIL",
    "亞特蘭大老鷹": "ATL",
    "達拉斯獨行俠": "DAL",
    "洛杉磯湖人": "LAL",
    "紐奧良鵜鶘": "NO",
    "沙加緬度國王": "SAC",
    "聖安東尼奧馬刺": "SA",
    "華盛頓巫師": "WAS",
    "丹佛金塊": "DEN",
    "夏洛特黃蜂": "CHA",
    "金州勇士": "GS",
    "曼斐斯灰熊": "MEM",
    "洛杉磯快艇": "LAC",
    "奧克拉荷馬雷霆": "OKC",
    "底特律活塞": "DET",
    "紐約尼克": "NY",
    "克里夫蘭騎士": "CLE",
    "印第安那溜馬": "IND",
    "多倫多暴龍": "TOR",
    "休士頓火箭": "HOU",
    "波士頓塞爾提克": "BOS",
    "費城76人": "PHI",
    "奧蘭多魔術": "ORL",
    "鳳凰城太陽": "PHX",
    "猶他爵士": "UTA",
    "布魯克林籃網": "BKN",
    "波特蘭拓荒者": "POR",
    "邁阿密熱火": "MIA",
    "芝加哥公牛": "CHI",
    "明尼蘇達灰狼": "MIN",
}

game_type_map = {"MLB": 1, "NPB": 2, "NBA": 3, "FOOTBALL": 4, "CPBL": 6, "WNBA": 7}

game_season = {
    "NBA": {
        "18": {"begin": "20181017", "end": "20190614"},
        "19": {"begin": "20191001", "end": "20200930"},
        "20": {"begin": "20201001", "end": "20210630"},
    },
    "MLB": {
        "18": {"begin": "20180329", "end": "20181001"},
        "19": {"begin": "20190320", "end": "20191020"},
    },
    "NPB": {"18": {"begin": "20180330", "end": "20181103"}},
}

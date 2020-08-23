# KaiJi Crawler

[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://github.com/tterb/atomic-design-ui/blob/master/LICENSEs)
[![Build Status](https://travis-ci.org/boennemann/badges.svg?branch=master)](https://travis-ci.org/boennemann/badges)    

Crawler for sports competition data.

Data source: https://www.playsport.cc/index.php

Supported sports:
 
* NBA
* MLB
* NPB

## Build

```
$ docker-compose build
```

## Docker Image

```
$ docker pull allensyk/sports_data_crawler
```

| Environment Variable | Description |
| :--- | :--- |
| DB_HOST | DB host address. |
| DB_PORT | DB port. |
| DB_USERNAME | DB username. |
| DB_PASSWORD | DB user password. |

## Crawled Data

Each crawled competition data would be save as a JSON document into mongoDB.

### Example

```json
{
    "_id" : ObjectId("5f3fee94a48cf4de2d416516"),
    "game_time" : ISODate("2019-10-04T01:00:00.000Z"),
    "gamble_id" : "313",
    "game_type" : "NBA",
    "guest" : {
        "name" : "HOU",
        "score" : 109
    },
    "host" : {
        "name" : "LAC",
        "score" : 96
    },
    "gamble_info" : {
        "total_point" : {
            "threshold" : 224.5,
            "response" : {
                "under" : 1.75,
                "over" : 1.75
            },
            "judgement" : "under",
            "prediction" : {
                "under" : {
                    "percentage" : 30.0,
                    "population" : 147
                },
                "over" : {
                    "percentage" : 70.0,
                    "population" : 335
                },
                "major" : false
            }
        },
        "spread_point" : {
            "guest" : -4.5,
            "host" : 4.5,
            "response" : {
                "guest" : 1.7,
                "host" : 1.8
            },
            "judgement" : "guest",
            "prediction" : {
                "guest" : {
                    "percentage" : 53.0,
                    "population" : 322
                },
                "host" : {
                    "percentage" : 47.0,
                    "population" : 283
                },
                "major" : true
            }
        },
        "original" : {
            "response" : {
                "guest" : null,
                "host" : null
            },
            "judgement" : "guest",
            "prediction" : {
                "guest" : {
                    "percentage" : 0.0,
                    "population" : 0
                },
                "host" : {
                    "percentage" : 0.0,
                    "population" : 0
                },
                "major" : false
            }
        }
    }
}
```
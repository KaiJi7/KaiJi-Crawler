# Sports Data Crawler

A CLI to crawl sports competition information into DB.

Data source: https://www.playsport.cc/index.php

## Usage
```
$ python3 main.py --help
Usage: main.py [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...

Options:
  --help  Show this message and exit.

Commands:
  crawl_data  Start crawler to get sports gambling data.
  create_db   Create DB.
  update_db   Update game data based on game_season.yml
  
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
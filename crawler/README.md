# Crawler

Crawl history data of sports competition according to the given date range
then store into MySQL for analysis.

The competition data comes from: https://www.playsport.cc/index.php

## Parameter

The date string have to follow the pattern: "YYYYmmDD".

| Parameter | type | Description | Example |
| :---: | :---: | :--- | :---: |
| start_date | string | Start date to get competition data. | '20180928' |
| end_date | string | Start date to get competition data. | '20181231' |

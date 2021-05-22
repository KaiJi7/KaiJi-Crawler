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

Each crawled competition data would be save as JSON document into mongoDB.

### 

## Setup Develop Environment

cd to the root of the project

### 1. Create Virtual Environment

```
$ python3 -m venv venv
```

### 2. Active Into The Virtual Environment

```
$ source venv/bin/activate
```

### 3. Install All The Dependencies

```
$ pip3 install -r requirements.txt 
```
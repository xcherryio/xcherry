# xCherry
Server and main repo of xCherry project

[![Go Report Card](https://goreportcard.com/badge/github.com/xcherryio/xcherry)](https://goreportcard.com/report/github.com/xcherryio/xcherry)

[![Coverage Status](https://codecov.io/github/xcherryio/xcherry/coverage.svg?branch=main)](https://app.codecov.io/gh/xcherryio/xcherry/branch/main)

[![Build status](https://github.com/xcherryio/xcherry/actions/workflows/ci-postgres14.yaml/badge.svg?branch=main)](https://github.com/xcherryio/xcherry/actions/workflows/ci-postgres14.yaml)


# Documentation

See [wiki](https://github.com/xcherryio/xcherry/wiki).

# How to use 

As a coding framework, iWF provides three SDKs to use with:


* [xCherry Golang SDK](https://github.com/xcherryio/sdk-go) and [samples](https://github.com/xcherryio/samples-go)
* [xCherry Java SDK](https://github.com/xcherryio/sdk-java) and [samples](https://github.com/xcherryio/samples-java)
* [xCherry Python SDK](https://github.com/xcherryio/sdk-python) and [samples](https://github.com/xcherryio/samples-python)
* [xCherry Typescript SDK](https://github.com/xcherryio/sdk-typescript) and [samples](https://github.com/xcherryio/samples-typescript)

The xCherry SDKs/Samples require to run with the server by one of the below options:


### Option 1: use example docker-compose of xCherry with a database
In this case you don't want to connect to your existing database:

* `wget https://raw.githubusercontent.com/xcherryio/xcherry/main/docker-compose/docker-compose-postgres14-example.yaml && docker compose -f docker-compose-postgres14-example.yaml up -d`
  * Will include a PostgresSQL database and some [sample tables](./extensions/postgres/schema/sample_tables.sql)


### Option 2: use docker-compose of xCherry to connect with your own database
* Install the database schema to your database
  * [Postgres schema](./extensions/postgres/schema)
* Run docker-compose file from this project:
  * `docker-compose -f ./docker-compose/docker-compose.yaml up`

# Contribution 
See [contribution](./CONTRIBUTING.md) 

### Option 3: brew install
TODO: brew install xCherry binaries

# Development Plan
See [development plan](https://github.com/xcherryio/sdk-go/wiki/DevelopmentPlan)
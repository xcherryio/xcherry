# xdb
Server and main repo of XDB project

[![Go Report Card](https://goreportcard.com/badge/github.com/xdblab/xdb)](https://goreportcard.com/report/github.com/xdblab/xdb)

[![Coverage Status](https://codecov.io/github/xdblab/xdb/coverage.svg?branch=main)](https://app.codecov.io/gh/xdblab/xdb/branch/main)

[![Build status](https://github.com/xdblab/xdb/actions/workflows/ci-postgres14.yaml/badge.svg?branch=main)](https://github.com/xdblab/xdb/actions/workflows/ci-postgres14.yaml)


# Documentation

See [wiki](https://github.com/xdblab/xdb/wiki).

# How to use 

### Option 1: brew install
TODO: brew install xdb


### Option 2: use docker-compose of xdb to connect with your own database
* Install the database schema to your database
  * [Postgres schema](./extensions/postgres/schema)
* Run docker-compose file from this project:
  * `docker-compose -f ./docker-compose/docker-compose.yaml up`

### Option 3: use example docker-compose of xdb with a database
In this case you don't want to connect to your existing database:

* `docker-compose -f ./docker-compose/docker-compose-postgres-example.yaml up`
  * Will include a PostgresSQL database 
# Contribution 
See [contribution](./CONTRIBUTING.md) 

# Development Plan
## 1.0
- [ ] StartProcessExecution API
  - [x] Basic
  - [x] ProcessIdReusePolicy
  - [ ] Process timeout
  - [ ] Retention policy after closed
- [ ] Executing `wait_until`/`execute` APIs
  - [x] Basic sequential execution
  - [x] Parallel execution of multiple states
  - [x] StateOption: WaitUntil/Execute API timeout and retry policy
  - [ ] AsyncState failure policy for recovery
- [ ] StateDecision
  - [x] Single next State
  - [x] Multiple next states
  - [x] Force completing process
  - [x] Graceful completing process
  - [x] Force fail process
  - [x] Dead end
  - [ ] Conditional complete process with checking queue emptiness
- [ ] Commands
  - [ ] AnyOfCompletion and AllOfCompletion waitingType
  - [ ] TimerCommand
- [ ] LocalQueue
  - [ ] LocalQueueCommand
  - [ ] MessageId for deduplication
  - [ ] SendMessage API without RPC
- [ ] LocalAttribute persistence
  - [ ] LoadingPolicy (attribute selection + locking)
  - [ ] InitialUpsert
- [ ] GlobalAttribute  persistence
  - [ ] LoadingPolicy (attribute selection + locking)
  - [ ] InitialUpsert
  - [ ] Multi-tables
- [ ] RPC
  - [ ] Basic
  - [ ] Persistence
  - [ ] Communication
- [ ] API error handling for canceled, failed, timeout, terminated
- [x] StopProcessExecution API
- [ ] WaitForStateCompletion API
- [ ] ResetStateExecution for operation
- [x] DescribeProcessExecution API
- [ ] WaitForProcessCompletion API
- [ ] History events for operation/debugging

## Future

- [ ] Skip timer API for testing/operation
- [ ] Dynamic attributes and queue definition
- [ ] State options overridden dynamically
- [ ] Consume more than one messages in a single command with FIFO/BestMatch policies
- [ ] WaitingType: AnyCombinationsOf
- [ ] GlobalQueue
- [ ] CronSchedule
- [ ] Batch operation
- [ ] DelayStart
- [ ] Caching (with Redis, etc)
- [ ] Custom Database Query
- [ ] SearchAttribute (with ElasticSearch, etc)
- [ ] ExternalAttribute (with S3, Snowflake, etc)
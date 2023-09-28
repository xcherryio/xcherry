# xdb
Server and main repo of XDB project

[![Go Report Card](https://goreportcard.com/badge/github.com/xdblab/xdb)](https://goreportcard.com/report/github.com/xdblab/xdb)

[![Coverage Status](https://codecov.io/github/xdblab/xdb/coverage.svg?branch=main)](https://app.codecov.io/gh/xdblab/xdb/branch/main)

[![Build status](https://github.com/xdblab/xdb/actions/workflows/ci-postgres.yaml/badge.svg?branch=main)](https://github.com/xdblab/xdb/actions/workflows/ci-postgres.yaml)


# Documentation

See [wiki](https://github.com/xdblab/xdb/wiki).

# Development 

## Dependencies
Run one of the [docker-compose files](./docker-compose/dev) to run a database + Apache Pulsar

## Build
* To build all the binaries: `make bins`
* To run integration tests
  * `make integTestsPostgres` will test against Postgres + Pulsar
  * `make integTestsAll` will test all supported databases + Pulsar

## Run

### server
* To run it with [default config](./config/development.yaml) `./xdb-server`. Or see help: `./xdb-server -h`
  * Alternatively, clicking the run button in an IDE should work.

### tools
* To run database tools:
  * see `./xdb-tools-postgres -h` for postgres tool (for setup database, schema, etc)
  
## 1.0
- [ ] StartProcessExecution API
  - [x] Basic
  - [ ] ProcessIdReusePolicy
  - [ ] Process timeout
  - [ ] Retention policy after closed
- [ ] Executing `wait_until`/`execute` APIs
  - [ ] Basic
  - [ ] Parallel execution of multiple states
  - [ ] StateOption: WaitUntil/Execute API timeout and retry policy
  - [ ] AsyncState failure policy for recovery
- [ ] StateDecision
  - [ ] Single next State
  - [ ] Multiple next states
  - [ ] Force completing process
  - [ ] Graceful completing process
  - [ ] Force fail process
  - [ ] Dead end
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
- [ ] API error handling for canceled, failed, timeout, terminated
- [ ] StopProcessExecution API
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
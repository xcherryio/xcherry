# xdb
Server and main repo of XDB project

# Documentation

See [wiki](https://github.com/xdblab/xdb/wiki).

# Development Plan

## 1.0
- [ ] Start ProcessExecution
    - [ ] Basic
    - [ ] ProcessIdReusePolicy
    - [ ] Process timeout
    - [ ] Retention policy after closed
- [ ] Executing `wait_until`/`execute` APIs
- [ ] StateDecision
    - [ ] Single next State
    - [ ] Multiple next states
    - [ ] Force completing process
    - [ ] Graceful completing process
    - [ ] Force fail process
    - [ ] Dead end
    - [ ] Conditional complete workflow with checking queue emptiness
- [ ] Parallel execution of multiple states
- [ ] WaitForProcessCompletion API
- [ ] StateOption: WaitUntil/Execute API timeout and retry policy
- [ ] AnyOfCompletion and AllOfCompletion waitingType
- [ ] TimerCommand
- [ ] LocalQueue
    - [ ] LocalQueueCommand
    - [ ] MessageId for deduplication
- [ ] LocalAttribute
    - [ ] LoadingPolicy (attribute selection + locking)
    - [ ] InitialUpsert
- [ ] GlobalAttribute
    - [ ] LoadingPolicy (attribute selection + locking)
    - [ ] InitialUpsert
    - [ ] Multi-tables
- [ ] Stop ProcessExecution
- [ ] Error handling for canceled, failed, timeout, terminated
- [ ] AsyncState failure policy for recovery
- [ ] RPC
- [ ] WaitForStateCompletion API
- [ ] ResetStateExecution for operation
- [ ] Describe ProcessExecution API
- [ ] History events for operation/debugging

## Future

- [ ] Skip timer API for testing/operation
- [ ] Dynamic attributes and queue definition
- [ ] State options overridden dynamically
- [ ] Consume more than one messages in a single command
    - [ ] FIFO/BestMatch policies
- [ ] WaitingType: AnyCombinationsOf
- [ ] GlobalQueue
- [ ] CronSchedule
- [ ] Batch operation
- [ ] DelayStart
- [ ] Caching (with Redis, etc)
- [ ] Custom Database Query
- [ ] SearchAttribute (with ElasticSearch, etc)
- [ ] ExternalAttribute (with S3, Snowflake, etc)
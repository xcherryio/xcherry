# Extensions

The goal of XDB is to write **MINIMUM** code to support any database. XDB creates a minimum interface to implement an extension.

This folder is for the extensions that XDB is extending:
* [SQL DB interfaces](./sql_db_interfaces.go) defines the contracts of how to implement an extension for a SQL database
  * A [go-sql-driver](https://github.com/golang/go/wiki/SQLDrivers) is required in order to use this interface 
* [NoSQL DB interfaces](./nosql_db_interfaces.go) defines the contracts of how to implement an extension for a NoSQL database
  * This is more generic interface for any database that doesn't works with the [go-sql-driver](https://github.com/golang/go/wiki/SQLDrivers) 


XDB will support any database as long as it can:
* Execute transactions on multiple tables with locking
* Has CDC(Change data capture) support (ideally, works with [Apache Pulsar Connect](https://pulsar.apache.org/docs/3.1.x/io-cdc-debezium/))

## Steps to implement a new database extension
TODO more details
* Implement interface
* Makefile for tools binary
* Schema
* Integration test
* CI

Each sub-directory should contain all the implementation for that database specific. For example, all the MySQL specific
logic should be in `mysql/` package, including:
* SQL schemas
* SQL implementations
* Error handling
* Testing
* Tooling logic
* etc
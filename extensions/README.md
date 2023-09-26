# Extensions

The goal of XDB is to write **MINIMUM** code to support any database. XDB creates a minimum interface to implement an extension.

This [interfaces](./interfaces.go) defines the contracts of how to implement an extension.

This folder is for the extensions that XDB is extending.

Each sub-directory should contain all the implementation for that database specific. For example, all the MySQL specific 
logic should be in `mysql/` package, including:
* SQL schemas
* SQL implementations
* Error handling
* Testing
* Tooling logic
* etc

 

   
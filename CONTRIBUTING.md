# Develop xCherry server

Any contribution is welcome. Even just a fix for typo in a code comment, or README/wiki.

See [Wiki](https://github.com/xcherryio/xcherry/wiki) for how it works.

Here is the repository layout if you are interested to learn about it:

* `cmd/` the code to bootstrap the server
* `service/` service implementation
    * `api/` API service implementation
    * `common/` some common libraries


## Build
* To build all the binaries: `make bins`

## Run server

* Prepare a supported database
    * If you don't have one, run a Postgres with [default config(with Postgres)](./config/development-postgres.yaml)
    * Run `./xcherry-tools-postgres install-schema` to install the required schema to your database
        * See more options in `./xcherry-tools-postgres`
* Then Run `./xcherry-server`.
    * Or see more options: `./xcherry-server -h`
    * Alternatively, clicking the run button in an IDE should also work(after schemas are install).

## Run Integration Test against the started server
Once the server is running:
* `make integTestsWithLocalServer` will run [the integration tests defined in this repo](./integTests).

  
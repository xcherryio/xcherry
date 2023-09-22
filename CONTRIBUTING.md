# Develop XDB server

Any contribution is welcome. Even just a fix for typo in a code comment, or README/wiki.

See [Wiki](https://github.com/xdblab/xdb/wiki) for how it works.

Here is the repository layout if you are interested to learn about it:

* `cmd/` the code to bootstrap the server
* `gen/` the generated code from xdb-apis (Open API definition/Swagger)
* `xdb-apis/` the xdb-apis submodule
* `service/` service implementation
    * `api/` API service implementation
    * `common/` some common libraries

## How to update xdb-apis and the generated code

1. Install openapi-generator using Homebrew if you haven't. See
   more [documentation](https://openapi-generator.tech/docs/installation)
2. Check out the xdb-apis submodule by running the command: `git submodule update --init --recursive`
3. Run the command `git submodule update --remote --merge` to update xdb-apis to the latest commit
4. Run `make apis-code-gen` to refresh the generated code. The command requires to have `openapi-generator` CLI.See
   the [openapi-generator doc](https://openapi-generator.tech/docs/installation/) for how to install it. And you may
   also need to upgrade it to the latest if it's older than what we are currently using.

An easy way to install openapi-generator CLI is to use Homebrew:

```
brew install openapi-generator
```

And to upgrade it:

```
brew update && brew upgrade openapi-generator
```

# How to run server

The first step you may want to explore is to run it locally!

* If you are in an IDE, you can run the main function in `./cmd/main.go` with argument `start`.
* Or in terminal `go run cmd/server/main.go start`
* Or build the binary and run it by `make bins` and then run `./xdb-server start`
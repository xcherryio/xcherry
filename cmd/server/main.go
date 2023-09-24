package main

import (
	"os"

	"github.com/xdblab/xdb/cmd/server/xdb"
)

// main entry point for the xdb server
func main() {
	app := xdb.BuildCLI()
	app.Run(os.Args)
}

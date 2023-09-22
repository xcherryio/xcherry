package main

import (
	"github.com/xdblab/xdb/cmd/server/xdb"
	"os"
)

// main entry point for the xdb server
func main() {
	app := xdb.BuildCLI()
	app.Run(os.Args)
}

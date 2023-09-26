package main

import (
	"github.com/xdblab/xdb/extensions/postgres/postgrestool"
	"log"
	"os"
)

func main() {
	app := postgrestool.BuildCLIOptions()
	
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

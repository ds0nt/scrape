package main

import (
	"context"
	"flag"
	"log"

	"github.com/ds0nt/scrape/scraped/service"
	"github.com/sirupsen/logrus"
)

var dataDir = flag.String("data-dir", "", "Data Directory for storing scraped files")

func main() {
	flag.Parse()

	ctx := context.Background()
	s := service.ScrapeDaemon{
		logger:  logrus.StandardLogger(),
		dataDir: *dataDir,
	}

	log.Fatal(s.start(ctx))
}

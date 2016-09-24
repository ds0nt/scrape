package main

import (
	"context"
	"flag"
	"log"

	"github.com/ds0nt/scrape/scraped/service"
)

var dataDir = flag.String("data-dir", "", "Data Directory for storing scraped files")

func main() {
	flag.Parse()

	ctx := context.Background()
	s := service.NewScrapeDaemon(*dataDir)

	log.Fatal(s.Start(ctx))
}

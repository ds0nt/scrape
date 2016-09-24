package main

import (
	"flag"

	"golang.org/x/net/context"

	"github.com/sirupsen/logrus"

	"github.com/ds0nt/scrape/scraped/service"
)

var addr = flag.String("addr", ":2100", "Listen Address")
var dataDir = flag.String("data-dir", "", "Data Directory for storing scraped files")

func main() {
	flag.Parse()

	s := service.NewScrapeDaemon(service.Config{
		DataDir: *dataDir,
		Addr:    *addr,
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		if err := s.Start(ctx); err != nil {
			logrus.Println("service error:", err)
		}
	}()

	select {
	case <-ctx.Done():
		logrus.Println("error:", ctx.Err())
	}
}

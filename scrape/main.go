package main

import (
	"flag"
	"log"
	"time"

	"golang.org/x/net/context"

	pb "github.com/ds0nt/scrape/scraped/service"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var addr = flag.String("addr", ":2100", "Listen Address")
var url = flag.String("url", "", "url to scrape")

func main() {
	flag.Parse()
	if *url == "" {
		log.Println("url is required")
		return
	}

	err := start()
	if err != nil {
		log.Println(err)
	}
}

func start() error {

	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "did not connect")
	}
	defer conn.Close()
	c := pb.NewScrapeServiceClient(conn)

	res, err := c.CreateTask(context.Background(), &pb.CreateTaskRequest{
		Task: &pb.TaskParams{
			Url:      *url,
			StartAt:  uint64(time.Now().Add(time.Second * 5).Unix()),
			Interval: uint64(15),
		},
	})
	if err != nil {
		return errors.Wrap(err, "could not create task")
	}
	log.Println("created task:", res.String())

	res2, err2 := c.GetTasks(context.Background(), &pb.GetTasksRequest{})
	if err2 != nil {
		return errors.Wrap(err, "could not create task")
	}
	log.Println("tasks:", res2.String())

	return nil
}

func CreateTask() {

}

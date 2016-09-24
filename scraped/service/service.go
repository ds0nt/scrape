package service

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ScrapeDaemon struct {
	ScrapeServiceServer
	logger     *logrus.Logger
	dataDir    string
	scrapedDir string
	tasksDB    string
	grpcAddr   string
	db         *bolt.DB
}

type Config struct {
	DataDir string
	Addr    string
}

func NewScrapeDaemon(config Config) *ScrapeDaemon {
	s := ScrapeDaemon{
		logger:   logrus.StandardLogger(),
		dataDir:  config.DataDir,
		grpcAddr: config.Addr,
	}

	s.scrapedDir = filepath.Join(s.dataDir, ".scraped")
	s.tasksDB = filepath.Join(s.scrapedDir, "tasks.db")
	return &s
}

func (s *ScrapeDaemon) l() *logrus.Logger {
	return s.logger
}

func (s *ScrapeDaemon) Start(ctx context.Context) error {
	s.l().Info("Scrape Daemon Starting")

	// initialize directories
	if _, err := os.Stat(s.dataDir); err != nil {
		return errors.Wrapf(err, "Could not stat %s", s.dataDir)
	}

	_, err := os.Stat(s.scrapedDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "Could not stat %s", s.scrapedDir)
		}
		err = os.Mkdir(s.scrapedDir, os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "Could not create %s", s.scrapedDir)
		}
		s.l().Printf("Created directory %s", s.scrapedDir)
	}

	// initialize and open db
	s.db, err = bolt.Open(s.tasksDB, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return errors.Wrapf(err, "Could not open boltdb %s", s.tasksDB)
	}
	defer s.db.Close()
	s.l().Printf("Opened Bolt Database %s", s.tasksDB)

	err = s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("tasks"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "Could not create tasks bucket in boltdb", s.tasksDB)
	}

	// start tasks
	tasks, err := s.getTasks()
	if err != nil {
		return errors.Wrap(err, "Could not load tasks")
	}
	for _, t := range tasks {
		go NewSupervisor(s, *t).Start()
	}

	// create tcp conn
	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return errors.Wrap(err, "Could to listen")
	}
	s.l().Infof("Listening on TCP %s", s.grpcAddr)

	// serve
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	done := make(chan error)
	defer close(done)
	go func() {
		RegisterScrapeServiceServer(grpcServer, s)
		done <- grpcServer.Serve(lis)
	}()

	s.l().Info("Scrape Daemon Ready")

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return errors.Wrap(err, "Server stopped")
	}
}

var m = jsonpb.Marshaler{EmitDefaults: true}

// CreateTask implements
func (s *ScrapeDaemon) CreateTask(ctx context.Context, in *CreateTaskRequest) (*CreateTaskResponse, error) {
	task := Task{
		Params: in.Task,
	}

	err := s.updateTask(&task)
	if err != nil {
		return nil, err
	}
	go NewSupervisor(s, task).Start()

	return &CreateTaskResponse{
		Task: &task,
	}, nil
}

// GetTasks implements
func (s *ScrapeDaemon) GetTasks(ctx context.Context, in *GetTasksRequest) (*GetTasksResponse, error) {
	tasks, err := s.getTasks()

	return &GetTasksResponse{
		Tasks: tasks,
	}, err
}

func (s *ScrapeDaemon) updateTask(task *Task) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tasks"))
		if task.Id == 0 {
			id, _ := b.NextSequence()
			task.Id = id
		}
		str, err := m.MarshalToString(task)
		if err != nil {
			return err
		}
		return b.Put(itob(task.Id), []byte(str))
	})
}

func (s *ScrapeDaemon) getTasks() ([]*Task, error) {
	tasks := []*Task{}
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tasks"))
		return b.ForEach(func(k []byte, v []byte) error {
			task := Task{}
			err := jsonpb.UnmarshalString(string(v), &task)
			if err != nil {
				return err
			}
			tasks = append(tasks, &task)
			return nil
		})
	})
	return tasks, err
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

type Supervisor struct {
	s       *ScrapeDaemon
	task    *Task
	ticker  *time.Ticker
	stopped bool
}

func NewSupervisor(s *ScrapeDaemon, task Task) *Supervisor {
	return &Supervisor{
		task: &task,
		s:    s,
	}
}

func (s *Supervisor) Start() {
	log.Printf("Starting task %d running every %d seconds", s.task.Id, s.task.Params.Interval)
	s.ticker = time.NewTicker(time.Second * time.Duration(s.task.Params.Interval))
	s.stopped = false
	for {
		err := s.s.runTask(s.task)
		if err != nil {
			log.Println("err", err)
		}
		<-s.ticker.C
		if s.stopped {
			break
		}
	}
}
func (s *Supervisor) Stop() {
	s.stopped = true
	s.ticker.Stop()
}

func (s *ScrapeDaemon) runTask(task *Task) error {
	log.Printf("[Task %d] Scraping %s", task.Id, task.Params.Url)
	log.Println(s.dataDir)
	log.Println(fmt.Sprintf("task-%d", task.Id))
	contextDir := filepath.Join(s.dataDir, fmt.Sprintf("task-%d", task.Id))
	log.Println(contextDir)

	_, err := os.Stat(contextDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "Could not stat %s", contextDir)
		}
		err = os.Mkdir(contextDir, os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "Could not create %s", contextDir)
		}
		s.l().Printf("Created directory %s", contextDir)
	}

	res, err := http.Get(task.Params.Url)
	if err != nil {
		return errors.Wrapf(err, "failed to GET %s", task.Params.Url)
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}

	fileName := filepath.Join(contextDir, "scrape-"+time.Now().Format(time.RFC3339))
	err = ioutil.WriteFile(fileName, body, os.ModePerm)
	if err != nil {
		return errors.Wrapf(err, "failed to write file %s", fileName)
	}
	return nil
}

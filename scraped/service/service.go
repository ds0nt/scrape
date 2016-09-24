package service

import (
	"context"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ScrapeDaemon struct {
	ScrapeServiceServer
	logger     *logrus.Logger
	dataDir    string
	scrapedDir string
	tasksDB    string
}

func NewScrapeDaemon(dataDir string) *ScrapeDaemon {
	s := ScrapeDaemon{
		logger:  logrus.StandardLogger(),
		dataDir: dataDir,
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

	stat, err := os.Stat(s.scrapedDir)
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
	if !stat.IsDir() {
		return errors.Wrapf(err, "%s is not a directory", s.scrapedDir)
	}

	// initialize and open db
	db, err := bolt.Open(s.tasksDB, 0600, nil)
	if err != nil {
		return errors.Wrapf(err, "Could not open boltdb %s", s.tasksDB)
	}
	defer db.Close()
	s.l().Printf("Opened Bolt Database %s", s.tasksDB)

	s.l().Info("Scrape Daemon Started")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

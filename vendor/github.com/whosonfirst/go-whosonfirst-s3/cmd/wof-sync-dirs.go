package main

import (
	"flag"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-s3"
	"github.com/whosonfirst/go-writer-slackcat"
	"io"
	golog "log"
	"os"
	"runtime"
)

func main() {

	var root = flag.String("root", "", "The directory to sync")
	var bucket = flag.String("bucket", "", "The S3 bucket to sync <root> to")
	var prefix = flag.String("prefix", "", "A prefix inside your S3 bucket where things go")
	var debug = flag.Bool("debug", false, "Don't actually try to sync anything and spew a lot of line noise")
	var dryrun = flag.Bool("dryrun", false, "Go through the motions but don't actually clone anything")
	var credentials = flag.String("credentials", "", "Your S3 credentials file")
	var procs = flag.Int("processes", (runtime.NumCPU() * 2), "The number of concurrent processes to sync data with")
	var loglevel = flag.String("loglevel", "info", "Log level for reporting")
	var slack = flag.Bool("slack", false, "Send status updates to Slack (via slackcat)")
	var slack_config = flag.String("slack-config", "", "The path to your slackcat config")

	flag.Parse()

	if *root == "" {
		golog.Fatal("missing root to sync")
	}

	_, err := os.Stat(*root)

	if os.IsNotExist(err) {
		golog.Fatal("root does not exist")
	}

	if *bucket == "" {
		golog.Fatal("missing bucket")
	}

	if *credentials != "" {
		os.Setenv("AWS_CREDENTIAL_FILE", *credentials)
	}

	if *debug || *dryrun {
		*loglevel = "debug"
	}

	logger := log.NewWOFLogger("[wof-sync-dirs] ")

	writer := io.MultiWriter(os.Stdout)
	logger.AddLogger(writer, *loglevel)

	s := s3.WOFSync(*bucket, *prefix, *procs, *debug, logger)

	if *dryrun {
		s.Dryrun = true
	}

	s.MonitorStatus()
	err = s.SyncDirectory(*root)

	if *slack {

		sl, err := slackcat.NewWriter(*slack_config)

		if err != nil {
			logger.Warning("failed to create slackcat writer, because %v", err)
		} else {

			logger.AddLogger(sl, "status")

			logger.Status(s.StatusReport())
			logger.Status("Time to process %v", s.TimeToProcess)
		}
	}
}

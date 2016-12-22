package main

import (
	"flag"
	log "github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-s3"
	"github.com/whosonfirst/go-writer-slackcat"
	"io"
	"os"
	"runtime"
)

func main() {

	var root = flag.String("root", "", "The directory where your Who's On First data is stored")
	var bucket = flag.String("bucket", "", "The S3 bucket to sync <root> to")
	var prefix = flag.String("prefix", "", "A prefix inside your S3 bucket where things go")
	var list = flag.String("file-list", "", "A single file containing a list of files to sync")
	var debug = flag.Bool("debug", false, "Be chatty")
	var dryrun = flag.Bool("dryrun", false, "Go through the motions but don't actually clone anything")
	var tidy = flag.Bool("tidy", false, "Remove -file-list file, if present")
	var credentials = flag.String("credentials", "", "Your S3 credentials file")
	var procs = flag.Int("processes", (runtime.NumCPU() * 2), "The number of concurrent processes to sync data with")
	var loglevel = flag.String("loglevel", "info", "Log level for reporting")
	var slack = flag.Bool("slack", false, "Send status updates to Slack (via slackcat)")
	var slack_config = flag.String("slack-config", "", "The path to your slackcat config")

	flag.Parse()

	if *root == "" {
		panic("missing root")
	}

	_, err := os.Stat(*root)

	if os.IsNotExist(err) {
		panic("root does not exist")
	}

	if *bucket == "" {
		panic("missing bucket")
	}

	if *credentials != "" {
		os.Setenv("AWS_CREDENTIAL_FILE", *credentials)
	}

	if *debug || *dryrun {
		*loglevel = "debug"
	}

	logger := log.NewWOFLogger("[wof-sync-files] ")

	writer := io.MultiWriter(os.Stdout)
	logger.AddLogger(writer, *loglevel)

	s := s3.WOFSync(*bucket, *prefix, *procs, *debug, logger)
	s.MonitorStatus()

	if *dryrun {
		s.Dryrun = true
	}

	if *list == "" {
		args := flag.Args()
		s.SyncFiles(args, *root)
	} else {

		_, err := os.Stat(*list)

		if os.IsNotExist(err) {
			panic(err)
		}

		s.SyncFileList(*list, *root)

		if !*debug && *tidy {
			os.Remove(*list)
		}
	}

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

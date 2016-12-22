package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/process"
	"gopkg.in/redis.v1"
	"io"
	golog "log"
	"os"
	"strings"
)

func main() {

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")
	var githooks = flag.Bool("githooks", false, "...")
	var s3 = flag.Bool("s3", false, "...")
	var s3_bucket = flag.String("s3-bucket", "whosonfirst.mapzen.com", "...")
	var s3_prefix = flag.String("s3-prefix", "", "...")
	var data_root = flag.String("data-root", "", "...")
	var logfile = flag.String("logfile", "", "Write logging information to this file")
	var loglevel = flag.String("loglevel", "info", "The amount of logging information to include, valid options are: debug, info, status, warning, error, fatal")
	var stdout = flag.Bool("stdout", false, "...")

	flag.Parse()

	writers := make([]io.Writer, 0)

	if *stdout {
		writers = append(writers, os.Stdout)
	}

	if *logfile != "" {

		fh, err := os.OpenFile(*logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)

		if err != nil {
			golog.Fatal(err)
		}

		writers = append(writers, fh)
	}

	writer := io.MultiWriter(writers...)

	logger := log.NewWOFLogger("updated")
	logger.AddLogger(writer, *loglevel)

	processors := make([]process.Processor, 0)

	if *githooks {

		pr, err := process.NewGitHooksProcessor(*data_root, logger)

		if err != nil {
			golog.Fatal("Failed to instantiate Git hooks processor", err)
		}

		processors = append(processors, pr)
	}

	if *s3 {

		pr, err := process.NewS3Processor(*data_root, *s3_bucket, *s3_prefix, logger)

		if err != nil {
			golog.Fatal("Failed to instantiate S3 hooks processor", err)
		}

		processors = append(processors, pr)
	}

	if len(processors) == 0 {
		golog.Fatal("You forgot to specify any processors, silly")
	}

	ps_messages := make(chan string)
	up_messages := make(chan updated.UpdateTask)

	go func() {

		redis_endpoint := fmt.Sprintf("%s:%d", *redis_host, *redis_port)

		redis_client := redis.NewTCPClient(&redis.Options{
			Addr: redis_endpoint,
		})

		defer redis_client.Close()

		pubsub_client := redis_client.PubSub()
		defer pubsub_client.Close()

		err := pubsub_client.Subscribe(*redis_channel)

		if err != nil {
			logger.Fatal("Failed to subscribe to Redis channel: %s", err)
		}

		logger.Info("ready to receive pubsub messages")

		for {

			i, _ := pubsub_client.Receive()

			if msg, _ := i.(*redis.Message); msg != nil {
				// log.Println("message", msg)
				ps_messages <- msg.Payload
			}
		}

	}()

	go func() {

		logger.Info("ready to process pubsub messages")

		for {

			// we are assuming this:
			// https://github.com/whosonfirst/go-webhookd/blob/master/transformations/github.commits.go

			msg := <-ps_messages

			rdr := csv.NewReader(strings.NewReader(msg))

			tasks := make(map[string][]string)

			for {
				row, err := rdr.Read()

				if err == io.EOF {
					break
				}

				if err != nil {
					logger.Error("Failed to read data: %s", err)
					break
				}

				if len(row) != 3 {
					logger.Warning("No idea how to process row", row)
					continue
				}

				repo := row[1]
				path := row[2]

				commits, ok := tasks[repo]

				if !ok {
					commits = make([]string, 0)
				}

				commits = append(commits, path)
				tasks[repo] = commits
			}

			for repo, commits := range tasks {

				t := updated.UpdateTask{
					Repo:    repo,
					Commits: commits,
				}

				up_messages <- t
			}
		}
	}()

	logger.Info("ready to process tasks")

	for {

		task := <-up_messages
		logger.Info("got task: %s", task)

		for _, p := range processors {

			go p.Process(task)
		}
	}

	logger.Info("stop")
}

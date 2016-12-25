package main

/*

This should still be considered "wet paint" as of 20161222 but something like this:

./bin/wof-updated -pull -s3 -es -es-host es.example.com -es-index spelunker -stdout -loglevel debug -data-root /usr/local/data
updated 18:48:52.829236 [info] ready to process tasks
updated 18:48:52.830488 [info] ready to receive pubsub messages
updated 18:48:52.831441 [info] ready to process pubsub messages

./bin/publish updated x,whosonfirst-data-venue-us-ca,data/110/878/641/1/1108786411.geojson
updated 18:48:58.333577 [info] got task: {whosonfirst-data-venue-us-ca [data/110/878/641/1/1108786411.geojson]}
updated 18:48:58.333607 [info] invoking pull
updated 18:49:10.888423 [debug] Already up-to-date.
updated 18:49:10.888499 [info] time to process (pull) whosonfirst-data-venue-us-ca: 12.554848051s
updated 18:49:10.888523 [info] invoking s3
updated 18:49:10.888545 [info] invoking elasticsearch
updated 18:49:10.888792 [debug] /usr/local/bin/wof-es-index-filelist --host es.example.com --port 9200 --index spelunker /tmp/updated099706883
updated 18:49:10.899789 [debug] Schedule /usr/local/data/whosonfirst-data-venue-us-ca/data/110/878/641/1/1108786411.geojson for sync
updated 18:49:10.900059 [debug] Schedule /usr/local/data/whosonfirst-data-venue-us-ca/data/110/878/641/1/1108786411.geojson for processing
updated 18:49:10.900335 [debug] Looking for changes to /data/110/878/641/1/1108786411.geojson (prefix: )
updated 18:49:10.900494 [debug] HEAD s3://whosonfirst.mapzen.com//data/110/878/641/1/1108786411.geojson
updated 18:49:11.151795 [debug] Local hash is 48fa97d0b9924b0d03bd88de182a6623 remote hash is 48fa97d0b9924b0d03bd88de182a6623
updated 18:49:11.152005 [debug] /usr/local/data/whosonfirst-data-venue-us-ca/data/110/878/641/1/1108786411.geojson has not changed, skipping
updated 18:49:11.152146 [debug] Completed sync for /usr/local/data/whosonfirst-data-venue-us-ca/data/110/878/641/1/1108786411.geojson
updated 18:49:11.152397 [info] time to process (s3) whosonfirst-data-venue-us-ca: 262.075848ms
updated 18:49:12.175761 [info] time to process (elasticsearch) whosonfirst-data-venue-us-ca: 1.287165453s

*/

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
	"time"
)

func main() {

	var data_root = flag.String("data-root", "", "...")
	var es = flag.Bool("es", false, "")
	var es_host = flag.String("es-host", "localhost", "")
	var es_port = flag.String("es-port", "9200", "")
	var es_index = flag.String("es-index", "whosonfirst", "")
	var es_index_tool = flag.String("es-index-tool", "/usr/local/bin/wof-es-index-filelist", "")
	var logfile = flag.String("logfile", "", "Write logging information to this file")
	var loglevel = flag.String("loglevel", "info", "The amount of logging information to include, valid options are: debug, info, status, warning, error, fatal")
	var pull = flag.Bool("pull", false, "...")
	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")
	var s3 = flag.Bool("s3", false, "...")
	var s3_bucket = flag.String("s3-bucket", "whosonfirst.mapzen.com", "...")
	var s3_prefix = flag.String("s3-prefix", "", "...")
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

	processors := make([]process.Process, 0)

	/*

		the order in which processes get added to the `processors` is important because
		some need to be run before others and block and check return values while others
		are assumed to be able to be run asynchronously (20161222/thisisaaronland)

	*/

	if *pull {

		pr, err := process.NewPullProcess(*data_root, logger)

		if err != nil {
			golog.Fatal("Failed to instantiate Git hooks processor", err)
		}

		processors = append(processors, pr)
	}

	if *s3 {

		pr, err := process.NewS3Process(*data_root, *s3_bucket, *s3_prefix, logger)

		if err != nil {
			golog.Fatal("Failed to instantiate S3 hooks processor", err)
		}

		processors = append(processors, pr)
	}

	if *es {

		pr, err := process.NewElasticsearchProcess(*data_root, *es_index_tool, *es_host, *es_port, *es_index, logger)

		if err != nil {
			golog.Fatal("Failed to instantiate Elasticsearch hooks processor", err)
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

				// hash := row[0]
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
					Hash:	 "fix me",
					Repo:    repo,
					Commits: commits,
				}

				up_messages <- t
			}
		}
	}()

	logger.Info("ready to process tasks")

	for _, pr := range processors {

		go func(pr process.Process) {

			buffer := time.Second * 30

			for {

				timer := time.NewTimer(buffer)
				<-timer.C

				logger.Info("invoking flush for %s", pr.Name())
				pr.Flush()
			}
		}(pr)
	}

	for {

		task := <-up_messages
		logger.Info("got task: %s", task)

		for _, pr := range processors {

			name := pr.Name()

			logger.Info("invoking %s", name)

			if name == "pull" {

				err := pr.ProcessTask(task)

				if err != nil {

					logger.Error("Failed to complete %s process for task (%s) because: %s", name, task, err)
					break
				}

				continue
			}

			go pr.ProcessTask(task)
		}
	}

	logger.Info("stop")
}

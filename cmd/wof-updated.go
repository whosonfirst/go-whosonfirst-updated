package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	// "github.com/whosonfirst/go-slackcat-writer"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/process"
	"gopkg.in/redis.v1"
	"io"
	golog "log"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {

	var data_root = flag.String("data-root", "", "...")
	var es_host = flag.String("es-host", "localhost", "")
	var es_port = flag.String("es-port", "9200", "")
	var es_index = flag.String("es-index", "whosonfirst", "")
	var es_index_tool = flag.String("es-index-tool", "/usr/local/bin/wof-es-index-filelist", "")
	var logfile = flag.String("logfile", "", "Write logging information to this file")
	var loglevel = flag.String("loglevel", "info", "The amount of logging information to include, valid options are: debug, info, status, warning, error, fatal")
	var processors = flag.String("processors", "", "Valid options include: es,null,s3")
	var post_processors = flag.String("post-processors", "", "Valid options include: pubsub")
	var pre_processors = flag.String("pre-processors", "", "Valid options include: pull")
	var pubsub_host = flag.String("pubsub-host", "localhost", "PubSub host (for notifications)")
	var pubsub_port = flag.Int("pubsub-port", 6379, "PubSub port (for notifications)")
	var pubsub_channel = flag.String("pubsub-channel", "pubssed", "PubSub channel (for notifications)")
	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")
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

	/*

	if *slack {

	   	slack_logger, err := slackcat.NewWriter(*slackcat_conf)

		if err != nil {
			golog.Fatal(err)
		}

		logger.AddLogger(slack_logger, "status")
	}
	*/
	
	processors_pre := make([]process.Process, 0)
	processors_post := make([]process.Process, 0)
	processors_async := make([]process.Process, 0)

	/*

		the order in which processes get added to the `processors` is important because
		some need to be run before others and block and check return values while others
		are assumed to be able to be run asynchronously (20161222/thisisaaronland)

	*/

	for _, name := range strings.Split(*pre_processors, ",") {

		logger.Debug("Configure pre processor %s", name)

		if name == "pull" {
			pr, err := process.NewPullProcess(*data_root, logger)

			if err != nil {
				golog.Fatal("Failed to instantiate Git hooks processor", err)
			}

			processors_pre = append(processors_pre, pr)
		}
	}

	for _, name := range strings.Split(*processors, ",") {

		logger.Debug("Configure async processor %s", name)

		if name == "s3" {

			pr, err := process.NewS3Process(*data_root, *s3_bucket, *s3_prefix, logger)

			if err != nil {
				golog.Fatal("Failed to instantiate S3 hooks processor", err)
			}

			processors_async = append(processors_async, pr)
		}

		if name == "es" {

			pr, err := process.NewElasticsearchProcess(*data_root, *es_index_tool, *es_host, *es_port, *es_index, logger)

			if err != nil {
				golog.Fatal("Failed to instantiate Elasticsearch hooks processor", err)
			}

			processors_async = append(processors_async, pr)
		}

		if name == "null" {

			pr, err := process.NewNullProcess(*data_root, logger)

			if err != nil {
				golog.Fatal("Failed to instantiate null hooks processor", err)
			}

			processors_async = append(processors_async, pr)
		}

	}

	for _, name := range strings.Split(*post_processors, ",") {

		logger.Debug("Configure post processor %s", name)

		if name == "pubsub" {

			pr, err := process.NewPubSubProcess(*data_root, *pubsub_host, *pubsub_port, *pubsub_channel, logger)

			if err != nil {
				golog.Fatal("Failed to instantiate PubSub hooks processor", err)
			}

			processors_post = append(processors_post, pr)
		}
	}

	if len(processors_pre) == 0 && len(processors_async) == 0 && len(processors_post) == 0 {
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

		logger.Debug("Ready to receive (updated) PubSub messages")

		for {

			i, _ := pubsub_client.Receive()

			if msg, _ := i.(*redis.Message); msg != nil {
				ps_messages <- msg.Payload
			}
		}

	}()

	go func() {

		logger.Debug("Ready to process (updated) PubSub messages")

		for {

			// we are assuming this:
			// https://github.com/whosonfirst/go-webhookd/blob/master/transformations/github.commits.go

			msg := <-ps_messages

			rdr := csv.NewReader(strings.NewReader(msg))

			tasks := make(map[string]map[string][]string)

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

				hash := row[0]
				repo := row[1]
				path := row[2]

				_, ok := tasks[repo]

				if !ok {
					tasks[repo] = make(map[string][]string)
				}

				commits, ok := tasks[repo][hash]

				if !ok {
					commits = make([]string, 0)
				}

				commits = append(commits, path)
				tasks[repo][hash] = commits
			}

			for repo, details := range tasks {

				for hash, commits := range details {

					t := updated.UpdateTask{
						Hash:    hash,
						Repo:    repo,
						Commits: commits,
					}

					up_messages <- t
				}
			}
		}
	}()

	logger.Debug("Ready to process (updated) tasks")

	all_processors := [][]process.Process{
		processors_pre,
		processors_async,
		processors_post,
	}

	for _, pr_group := range all_processors {

		for _, pr := range pr_group {

			logger.Debug("Set up monitoring for %s", pr.Name())

			go func(pr process.Process) {

				buffer := time.Second * 60

				for {

					timer := time.NewTimer(buffer)
					<-timer.C

					logger.Debug("Invoking flush for %s", pr.Name())
					pr.Flush()
				}
			}(pr)
		}
	}

	for {

		task := <-up_messages
		logger.Info("Processing task: %s", task)

		ok := true

		for _, pr := range processors_pre {

			name := pr.Name()
			logger.Debug("Invoking pre-processor %s", name)

			err := pr.ProcessTask(task)

			if err != nil {
				logger.Error("Failed to complete %s process for task (%s) because: %s", name, task, err)
				ok = false
				break
			}

		}

		if !ok {
			continue
		}

		wg := new(sync.WaitGroup)

		for _, pr := range processors_async {

			name := pr.Name()
			logger.Debug("Invoking processor %s", name)

			wg.Add(1)

			go func(pr process.Process, wg *sync.WaitGroup) {
				defer wg.Done()
				pr.ProcessTask(task)
			}(pr, wg)

		}

		// This does not account for things that might still be in a
		// pending queue waiting to be processed, usually because some	
		// other earlier process hasn't finished (20161227/thisisaaronland)

		wg.Wait()

		for _, pr := range processors_post {

			name := pr.Name()
			logger.Debug("Invoking post-processor %s", name)

			pr.ProcessTask(task)
		}

	}

	os.Exit(0)
}

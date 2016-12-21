package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/process"
	"gopkg.in/redis.v1"
	"io"
	"log"
	"strings"
)

func main() {

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")
	var githooks = flag.Bool("githooks", false, "...")
	var githooks_root = flag.String("githooks-data-root", "", "...")

	flag.Parse()

	processors := make([]process.Processor, 0)

	if *githooks {

		gh, err := process.NewGitHooksProcessor(*githooks_root)

		if err != nil {
			log.Fatal("Failed to instantiate Git hooks processor", err)
		}

		processors = append(processors, gh)
	}

	if len(processors) == 0 {
		log.Fatal("You forgot to specify any processors, silly")
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
			log.Fatal(err)
		}

		log.Println("ready to receive pubsub messages")

		for {

			i, _ := pubsub_client.Receive()

			if msg, _ := i.(*redis.Message); msg != nil {
				// log.Println("message", msg)
				ps_messages <- msg.Payload
			}
		}

	}()

	go func() {

		log.Println("ready to process pubsub messages")

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
					log.Println(err)
					break
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

	log.Println("ready to process tasks")

	for {

		task := <-up_messages
		log.Println("got task", task)

		for _, p := range processors {

			go p.Process(task)
		}
	}

	log.Println("stop")
}

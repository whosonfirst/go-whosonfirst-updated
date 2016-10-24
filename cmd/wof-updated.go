package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/go-github/github"
	"gopkg.in/redis.v1"
	"log"
	"sync"
	"time"
)

var pending map[string][]string
var processing map[string]chan bool

var mu *sync.Mutex

type UpdateTask struct {
	Repo       string
	CommitHash string
	Commits    []string
}

func init() {

	pending = make(map[string][]string)
	processing = make(map[string]chan bool)

	mu = new(sync.Mutex)
}

func Process(repo string, files []string) error {

	defer func() {
		mu.Lock()

		log.Println("All done")
		processing[repo] <- true
		delete(processing, repo)
		mu.Unlock()
	}()

	// cd repo
	// post-merge

	log.Println("process", repo, files)

	timer := time.NewTimer(time.Second * 90)
	<-timer.C

	log.Println("processed", repo)

	return nil
}

func main() {

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")

	flag.Parse()

	buffer := time.Second * 30

	ps_messages := make(chan string)
	up_messages := make(chan UpdateTask)

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

			msg := <-ps_messages
			log.Println("got message", msg)

			var event github.PushEvent

			err := json.Unmarshal([]byte(msg), &event)

			if err != nil {
				log.Println(err)
				return
			}

			repo := event.Repo
			repo_name := *repo.Name

			hash := *event.HeadCommit.ID

			commits := make([]string, 0)

			for _, c := range event.Commits {

				for _, path := range c.Added {
					commits = append(commits, path)
				}

				for _, path := range c.Modified {
					commits = append(commits, path)
				}

				for _, path := range c.Removed {
					commits = append(commits, path)
				}
			}

			task := UpdateTask{
				Repo:       repo_name,
				CommitHash: hash,
				Commits:    commits,
			}

			up_messages <- task
		}
	}()

	log.Println("ready to process tasks")

	for {

		task := <-up_messages
		log.Println("got task", task)

		repo := task.Repo

		mu.Lock()

		files, ok := pending[repo]

		if !ok {
			files = make([]string, 0)
		}

		for _, path := range task.Commits {
			files = append(files, path)
		}

		pending[repo] = files

		mu.Unlock()

		if ok {
			continue
		}

		log.Printf("buffer %s for %v\n", repo, buffer)

		go func(r string) {

			timer := time.NewTimer(buffer)
			<-timer.C

			ch, ok := processing[r]

			if ok {

				log.Printf("%s is unbuffered but another instance is processing, waiting\n", r)
				<-ch

				log.Printf("%s finished processing, doing it again...\n", r)
			}

			mu.Lock()
			f := pending[r]
			delete(pending, r)

			processing[r] = make(chan bool)

			mu.Unlock()

			Process(r, f)

		}(repo)

	}

	log.Println("stop")
}

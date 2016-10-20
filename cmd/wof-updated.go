package main

import (
	"flag"
	"fmt"
	"gopkg.in/redis.v1"
	"log"
	"sync"
	"time"
)

var pending map[string]string
var processing map[string]bool

var mu *sync.Mutex

type UpdateTask struct {
	Repo       string
	CommitHash string
}

func init() {

	pending = make(map[string]string)
	processing = make(map[string]bool)

	mu = new(sync.Mutex)
}

func Process(repo string, hash string) error {

	go func() {
		mu.Lock()
		delete(processing, repo)
		mu.Unlock()
	}()

	// cd repo
	// post-merge

	timer := time.NewTimer(time.Second * 10)
	<-timer.C

	log.Println("processed")

	return nil
}

func Schedule(repo string) error {

	timer := time.NewTimer(time.Minute * 1)
	<-timer.C

	mu.Lock()

	hash := pending[repo]
	delete(pending, repo)

	processing[repo] = true

	mu.Unlock()

	go Process(repo, hash)

	return nil
}

func main() {

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")

	flag.Parse()

	ps_messages := make(chan string)
	up_messages := make(chan UpdateTask)

	go func() {

		redis_endpoint := fmt.Sprintf("%s:%d", redis_host, redis_port)

		redis_client := redis.NewTCPClient(&redis.Options{
			Addr: redis_endpoint,
		})

		defer redis_client.Close()

		pubsub_client := redis_client.PubSub()
		defer pubsub_client.Close()

		pubsub_client.Subscribe(*redis_channel)

		for {

			i, _ := pubsub_client.Receive()

			if msg, _ := i.(*redis.Message); msg != nil {
				ps_messages <- msg.Payload
			}
		}
	}()

	go func() {

		for {

			msg := <-ps_messages
			log.Println(msg)

			task := UpdateTask{
				Repo:       "debug",
				CommitHash: "xxx",
			}

			up_messages <- task
		}
	}()

	go func() {

		for {

			task := <-up_messages

			repo := task.Repo

			_, waiting := pending[repo]

			if waiting {
				continue
			}

			pending[repo] = task.CommitHash
		}

	}()

	for {

		for repo, _ := range pending {

			_, working := processing[repo]

			if working {
				continue
			}

			go Schedule(repo)
		}
	}
}

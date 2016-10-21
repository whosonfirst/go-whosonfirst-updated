package main

import (
	"flag"
	"fmt"
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
}

func init() {

	pending = make(map[string][]string)
	processing = make(map[string]chan bool)

	mu = new(sync.Mutex)
}

func Process(repo string, files []string) error {

	processing[repo] = make(chan bool)

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

	ps_messages := make(chan string)
	up_messages := make(chan UpdateTask)

	go func() {

		redis_endpoint := fmt.Sprintf("%s:%d", *redis_host, *redis_port)

		log.Println(redis_endpoint)

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

		for {

			i, _ := pubsub_client.Receive()

			if msg, _ := i.(*redis.Message); msg != nil {
				log.Println("message", msg)
				ps_messages <- msg.Payload
			}
		}

	}()

	go func() {

		log.Println("handle messages")
		
		for {

			msg := <-ps_messages
			log.Println("got message", msg)

			task := UpdateTask{
				Repo:       "debug",
				CommitHash: msg,
			}

			up_messages <- task
		}
	}()

	log.Println("handle tasks")

	for {

		task := <-up_messages
		log.Println("got task", task)

		repo := task.Repo

		mu.Lock()

		files, ok := pending[repo]

		if !ok {
			files = make([]string, 0)
		}

		files = append(files, task.CommitHash)
		pending[repo] = files

		mu.Unlock()

		if ok {
			continue
		}

		go func(r string) {

			log.Println("schedule", r)

			timer := time.NewTimer(time.Second * 30)
			<-timer.C

			ch, ok := processing[r]

			if ok {

				log.Println(r, "is processing", "waiting")
				<-ch
			}

			mu.Lock()
			f := pending[r]
			delete(pending, r)
			mu.Unlock()

			Process(r, f)

		}(repo)

	}

	log.Println("stop")
}

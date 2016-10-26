package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	_ "github.com/whosonfirst/go-whosonfirst-s3"
	_ "github.com/whosonfirst/go-whosonfirst-tile38/index"
	"gopkg.in/redis.v1"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var pending map[string][]string
var processing map[string]chan bool

var mu *sync.Mutex

type UpdateTask struct {
	Repo    string
	Commits []string
}

func init() {

	pending = make(map[string][]string)
	processing = make(map[string]chan bool)

	mu = new(sync.Mutex)
}

func Pause(seconds int) {

	d := time.Duration(rand.Int31n(int32(seconds * 1000)))
	log.Println("pause for %d ms", d)

	timer := time.NewTimer(time.Millisecond * d)
	<-timer.C

}

func Process(repo string, files []string) error {

	defer func() {
		mu.Lock()

		log.Println("All done")
		processing[repo] <- true
		delete(processing, repo)
		mu.Unlock()
	}()

	log.Println("process", repo, files)
	t1 := time.Now()

	// first create a file of all the things to process

	tmpfile, err := ioutil.TempFile("", "updated")

	if err != nil {
		return err
	}

	for _, relpath := range files {

		// TO DO: add abs path ?

		path := filepath.Join(repo, relpath)
		_, err = tmpfile.Write([]byte(path + "\n"))

		if err != nil {
			return err
		}

		log.Println("schedule", path)
	}

	err = tmpfile.Close()

	if err != nil {
		return err
	}

	defer func() {
		path := tmpfile.Name()
		log.Println("remove", path)
		os.Remove(path)
	}()

	// cd repo (maybe?)

	// update metafiles - this still needs to block for now...

	log.Println("update metafiles")

	Pause(5)

	wg := new(sync.WaitGroup)

	// sync to ES

	wg.Add(1)

	go func() {

		defer wg.Done()

		log.Println("sync ES")

		Pause(5)
	}()

	// sync to S3

	wg.Add(1)

	go func() {

		defer wg.Done()

		log.Println("sync S3")

		Pause(5)

		// s := s3.WOFSync(auth, *bucket, *prefix, *procs, *debug, logger)
		// s.SyncFileList(tmpfile, root)
	}()

	// sync to Tile38

	wg.Add(1)

	go func() {

		defer wg.Done()

		log.Println("sync T38")

		Pause(5)

		// client, err := tile38.NewTile38Client(*tile38_host, *tile38_port)
		// client.IndexFileList(tmpfile, *collection)

	}()

	// generate bundle(s) or not - it's more likely these would happen through a
	// cron job (20161025/thisisaaronland)

	wg.Wait()

	t2 := time.Since(t1)

	log.Println("processed", repo)
	log.Println(t2)

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

				t := UpdateTask{
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

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-csv"
	"gopkg.in/redis.v1"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	_ "time"
)

func main() {

	var dryrun = flag.Bool("dryrun", false, "Just show which files would be updated but don't actually do anything.")
	var verbose = flag.Bool("verbose", false, "Enable verbose logging.")

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")

	var repo = flag.String("repo", "", "The path to a valid Who's On First repo to run updates from")
	var start_commit = flag.String("start-commit", "", "A valid Git commit hash to start updates from. If empty then the current hash will be used.")
	var stop_commit = flag.String("stop-commit", "", "A valid Git commit hash to limit updates to.")

	flag.Parse()

	_, err := os.Stat(*repo)

	if os.IsNotExist(err) {
		log.Fatal("Repo does not exist", *repo)
	}

	cwd, err := os.Getwd()

	if err != nil {
		log.Fatal(err)
	}

	// I wish this wasn't necessary. I wish I could make sense of the
	// libgit2 documentation... (20161222/thisisaaronland)

	// See also: https://github.com/whosonfirst/go-whosonfirst-updated/issues/1

	err = os.Chdir(*repo)

	if err != nil {
		log.Fatal(err)
	}

	// https://git-scm.com/docs/git-diff

	if *start_commit == "" {

		git_args := []string{
			"log", "--pretty=format:%H", "-n", "1",
		}

		log.Println(strings.Join(git_args, " "))

		cmd := exec.Command("git", git_args...)
		hash, err := cmd.Output()

		if err != nil {
			log.Fatal("Can not determined start hash for %s", *repo)
		}

		log.Printf("Current hash %s\n", hash)
		*start_commit = string(hash)
	}

	git_args := []string{
		"show", "--pretty=format:#%H", "--name-only",
	}

	var commit_range string

	if *stop_commit == "" {
		commit_range = *start_commit
	} else {
		commit_range = fmt.Sprintf("%s^...%s", *start_commit, *stop_commit)
	}

	git_args = append(git_args, commit_range)

	log.Println(strings.Join(git_args, " "))

	cmd := exec.Command("git", git_args...)
	out, err := cmd.Output()

	os.Chdir(cwd)

	if err != nil {
		log.Fatal(err)
	}

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)

	// please add support for multiwriters here to send
	// verbose output to STDOUT

	fieldnames := []string{"hash", "repo", "path"}
	writer, err := csv.NewDictWriter(buf, fieldnames)

	var hash string

	rows := 0

	for _, ln := range strings.Split(string(out), "\n") {

		if strings.HasPrefix(ln, "#") {
			hash = strings.Replace(ln, "#", "", 1)
		}

		if strings.HasSuffix(ln, ".geojson") {

			row := make(map[string]string)
			row["hash"] = hash
			row["repo"] = filepath.Base(*repo)
			row["path"] = ln

			writer.WriteRow(row)
			rows += 1
		}
	}

	buf.Flush()

	// see above inre multiwriters...

	if *verbose {
		log.Println(b.String())
	}

	log.Printf("sending %d rows\n", rows)

	if !*dryrun {

		redis_endpoint := fmt.Sprintf("%s:%d", *redis_host, *redis_port)

		redis_client := redis.NewTCPClient(&redis.Options{
			Addr: redis_endpoint,
		})

		defer redis_client.Close()

		rsp := redis_client.Publish(*redis_channel, b.String())
		err := rsp.Err()

		/*

			For example...

			./bin/wof-updated-replay -repo /usr/local/data/whosonfirst-data -start-commit 2569568cd91df9a682c01793930009a9e2850e90
			2017/08/03 15:04:45 log --pretty=format:#%H --name-only 2569568cd91df9a682c01793930009a9e2850e90
			2017/08/03 15:05:32 sending 6609492 rows
			2017/08/03 15:05:33 write tcp 127.0.0.1:49847->127.0.0.1:6379: write: connection reset by peer

			I suppose there is a config flag somewhere in the Redis/PubSub stack to enable MASSIVE messages?
			I haven't found it yet if it exists (20170803/thisisaaronland)

		*/

		if err != nil {
			log.Fatal(err)
		}
	}

	os.Exit(0)
}

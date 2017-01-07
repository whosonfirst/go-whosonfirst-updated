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

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")

	var repo = flag.String("repo", "", "The path to a valid Who's On First repo to run updates from")
	var start_commit = flag.String("start-commit", "", "A valid Git commit hash to start updates from. If empty then the current hash will be used.")
	var stop_commit = flag.String("stop-commit", "HEAD", "A valid Git commit hash to limit updates to.")

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
		"log", "--pretty=format:#%H", "--name-only",
	}

	commit_range := fmt.Sprintf("%s^...%s", *start_commit, *stop_commit)
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

	fieldnames := []string{"hash", "repo", "path"}
	writer, err := csv.NewDictWriter(buf, fieldnames)

	var hash string

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
		}
	}

	buf.Flush()

	log.Println(b.String())

	if !*dryrun {
		redis_endpoint := fmt.Sprintf("%s:%d", *redis_host, *redis_port)

		redis_client := redis.NewTCPClient(&redis.Options{
			Addr: redis_endpoint,
		})

		defer redis_client.Close()

		redis_client.Publish(*redis_channel, b.String())
	}

	// log.Printf("%s", files)
}

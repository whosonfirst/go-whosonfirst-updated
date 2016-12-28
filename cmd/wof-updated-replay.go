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

	var dryrun = flag.Bool("dryrun", false, "...")

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")

	var repo = flag.String("repo", "", "...")
	var start_commit = flag.String("start-commit", "", "...")
	var stop_commit = flag.String("stop-commit", "", "...")

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

	git_args := []string{
		// "diff", "--pretty=format:", "--name-only",
		"log", "--pretty=format:#%H", "--name-only",
	}

	if *start_commit != "" {

		stop := "HEAD"

		if *stop_commit != "" {
			stop = *stop_commit
		}

		commit_range := fmt.Sprintf("%s^...%s", *start_commit, stop)
		git_args = append(git_args, commit_range)
	}

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

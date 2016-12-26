package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	_ "path/filepath"
	"gopkg.in/redis.v1"
	"strings"
	_ "time"
)

func main() {

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

	err = os.Chdir(*repo)

	if err != nil {
		log.Fatal(err)
	}

	// https://git-scm.com/docs/git-diff

	/*

	git log --pretty=format:%H --no-merges --name-only 613b6e7cf63ae58231a596ffa1b2e80e9f2b9038^..master
	044ca5543338d1e3d1788a3d522f42b9cea08517
	data/110/878/641/1/1108786411.geojson
	data/110/878/641/3/1108786413.geojson

	e0653652b33a8f1b473c05f8815131b404b7ffde
	data/588/389/817/588389817.geojson
	data/588/390/107/588390107.geojson

	*/

	git_args := []string{
		"diff", "--pretty=format:", "--name-only",
	}

	if *start_commit != "" {
		git_args = append(git_args, *start_commit)
	}

	if *stop_commit != "" {
		git_args = append(git_args, *stop_commit)
	}

	log.Println(strings.Join(git_args, " "))

	cmd := exec.Command("git", git_args...)
	out, err := cmd.Output()

	os.Chdir(cwd)

	if err != nil {
		log.Fatal(err)
	}

	files := make([]string, 0)

	for _, path := range strings.Split(string(out), "\n") {

		if strings.HasSuffix(path, ".geojson") {
			files = append(files, path)
		}
	}

	redis_endpoint := fmt.Sprintf("%s:%d", *redis_host, *redis_port)

	redis_client := redis.NewTCPClient(&redis.Options{
		Addr: redis_endpoint,
	})

	defer redis_client.Close()

	redis_client.Publish(*redis_channel, "foo")

	log.Printf("%s", files)
}

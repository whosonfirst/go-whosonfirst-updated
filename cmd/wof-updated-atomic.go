package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-csv"
	"github.com/whosonfirst/go-whosonfirst-repo"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"gopkg.in/redis.v1"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {

	var dryrun = flag.Bool("dryrun", false, "Just show which files would be updated but don't actually do anything.")
	var verbose = flag.Bool("verbose", false, "Enable verbose logging.")

	var redis_host = flag.String("redis-host", "localhost", "Redis host")
	var redis_port = flag.Int("redis-port", 6379, "Redis port")
	var redis_channel = flag.String("redis-channel", "updated", "Redis channel")

	flag.Parse()

	var b bytes.Buffer
	buf := bufio.NewWriter(&b)

	// please add support for multiwriters here to send
	// verbose output to STDOUT

	fieldnames := []string{"hash", "repo", "path"}
	writer, err := csv.NewDictWriter(buf, fieldnames)

	if err != nil {
		log.Fatal(err)
	}

	for _, a := range flag.Args() {

		parts := strings.Split(a, "#")

		if len(parts) != 2 {
			log.Fatal("Invalid arg", a)
		}

		repo_name := parts[0]
		path := parts[1]

		_, err := repo.NewDataRepoFromString(repo_name)

		if err != nil {
			log.Fatal("Invalid repo", repo_name, err)
		}

		wofid, err := strconv.ParseInt(path, 10, 64)

		if err == nil {

			rel_path, err := uri.Id2RelPath(wofid)

			if err != nil {
				log.Fatal("Invalid WOF ID", wofid, err)
			}

			path = filepath.Join("data", rel_path)
		}

		// check WOF ID against repo because a) general validation and
		// b) https://github.com/whosonfirst/go-whosonfirst-updated/issues/17

		row := make(map[string]string)
		row["hash"] = "atomic-update"
		row["repo"] = repo_name
		row["path"] = path

		writer.WriteRow(row)
	}

	buf.Flush()

	if *verbose {
		log.Println(b.String())
	}

	if !*dryrun {

		redis_endpoint := fmt.Sprintf("%s:%d", *redis_host, *redis_port)

		redis_client := redis.NewTCPClient(&redis.Options{
			Addr: redis_endpoint,
		})

		defer redis_client.Close()

		rsp := redis_client.Publish(*redis_channel, b.String())
		err := rsp.Err()

		if err != nil {
			log.Fatal(err)
		}
	}

	os.Exit(0)
}

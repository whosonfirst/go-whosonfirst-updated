package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	_ "path/filepath"
	"strings"
	_ "time"
)

func main() {

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

	log.Printf("%s", files)
}

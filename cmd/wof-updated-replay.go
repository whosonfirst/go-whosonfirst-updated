package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {

	var repo = flag.String("repo", "", "...")
	var start_commit = flag.String("start-commit", "", "...")
	var stop_commit = flag.String("stop-commit", "", "...")

	flag.Parse()

	dot_git := filepath.Join(*repo, ".git")

	git_dir := fmt.Sprintf("--git-dir=%s", dot_git)
	work_tree := fmt.Sprintf("--work-tree=%s", dot_git)

	// pretty sure the git/work dir stuff is incorrect in this context...
	// would that I could understand the libgit2 documentation...
	// (20161222/thisisaaronland)

	git_args := []string{
		git_dir, work_tree,
		"diff", "--pretty='format:'", "--name-only"
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

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s", out)
}

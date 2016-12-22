package process

import (
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type GitHooksProcessor struct {
	Processor
	queue     *queue.Queue
	data_root string
	flushing  bool
	mu        *sync.Mutex
}

func NewGitHooksProcessor(data_root string) (*GitHooksProcessor, error) {

	data_root, err := filepath.Abs(data_root)

	if err != nil {
		return nil, err
	}

	_, err = os.Stat(data_root)

	if os.IsNotExist(err) {
		return nil, err
	}

	q, err := queue.NewQueue()

	if err != nil {
		return nil, err
	}

	mu := new(sync.Mutex)

	gh := GitHooksProcessor{
		queue:     q,
		data_root: data_root,
		flushing:  false,
		mu:        mu,
	}

	go gh.Monitor()

	return &gh, nil
}

func (gh *GitHooksProcessor) Monitor() {

	buffer := time.Second * 30

	for {

		timer := time.NewTimer(buffer)
		<-timer.C

		gh.Flush()
	}

}

func (gh *GitHooksProcessor) Flush() {

	gh.mu.Lock()

	if gh.flushing {
		gh.mu.Unlock()
		return
	}

	gh.flushing = true
	gh.mu.Unlock()

	for _, repo := range gh.queue.Pending() {
		go gh.ProcessRepo(repo)
	}

	gh.mu.Lock()

	gh.flushing = false
	gh.mu.Unlock()
}

func (gh *GitHooksProcessor) Process(task updated.UpdateTask) error {

	repo := task.Repo
	return gh.ProcessRepo(repo)
}

func (gh *GitHooksProcessor) ProcessRepo(repo string) error {

	if gh.queue.IsProcessing(repo) {
		return gh.queue.Schedule(repo)
	}

	err := gh.queue.Lock(repo)

	if err != nil {
		return err
	}

	err = gh._process(repo)

	if err != nil {
		return err
	}

	err = gh.queue.Release(repo)

	if err != nil {
		return err
	}

	return nil
}

func (gh *GitHooksProcessor) _process(repo string) error {

	t1 := time.Now()

	defer func() {

		t2 := time.Since(t1)
		log.Printf("time to process %s: %v\n", repo, t2)
	}()

	abs_path := filepath.Join(gh.data_root, repo)
	log.Println("process", abs_path)

	_, err := os.Stat(abs_path)

	if os.IsNotExist(err) {
		log.Println("Can't find repo", abs_path)
		return err
	}

	dot_git := filepath.Join(abs_path, ".git")

	git_dir := fmt.Sprintf("--git-dir=%s", dot_git)
	work_tree := fmt.Sprintf("--work-tree=%s", dot_git)

	git_args := []string{git_dir, work_tree, "pull", "origin", "master"}

	cmd := exec.Command("git", git_args...)

	out, err := cmd.Output()

	if err != nil {
		log.Println("failed to pull from master", err)
		return err
	}

	log.Println(out)
	return nil
}

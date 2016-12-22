package process

import (
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	_ "log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type GitProcessor struct {
	Processor
	queue     *queue.Queue
	data_root string
	flushing  bool
	mu        *sync.Mutex
	logger    *log.WOFLogger
}

func NewGitProcessor(data_root string, logger *log.WOFLogger) (*GitProcessor, error) {

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

	pr := GitProcessor{
		queue:     q,
		data_root: data_root,
		flushing:  false,
		mu:        mu,
		logger:    logger,
	}

	go pr.Monitor()

	return &pr, nil
}

func (pr *GitProcessor) Monitor() {

	buffer := time.Second * 30

	for {

		timer := time.NewTimer(buffer)
		<-timer.C

		pr.Flush()
	}

}

func (pr *GitProcessor) Flush() {

	pr.mu.Lock()

	if pr.flushing {
		pr.mu.Unlock()
		return
	}

	pr.flushing = true
	pr.mu.Unlock()

	for _, repo := range pr.queue.Pending() {
		go pr.ProcessRepo(repo)
	}

	pr.mu.Lock()

	pr.flushing = false
	pr.mu.Unlock()
}

func (pr *GitProcessor) Process(task updated.UpdateTask) error {

	repo := task.Repo
	return pr.ProcessRepo(repo)
}

func (pr *GitProcessor) ProcessRepo(repo string) error {

	if pr.queue.IsProcessing(repo) {
		return pr.queue.Schedule(repo)
	}

	err := pr.queue.Lock(repo)

	if err != nil {
		return err
	}

	err = pr._process(repo)

	if err != nil {
		return err
	}

	err = pr.queue.Release(repo)

	if err != nil {
		return err
	}

	return nil
}

func (pr *GitProcessor) _process(repo string) error {

	t1 := time.Now()

	defer func() {

		t2 := time.Since(t1)
		pr.logger.Info("time to process %s: %v\n", repo, t2)
	}()

	abs_path := filepath.Join(pr.data_root, repo)

	_, err := os.Stat(abs_path)

	if os.IsNotExist(err) {
		pr.logger.Error("Can't find repo %s", abs_path)
		return err
	}

	dot_git := filepath.Join(abs_path, ".git")

	git_dir := fmt.Sprintf("--git-dir=%s", dot_git)
	work_tree := fmt.Sprintf("--work-tree=%s", dot_git)

	git_args := []string{git_dir, work_tree, "pull", "origin", "master"}

	cmd := exec.Command("git", git_args...)

	out, err := cmd.Output()

	if err != nil {
		pr.logger.Error("failed to pull from master %s", err)
		return err
	}

	pr.logger.Debug("%s\n", out)
	return nil
}

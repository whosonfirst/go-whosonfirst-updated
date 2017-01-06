package process

import (
	_ "fmt"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	_ "log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type PullProcess struct {
	Process
	queue     *queue.Queue
	data_root string
	flushing  bool
	mu        *sync.Mutex
	logger    *log.WOFLogger
}

func NewPullProcess(data_root string, logger *log.WOFLogger) (*PullProcess, error) {

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

	pr := PullProcess{
		queue:     q,
		data_root: data_root,
		flushing:  false,
		mu:        mu,
		logger:    logger,
	}

	return &pr, nil
}

func (pr *PullProcess) Name() string {
	return "pull"
}

func (pr *PullProcess) Flush() error {

	pr.mu.Lock()

	if pr.flushing {
		pr.mu.Unlock()
		return nil
	}

	pr.flushing = true
	pr.mu.Unlock()

	for _, repo := range pr.queue.Pending() {
		go pr.ProcessRepo(repo)
	}

	pr.mu.Lock()

	pr.flushing = false
	pr.mu.Unlock()

	return nil
}

func (pr *PullProcess) ProcessTask(task updated.UpdateTask) error {

	repo := task.Repo
	return pr.ProcessRepo(repo)
}

func (pr *PullProcess) ProcessRepo(repo string) error {

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

func (pr *PullProcess) _process(repo string) error {

	t1 := time.Now()

	defer func() {

		t2 := time.Since(t1)
		pr.logger.Info("Time to process (%s) %s: %v", pr.Name(), repo, t2)
	}()

	cwd, err := os.Getwd()

	if err != nil {
		pr.logger.Error("Can't get cwd, because %s", err)
		return err
	}

	abs_path := filepath.Join(pr.data_root, repo)

	_, err = os.Stat(abs_path)

	if os.IsNotExist(err) {
		pr.logger.Error("Can't find repo %s", abs_path)
		return err
	}

	err = os.Chdir(abs_path)

	if err != nil {
		pr.logger.Error("Can't chdir to %s, because %s", abs_path, err)
		return err
	}

	defer os.Chdir(cwd) // make sure we go back to where we came from

	//

	git_args := make([]string, 0)
	var cmd *exec.Cmd

	git_args = []string{"log", "--pretty=format:%H", "-n", "1"}
	cmd = exec.Command("git", git_args...)

	pr.logger.Debug("git %s", strings.Join(git_args, " "))

	hash, err := cmd.Output()

	if err != nil {
		pr.logger.Error("Failed to determine current hash: %s (git %s)", err, strings.Join(git_args, " "))
		return err
	}

	pr.logger.Debug("current git hash is %s", hash)

	//

	git_args = []string{"reset", "--hard", string(hash)}
	cmd = exec.Command("git", git_args...)

	pr.logger.Debug("git %s", strings.Join(git_args, " "))

	_, err = cmd.Output()

	if err != nil {
		pr.logger.Error("Failed to reset: %s (git %s)", err, strings.Join(git_args, " "))
		return err
	}

	//

	git_args = []string{"fetch", "origin", "master"}
	cmd = exec.Command("git", git_args...)

	pr.logger.Debug("git %s", strings.Join(git_args, " "))

	_, err = cmd.Output()

	if err != nil {
		pr.logger.Error("Failed to fetch: %s (git %s)", err, strings.Join(git_args, " "))
		return err
	}

	//

	git_args = []string{"merge", "origin", "master"}
	cmd = exec.Command("git", git_args...)

	cmd = exec.Command("git", git_args...)
	_, err = cmd.Output()

	if err != nil {
		pr.logger.Error("Failed to merge from origin/master: %s (git %s)", err, strings.Join(git_args, " "))
		return err
	}

	return nil
}

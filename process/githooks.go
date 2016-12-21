package process

import (
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"log"
	"os"
	"path/filepath"
)

type GitHooksProcessor struct {
	Processor
	queue     *queue.Queue
	data_root string
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

	p := GitHooksProcessor{
		queue:     q,
		data_root: data_root,
	}

	return &p, nil
}

func (gh *GitHooksProcessor) Process(task updated.UpdateTask) error {

	repo := task.Repo

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

	abs_path := filepath.Join(gh.data_root, repo)
	log.Println("process", abs_path)

	return nil
}

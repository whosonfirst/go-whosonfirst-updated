package process

import (
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
)

type GitHooksProcessor struct {
	Processor
	queue *queue.Queue
}

func NewGitHooksProcessor() (*GitHooksProcessor, error) {

	q, err := queue.NewQueue()

	if err != nil {
		return nil, err
	}

	p := GitHooksProcessor{
		queue: q,
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

	return nil
}

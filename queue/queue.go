package queue

import (
	"errors"
	"sync"
)

type Queue struct {
	pending    map[string]int
	processing map[string]int
	mu         *sync.Mutex
}

func NewQueue() (*Queue, error) {

	pending := make(map[string]int)
	processing := make(map[string]int)

	mu := new(sync.Mutex)

	queue := Queue{
		pending:    pending,
		processing: processing,
		mu:         mu,
	}

	return &queue, nil
}

func (q *Queue) Schedule(repo string) error {

	count, ok := q.pending[repo]

	if ok {
		count += 1
	} else {
		count = 1
	}

	q.mu.Lock()
	q.pending[repo] = count

	q.mu.Unlock()

	return nil
}

func (q *Queue) Lock(repo string) error {

	q.mu.Lock()
	_, ok := q.processing[repo]
	q.mu.Unlock()

	if ok {
		return errors.New("repo is already being processed")
	}

	q.mu.Lock()
	q.processing[repo] = 1
	q.mu.Unlock()

	return nil
}

func (q *Queue) Release(repo string) error {

	q.mu.Lock()
	_, ok := q.processing[repo]
	q.mu.Unlock()

	if !ok {
		return errors.New("repo has already been released")
	}

	q.mu.Lock()

	delete(q.processing, repo)
	q.mu.Unlock()

	return nil
}

func (q *Queue) IsProcessing(repo string) bool {

	q.mu.Lock()
	_, ok := q.processing[repo]
	q.mu.Unlock()

	return ok
}

func (q *Queue) Pending() []string {

	pending := make([]string, 0)

	for repo, _ := range q.pending {

		if q.IsProcessing(repo) {
			continue
		}

		pending = append(pending, repo)
	}

	return pending
}

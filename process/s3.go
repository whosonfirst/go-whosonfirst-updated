package process

import (
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"github.com/whosonfirst/go-whosonfirst-s3"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type S3Processor struct {
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

	pr := S3Processor{
		queue:     q,
		data_root: data_root,
		flushing:  false,
		mu:        mu,
	}

	// go pr.Monitor()

	return &pr, nil
}

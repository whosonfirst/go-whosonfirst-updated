package process

import (
	_ "fmt"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-s3"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"io/ioutil"
	"os"
	_ "os/exec"
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
	files     map[string][]string
	s3_bucket string
	s3_prefix string
	logger    *log.WOFLogger
}

func NewS3Processor(data_root string, s3_bucket string, s3_prefix string, logger *log.WOFLogger) (*S3Processor, error) {

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

	files := make(map[string][]string)

	mu := new(sync.Mutex)

	pr := S3Processor{
		queue:     q,
		data_root: data_root,
		flushing:  false,
		mu:        mu,
		files:     files,
		s3_bucket: s3_bucket,
		s3_prefix: s3_prefix,
		logger:    logger,
	}

	// go pr.Monitor()

	return &pr, nil
}

func (pr *S3Processor) Process(task updated.UpdateTask) error {

	repo := task.Repo

	pr.mu.Lock()

	files, ok := pr.files[repo]

	if !ok {
		files = make([]string, 0)
	}

	for _, path := range task.Commits {
		files = append(files, path)
	}

	pr.files[repo] = files
	pr.mu.Unlock()

	return pr.ProcessRepo(repo)
}

func (pr *S3Processor) ProcessRepo(repo string) error {

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

func (pr *S3Processor) _process(repo string) error {

	t1 := time.Now()

	defer func() {
		t2 := time.Since(t1)
		pr.logger.Info("time to process %s: %v\n", repo, t2)
	}()

	root := filepath.Join(pr.data_root, repo)

	_, err := os.Stat(root)

	if os.IsNotExist(err) {
		pr.logger.Error("Can't find repo", root)
		return err
	}

	tmpfile, err := ioutil.TempFile("", "updated")

	if err != nil {
		pr.logger.Error("Failed to create tmp file", err)
		return err
	}

	defer func() {
		os.Remove(tmpfile.Name())
	}()

	pr.mu.Lock()
	files := pr.files[repo]

	delete(pr.files, repo)
	pr.mu.Unlock()

	seen := make(map[string]bool)

	for _, path := range files {

		_, ok := seen[path]

		if ok {
			continue
		}

		tmpfile.Write([]byte(path + "\n"))
		seen[path] = true
	}

	pr.logger.Info(tmpfile.Name())

	debug := false
	procs := 10

	sink := s3.WOFSync(pr.s3_bucket, pr.s3_prefix, procs, debug, pr.logger)

	pr.logger.Info("%v", sink)

	// sink.SyncFileList(tmpfile.Name(), root)

	return nil
}

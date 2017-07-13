package process

import (
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-s3"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"github.com/whosonfirst/go-whosonfirst-updated/utils"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"os"
	_ "os/exec"
	"path/filepath"
	"sync"
	"time"
)

type S3Process struct {
	Process
	queue     *queue.Queue
	data_root string
	flushing  bool
	mu        *sync.Mutex
	files     map[string][]string
	s3_bucket string
	s3_prefix string
	logger    *log.WOFLogger
}

func NewS3Process(data_root string, s3_bucket string, s3_prefix string, logger *log.WOFLogger) (*S3Process, error) {

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

	pr := S3Process{
		queue:     q,
		data_root: data_root,
		flushing:  false,
		mu:        mu,
		files:     files,
		s3_bucket: s3_bucket,
		s3_prefix: s3_prefix,
		logger:    logger,
	}

	return &pr, nil
}

func (pr *S3Process) Flush() error {

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

func (pr *S3Process) Name() string {
	return "s3"
}

func (pr *S3Process) ProcessTask(task updated.UpdateTask) error {

	repo := task.Repo

	pr.mu.Lock()

	files, ok := pr.files[repo]

	if !ok {
		files = make([]string, 0)
	}

	for _, path := range task.Commits {

		repo_path := filepath.Join(pr.data_root, repo)
		abs_path := filepath.Join(repo_path, path)

		wof, err := uri.IsWOFFile(abs_path)

		if err != nil {
			pr.logger.Warning(fmt.Sprintf("Failed to determine if %s is a WOF file, because %s", abs_path, err))
			continue
		}

		if !wof {
			continue
		}

		_, err = os.Stat(abs_path)

		// because this: https://github.com/whosonfirst/go-whosonfirst-updated/issues/8
		//
		// this one is a bit complicated and the decision to disable explicit warnings
		// may well bite us in the ass one day but the problem is that there are sometimes
		// legitimate reasons why a file (specifically an -alt file for which we don't
		// make the same kinds of promises) might be deleted from a repo, but will still
		// show up in the list of files for a commit - maybe it's possible to filter that
		// list to prune things that have been deleted and maybe the rule needs to be that
		// dealing with non-existant files needs to be handled before we get here but today
		// and right now we're not... (20170713/thisisaaronland)

		if os.IsNotExist(err) {
			pr.logger.Debug(fmt.Sprintf("Failed to clone %s, because it doesn't exist", abs_path))
			continue
		}

		files = append(files, path)
	}

	pr.files[repo] = files
	pr.mu.Unlock()

	return pr.ProcessRepo(repo)
}

func (pr *S3Process) ProcessRepo(repo string) error {

	if pr.queue.IsProcessing(repo) {
		return pr.queue.Schedule(repo)
	}

	err := pr.queue.Lock(repo)

	if err != nil {
		return err
	}

	if len(pr.files[repo]) > 0 {
		err = pr._process(repo)

		if err != nil {
			return err
		}
	}

	err = pr.queue.Release(repo)

	if err != nil {
		return err
	}

	return nil
}

func (pr *S3Process) _process(repo string) error {

	t1 := time.Now()

	defer func() {
		t2 := time.Since(t1)
		pr.logger.Status("Time to process (%s) %s: %v", pr.Name(), repo, t2)
	}()

	root := filepath.Join(pr.data_root, repo)

	_, err := os.Stat(root)

	if os.IsNotExist(err) {
		pr.logger.Error("Can't find repo", root)
		return err
	}

	/* sudo wrap all of this in a single function somewhere... */

	pr.mu.Lock()
	files := pr.files[repo]

	delete(pr.files, repo)
	pr.mu.Unlock()

	tmpfile, err := utils.FilesToFileList(files, root)

	if err != nil {

		pr.mu.Lock()

		_, ok := pr.files[repo]

		if ok {

			for _, path := range files {
				pr.files[repo] = append(pr.files[repo], path)
			}

		} else {
			pr.files[repo] = files
		}

		pr.mu.Unlock()

		return err
	}

	/* end of sudo wrap all of this in a single function somewhere... */

	debug := false
	procs := 10

	pr.logger.Debug("Process (S3) file list %s", tmpfile.Name())

	sink := s3.WOFSync(pr.s3_bucket, pr.s3_prefix, procs, debug, pr.logger)

	err = sink.SyncFileList(tmpfile.Name(), root)

	if err != nil {
		pr.logger.Error("Failed to process (S3) file list because %s (%s)", err, tmpfile.Name())
		return err
	}

	pr.logger.Debug("Successfully processed (S3) file list %s", tmpfile.Name())
	os.Remove(tmpfile.Name())

	return nil
}

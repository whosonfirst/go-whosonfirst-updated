package process

import (
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"github.com/whosonfirst/go-whosonfirst-updated/utils"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type ElasticsearchProcess struct {
	Process
	queue      *queue.Queue
	data_root  string
	flushing   bool
	mu         *sync.Mutex
	files      map[string][]string
	es_host    string
	es_port    string
	es_index   string
	index_tool string
	logger     *log.WOFLogger
}

func NewElasticsearchProcess(data_root string, index_tool string, es_host string, es_port string, es_index string, logger *log.WOFLogger) (*ElasticsearchProcess, error) {

	data_root, err := filepath.Abs(data_root)

	if err != nil {
		return nil, err
	}

	_, err = os.Stat(data_root)

	if os.IsNotExist(err) {
		return nil, err
	}

	index_tool, err = filepath.Abs(index_tool)

	if err != nil {
		return nil, err
	}

	_, err = os.Stat(index_tool)

	if os.IsNotExist(err) {
		return nil, err
	}

	q, err := queue.NewQueue()

	if err != nil {
		return nil, err
	}

	files := make(map[string][]string)

	mu := new(sync.Mutex)

	pr := ElasticsearchProcess{
		queue:      q,
		data_root:  data_root,
		flushing:   false,
		mu:         mu,
		files:      files,
		index_tool: index_tool,
		es_host:    es_host,
		es_port:    es_port,
		es_index:   es_index,
		logger:     logger,
	}

	return &pr, nil
}

func (pr *ElasticsearchProcess) Name() string {
	return "elasticsearch"
}

func (pr *ElasticsearchProcess) Flush() error {

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

func (pr *ElasticsearchProcess) ProcessTask(task updated.UpdateTask) error {

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

func (pr *ElasticsearchProcess) ProcessRepo(repo string) error {

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

func (pr *ElasticsearchProcess) _process(repo string) error {

	t1 := time.Now()

	defer func() {
		t2 := time.Since(t1)
		pr.logger.Info("Time to process (%s) %s: %v", pr.Name(), repo, t2)
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

	defer os.Remove(tmpfile.Name())

	// please write me in Go... (20161222/thisisaaronland)

	index_args := []string{
		"--host", pr.es_host,
		"--port", pr.es_port,
		"--index", pr.es_index,
		tmpfile.Name(),
	}

	cmd := exec.Command(pr.index_tool, index_args...)

	pr.logger.Debug("%s %s", pr.index_tool, strings.Join(index_args, " "))

	_, err = cmd.Output()

	if err != nil {
		pr.logger.Error("failed to index Elasticsearch %s", err)
		return err
	}

	return nil
}

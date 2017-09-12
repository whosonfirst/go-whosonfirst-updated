package process

import (
	idx "github.com/whosonfirst/go-whosonfirst-index"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-tile38/client"
	"github.com/whosonfirst/go-whosonfirst-tile38/index"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"github.com/whosonfirst/go-whosonfirst-updated/utils"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"os"
	"path/filepath"
	"sync"
)

type Tile38Process struct {
	Process
	queue      *queue.Queue
	indexer    *index.Tile38Indexer
	data_root  string
	flushing   bool
	mu         *sync.Mutex
	files      map[string][]string
	collection string
	logger     *log.WOFLogger
}

func NewTile38Process(data_root string, t38_host string, t38_port int, t38_collection string, logger *log.WOFLogger) (*Tile38Process, error) {

	data_root, err := filepath.Abs(data_root)

	if err != nil {
		return nil, err
	}

	_, err = os.Stat(data_root)

	if os.IsNotExist(err) {
		return nil, err
	}

	t38_client, err := client.NewRESPClient(t38_host, t38_port)

	if err != nil {
		return nil, err
	}

	t38_indexer, err := index.NewTile38Indexer(t38_client)

	q, err := queue.NewQueue()

	if err != nil {
		return nil, err
	}

	files := make(map[string][]string)

	mu := new(sync.Mutex)

	pr := Tile38Process{
		indexer:    t38_indexer,
		data_root:  data_root,
		collection: t38_collection,
		queue:      q,
		flushing:   false,
		mu:         mu,
		files:      files,
		logger:     logger,
	}

	return &pr, nil
}

func (pr *Tile38Process) Name() string {
	return "tile38"
}

func (pr *Tile38Process) Flush() error {

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

func (pr *Tile38Process) ProcessTask(task updated.UpdateTask) error {

	repo := task.Repo

	pr.mu.Lock()

	files, ok := pr.files[repo]

	if !ok {
		files = make([]string, 0)
	}

	for _, path := range task.Commits {

		is_wof, _ := uri.IsWOFFile(path)

		if !is_wof {
			continue
		}

		is_alt, _ := uri.IsAltFile(path)

		if is_alt {
			continue
		}

		files = append(files, path)
	}

	pr.files[repo] = files
	pr.mu.Unlock()

	return pr.ProcessRepo(repo)
}

func (pr *Tile38Process) ProcessRepo(repo string) error {

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

func (pr *Tile38Process) _process(repo string) error {

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

	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()

	err = pr.indexer.IndexFileList(tmpfile.Name(), pr.collection)

	if err != nil {
		pr.logger.Error("Failed to process (Tile38) file list because %s (%s)", err, tmpfile.Name())
		return err
	}

	pr.logger.Debug("Successfully processed (Tile38) file list %s", tmpfile.Name())

	return nil

}

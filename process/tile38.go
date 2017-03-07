package process

import (
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-tile38/index"
	"github.com/whosonfirst/go-whosonfirst-updated"
	"github.com/whosonfirst/go-whosonfirst-updated/queue"
	"github.com/whosonfirst/go-whosonfirst-updated/utils"
	"github.com/whosonfirst/go-whosonfirst-uri"	
)

type Tile38Process struct {
	Process
	client     *index.Tile38Client
	data_root  string
	flushing   bool
	mu         *sync.Mutex
	files      map[string][]string
	collection string
	logger     *log.WOFLogger
}

func NewTile38Process(tile38_host string, tile38_port int, tile38_collection string, logger *log.WOFLogger) (*Tile38Process, error) {

	client, err := tile38.NewTile38Client(*tile38_host, *tile38_port)

	if err != nil {
		return nil, err
	}

	q, err := queue.NewQueue()

	if err != nil {
		return nil, err
	}

	files := make(map[string][]string)

	mu := new(sync.Mutex)

	pr := Tile38Process{
		client:     client,
		collection: collection,
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

		if ! uri.IsWOFFile(path){
		   continue
		}
		
		if uri.IsAltFile(path){
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

	pr.client.IndexFile(path, pr.collection)
}

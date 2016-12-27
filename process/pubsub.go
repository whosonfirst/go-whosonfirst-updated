package process

import (
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-updated"
	// "github.com/whosonfirst/go-whosonfirst-updated/queue"
	"gopkg.in/redis.v1"
	// "sync"
	// "time"
)

type PubSubProcess struct {
	Process
	// queue     *queue.Queue
	data_root string
	flushing  bool
	// mu        *sync.Mutex
	// files     map[string][]string
	logger         *log.WOFLogger
	pubsub_host    string
	pubsub_port    int
	pubsub_channel string
}

func NewPubSubProcess(data_root, pubsub_host string, pubsub_port int, pubsub_channel string, logger *log.WOFLogger) (*PubSubProcess, error) {

	// TO DO: ensure pubsub connection here

	pr := PubSubProcess{
		data_root:      data_root,
		flushing:       false,
		pubsub_host:    pubsub_host,
		pubsub_port:    pubsub_port,
		pubsub_channel: pubsub_channel,
		logger:         logger,
	}

	return &pr, nil
}

func (pr *PubSubProcess) Name() string {
	return "pubsub"
}

// TO DO: figure out what blocking/flushing means here...

func (pr *PubSubProcess) Flush() error {
	return nil
}

func (pr *PubSubProcess) ProcessTask(task updated.UpdateTask) error {

	redis_endpoint := fmt.Sprintf("%s:%d", pr.pubsub_host, pr.pubsub_port)

	redis_client := redis.NewTCPClient(&redis.Options{
		Addr: redis_endpoint,
	})

	defer redis_client.Close()

	for _, path := range task.Commits {
		redis_client.Publish(pr.pubsub_channel, path)
	}

	return nil
}

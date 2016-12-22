package process

import (
	"github.com/whosonfirst/go-whosonfirst-updated"
)

type Process interface {
	ProcessTask(task updated.UpdateTask) error
	Name() string
}

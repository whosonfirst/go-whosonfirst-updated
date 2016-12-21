package process

import (
	"github.com/whosonfirst/go-whosonfirst-updated"
)

type Processor interface {
	Process(task updated.UpdateTask) error
}

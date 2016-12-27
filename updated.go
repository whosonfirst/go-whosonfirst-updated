package updated

import (
	"fmt"
)

type UpdateTask struct {
	Hash    string
	Repo    string
	Commits []string
}

func (t UpdateTask) String() string {

	count := len(t.Commits)

	if count == 1 {
		return fmt.Sprintf("%s#%s (1 file)", t.Hash, t.Repo)
	} else {
		return fmt.Sprintf("%s#%s (%d files)", t.Hash, t.Repo, count)
	}
}

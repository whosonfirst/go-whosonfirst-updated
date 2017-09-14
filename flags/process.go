package flags

import (
	"github.com/whosonfirst/go-whosonfirst-updated/process"
	"strconv"
	"strings"
)

type ProcessFlags struct {
	flags []string
}

func (fl *ProcessFlags) String() string {
	return strings.Join(fl.flags, "\n")
}

func (fl *ProcessFlags) Set(value string) error {
	fl.flags = append(fl.flags, value)
	return nil
}

func (fl ProcessFlags) ToProcesses() ([]process.Process, error) {

	procs := make([]process.Process, 0)

	for _, p := range fl.flags {

	}

	return procs, nil
}

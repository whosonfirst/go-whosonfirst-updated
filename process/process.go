package process

type Processor interface {
	Process(repo string) error
}

package store

type Storer interface {
	Init() (err error)
	GetVolumer(vid int32) (*Volumer, bool)
	NewVolumer(vid int32, dir string) *Volumer
}

type Volumer interface {
	NewDataer(vid int32, dir string) *Dataer
	NewIndexer(vid int32, dir string) *Indexer
	Close()
}

type Dataer interface {
	Init() (err error)
	Close()
}

type Indexer interface {
	Close()
}

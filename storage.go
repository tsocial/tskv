package tskv

import (
	"path"

	uuid "github.com/satori/go.uuid"
)

const Latest = "latest"

// Dir is a Hierarchical representation of a Path at which a node is expected to be found.
type Dir struct {
	Name  string
	Child *Dir
}

func (n *Dir) Path() string {
	d := n.Name
	if n.Child != nil {
		d = path.Join(d, n.Child.Path())
	}
	return d
}
func GenerateUuid() string {
	return uuid.NewV4().String()
}

func MakeDir(nodes ...string) *Dir {
	if len(nodes) < 1 {
		return nil
	}

	t := Dir{Name: nodes[0]}
	if len(nodes) > 1 {
		t.Child = MakeDir(nodes[1:]...)
	}

	return &t
}

type FileHandler interface {
	Name() string
	Path(dir *Dir) string
	Read() ([]byte, error)
	Write([]byte) error
	UTime(string)
	IsCompressed() bool
}

type Storer interface {
	Setup() error
	Teardown() error

	Get(file FileHandler, dir *Dir) error
	GetKeys(prefix string, separator string) ([]string, error)
	GetVersion(file FileHandler, dir *Dir, version string) error
	GetVersions(file FileHandler, dir *Dir) ([]string, error)
	Save(file FileHandler, dir *Dir) error
	SaveTag(file FileHandler, dir *Dir, ts string) error
	DeleteKeys(prefix string) error

	Lock(key, s string) error
	Unlock(key string) error
}

package storage

import (
	"path"

	uuid "github.com/satori/go.uuid"
)

const Latest = "latest"

// Tree is a Hierarchical representation of a Path at which a node is expected to be found.
type Tree struct {
	Name  string
	Child *Tree
}

func (n *Tree) MakePath() string {
	d := n.Name
	if n.Child != nil {
		d = path.Join(d, n.Child.MakePath())
	}
	return d
}
func GenerateUuid() string {
	return uuid.NewV4().String()
}

func MakeTree(nodes ...string) *Tree {
	if len(nodes) < 1 {
		return &Tree{Name: "unknown"}
	}

	t := Tree{Name: nodes[0]}
	if len(nodes) > 1 {
		t.Child = MakeTree(nodes[1:]...)
	}

	return &t
}

type ReaderWriter interface {
	Key() string
	MakePath(tree *Tree) string
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	SaveId(string)
	IsCompressed() bool
}

type Store interface {
	Setup() error
	Teardown() error

	Get(reader ReaderWriter, tree *Tree) error
	GetKeys(prefix string, separator string) ([]string, error)
	GetVersion(reader ReaderWriter, tree *Tree, version string) error
	GetVersions(reader ReaderWriter, tree *Tree) ([]string, error)
	Save(reader ReaderWriter, tree *Tree) error
	SaveTag(reader ReaderWriter, tree *Tree, ts string) error
	DeleteKeys(prefix string) error

	Lock(key, s string) error
	Unlock(key string) error
}

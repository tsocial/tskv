package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"github.com/tsocial/tskv/storage"
	"gopkg.in/alecthomas/kingpin.v2"
)

const Version = "0.0.1"
const Archive = "archive"

func timestamp() string {
	return fmt.Sprintf("%v", time.Now().UnixNano())
}

var (
	app        = kingpin.New("tskv", "")
	consulAddr = app.Flag("consul", "Consul address").OverrideDefaultFromEnvar("TSKV_CONSUL_ADDR").String()

	getCmd    = app.Command("get", "Get last set value of a key")
	getCmdKey = getCmd.Arg("key", "Name to get").Required().String()

	setCmd    = app.Command("set", "Set a key")
	setCmdKey = setCmd.Arg("key", "Name").Required().String()
	setCmdTag = setCmd.Flag("tag", "Tag").Default(timestamp()).String()
	setCmdVal = setCmd.Arg("value", "File").Required().File()

	rollbackCmd    = app.Command("rollbackVersion", "Rollback value of key to a specified tag")
	rollbackCmdTag = rollbackCmd.Flag("tag", "Tag").Required().String()
	rollbackCmdKey = rollbackCmd.Arg("key", "Name").Required().String()

	listTagCmd = app.Command("list", "List tags")
	listCmdKey = listTagCmd.Arg("key", "Name").Required().String()
)

func MakeFile(name string, content []byte) *File {
	if content == nil {
		content = []byte{}
	}
	return &File{name: name, content: content}
}

type File struct {
	content []byte
	name    string
}

func (w *File) UTime(string) {}

func (w *File) IsCompressed() bool {
	return false
}

func (w *File) Name() string {
	return w.name
}

func (w *File) Path(t *storage.Dir) string {
	return path.Join(t.Path(), w.name)
}

func (w *File) Write(b []byte) error {
	if b == nil {
		b = []byte{}
	}
	w.content = b
	return nil
}

func (w *File) Read() ([]byte, error) {
	return w.content, nil
}

func getFile(c storage.Storer, name string) []byte {
	w := MakeFile(name, nil)

	err := c.Get(w, storage.MakeDir())
	if err != nil {
		panic(err)
	}
	return w.content
}

func createFile(c storage.Storer, name, version string, b []byte) {
	w := MakeFile(name, b)
	if err := c.SaveTag(w, storage.MakeDir(Archive), version); err != nil {
		panic(err)
	}

	if err := c.SaveTag(w, storage.MakeDir(), version); err != nil {
		panic(err)
	}
}

func rollbackVersion(c storage.Storer, name, version string) {
	dir := storage.MakeDir(Archive)
	w := MakeFile(name, nil)
	if err := c.GetVersion(w, dir, version); err != nil {
		panic(err)
	}

	if err := c.SaveTag(w, dir, timestamp()); err != nil {
		panic(err)
	}

	if err := c.SaveTag(w, storage.MakeDir(), version); err != nil {
		panic(err)
	}
}

func listVersions(c storage.Storer, name string) []string {
	dir := storage.MakeDir(Archive)
	w := MakeFile(name, nil)

	l, err := c.GetVersions(w, dir)
	if err != nil {
		panic(err)
	}
	return l
}

func main() {
	app.Version(Version)

	c := storage.MakeConsulStore(*consulAddr)
	if err := c.Setup(); err != nil {
		panic(err)
	}

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case getCmd.FullCommand():
		fmt.Printf(string(getFile(c, *getCmdKey)))

	case setCmd.FullCommand():
		b, err := ioutil.ReadAll(*setCmdVal)
		if err != nil {
			panic(err)
		}
		createFile(c, *setCmdKey, *setCmdTag, b)

	case rollbackCmd.FullCommand():
		rollbackVersion(c, *rollbackCmdKey, *rollbackCmdTag)

	case listTagCmd.FullCommand():
		versions := listVersions(c, *listCmdKey)
		fmt.Println(versions)

	default:
		log.Println(app.Help)
	}
}

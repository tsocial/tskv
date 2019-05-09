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

	getCmd    = app.Command("get", "Get last set value of a name")
	getCmdKey = getCmd.Arg("name", "Name to get").Required().String()

	setCmd    = app.Command("set", "Set a name")
	setCmdKey = setCmd.Arg("name", "Name").Required().String()
	setCmdTag = setCmd.Flag("tag", "Tag").Default(timestamp()).String()
	setCmdVal = setCmd.Arg("value", "File").Required().File()

	rollbackCmd    = app.Command("rollback", "Rollback value of name to a specified tag")
	rollbackCmdTag = rollbackCmd.Flag("tag", "Tag").Required().String()
	rollbackCmdKey = rollbackCmd.Arg("name", "Name").Required().String()

	listTagCmd = app.Command("list", "List tags")
	listCmdKey = listTagCmd.Arg("name", "Name").Required().String()
)

func MakeFile(name string, content []byte) *File {
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
	w.content = b
	return nil
}

func (w *File) Read() ([]byte, error) {
	return w.content, nil
}

func getKey(c storage.Storer, name string) []byte {
	w := MakeFile(name, nil)

	err := c.Get(w, nil)
	if err != nil {
		panic(err)
	}
	return w.content
}

func setKey(c storage.Storer, name, version string, b []byte) {
	//NOTE: Trim the extra newlinke character
	//b = bytes.TrimRight(b, "\n")

	w := MakeFile(name, b)
	if err := c.SaveTag(w, storage.MakeDir(Archive), version); err != nil {
		panic(err)
	}

	if err := c.SaveTag(w, nil, version); err != nil {
		panic(err)
	}
}

func rollback(c storage.Storer, name, version string) {
	tree := storage.MakeDir(Archive)
	w := MakeFile(name, nil)
	if err := c.GetVersion(w, tree, version); err != nil {
		panic(err)
	}

	if err := c.SaveTag(w, tree, timestamp()); err != nil {
		panic(err)
	}

	if err := c.SaveTag(w, nil, version); err != nil {
		panic(err)
	}
}

func listVersions(c storage.Storer, name string) []string {
	tree := storage.MakeDir(Archive)
	w := MakeFile(name, nil)

	l, err := c.GetVersions(w, tree)
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
		log.Println(string(getKey(c, *getCmdKey)))

	case setCmd.FullCommand():
		b, err := ioutil.ReadAll(*setCmdVal)
		if err != nil {
			panic(err)
		}
		setKey(c, *setCmdKey, *setCmdTag, b)

	case rollbackCmd.FullCommand():
		rollback(c, *rollbackCmdKey, *rollbackCmdTag)

	case listTagCmd.FullCommand():
		versions := listVersions(c, *listCmdKey)
		log.Println(versions)

	default:
		log.Println(app.Help)
	}
}

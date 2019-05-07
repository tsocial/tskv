package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"github.com/tsocial/tessellate/storage/types"

	"github.com/tsocial/tessellate/storage/consul"

	"gopkg.in/alecthomas/kingpin.v2"
)

const Version = "0.0.1"

func timestamp() string {
	return fmt.Sprintf("%v", time.Now().UnixNano())
}

var (
	app        = kingpin.New("tskv", "")
	consulAddr = app.Flag("consul", "Consul address").OverrideDefaultFromEnvar("TSKV_CONSUL_ADDR").String()

	getCmd    = app.Command("get", "Get last set value of a key")
	getCmdKey = getCmd.Arg("key", "Key to get").Required().String()

	setCmd    = app.Command("set", "Set a key")
	setCmdKey = setCmd.Arg("key", "Key").Required().String()
	setCmdTag = setCmd.Flag("tag", "Tag").Default(timestamp()).String()
	setCmdVal = setCmd.Arg("value", "Value").Required().File()

	rollbackCmd    = app.Command("rollback", "Rollback value of key to a specified tag")
	rollbackCmdTag = rollbackCmd.Flag("tag", "Tag").Required().String()
	rollbackCmdKey = rollbackCmd.Arg("key", "Key").Required().String()

	listTagCmd = app.Command("list", "List tags")
	listCmdKey = listTagCmd.Arg("key", "Key").Required().String()
)

type Value []byte

func (w *Value) SaveId(string) {}

func (w *Value) MakePath(t *types.Tree) string {
	return path.Join("archive", t.Name)
}

func (w *Value) Unmarshal(b []byte) error {
	*w = Value(b)
	return nil
}

func (w *Value) Marshal() ([]byte, error) {
	return []byte(*w), nil
}

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	app.Version(Version)
	c := consul.MakeConsulStore(*consulAddr)
	if err := c.Setup(); err != nil {
		panic(err)
	}

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case getCmd.FullCommand():
		b, err := c.GetKey(*getCmdKey)
		if err != nil {
			panic(err)
		}
		log.Println(string(b))

	case setCmd.FullCommand():
		b, err := ioutil.ReadAll(*setCmdVal)
		if err != nil {
			panic(err)
		}
		// Tree for workspace ID.
		tree := types.MakeTree(*setCmdKey)

		// Create a new types.Workspace instance to be returned.
		w := Value(b)
		if err := c.SaveTag(&w, tree, *setCmdTag); err != nil {
			panic(err)
		}
		// Latest value
		if err := c.SaveKey(*setCmdKey, b); err != nil {
			panic(err)
		}

	case rollbackCmd.FullCommand():
		tree := types.MakeTree(*rollbackCmdKey)
		w := Value(nil)
		if err := c.GetVersion(&w, tree, *rollbackCmdTag); err != nil {
			panic(err)
		}

		if err := c.SaveTag(&w, tree, timestamp()); err != nil {
			panic(err)
		}

		if err := c.SaveKey(*rollbackCmdKey, []byte(w)); err != nil {
			panic(err)
		}

	case listTagCmd.FullCommand():
		tree := types.MakeTree(*listCmdKey)
		w := Value(nil)

		l, err := c.GetVersions(&w, tree)
		if err != nil {
			panic(err)
		}
		log.Println(l)

	default:
		log.Println(app.Help)
	}
}

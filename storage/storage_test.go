package storage

import (
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var store Store

func MakeValue(key string, value []byte) *Value {
	return &Value{key: key, storage: value}
}

type Value struct {
	storage []byte
	key     string
}

func (w *Value) SaveId(string) {}

func (w *Value) IsCompressed() bool {
	return false
}

func (w *Value) Key() string {
	return w.key
}

func (w *Value) MakePath(t *Tree) string {
	return path.Join(t.MakePath(), w.key)
}

func (w *Value) Unmarshal(b []byte) error {
	w.storage = b
	return nil
}

func (w *Value) Marshal() ([]byte, error) {
	return w.storage, nil
}

func TestStore(t *testing.T) {
	t.Run("Lock tests", func(t *testing.T) {
		t.Run("Lock a Key", func(t *testing.T) {
			err := store.Lock("key3", "c1")
			assert.Nil(t, err)
		})

		t.Run("Un-Idempotent Lock a Key", func(t *testing.T) {
			err := store.Lock("key3", "c12")
			assert.NotNil(t, err, "Should have raised a key")
		})

		t.Run("Release a Key", func(t *testing.T) {
			err := store.Unlock("key3")
			assert.Nil(t, err)
		})

		t.Run("Idempotent Release a Key", func(t *testing.T) {
			err := store.Unlock("key3")
			assert.Nil(t, err)
		})
	})

	t.Run("Storage tests", func(t *testing.T) {
		tree := MakeTree("store_tree")

		wid := fmt.Sprintf("alibaba-%s", GenerateUuid())
		workspace := MakeValue(wid, nil)

		t.Run("Workspace does not exist", func(t *testing.T) {
			err := store.Get(workspace, tree)
			assert.Nil(t, err, "Should be nil")
		})

		t.Run("Get a Workspace after creation", func(t *testing.T) {
			err := store.Save(workspace, tree)
			assert.Nil(t, err)

			err = store.Get(workspace, tree)
			assert.Nil(t, err)
		})

		t.Run("Re-saving a Workspace doesn't raise an Error", func(t *testing.T) {
			err := store.Save(workspace, tree)
			assert.Nil(t, err)

			err = store.Get(workspace, tree)
			assert.Nil(t, err)

			v, err := store.GetVersions(workspace, tree)
			assert.Nil(t, err)

			assert.Equal(t, 3, len(v))
			assert.Contains(t, strings.Join(v, ""), "latest")
		})

		//t.Run("Save Layout", func(t *testing.T) {
		//	tree := types.MakeTree(wid)
		//	l := types.Layout{
		//		Id:   "test-hello",
		//		Plan: map[string]json.RawMessage{},
		//	}
		//
		//	err := store.Save(&l, tree)
		//	assert.Nil(t, err)
		//
		//	lTree := types.MakeTree(wid, "test-hello")
		//	v := types.Vars(map[string]interface{}{})
		//	err = store.Save(&v, lTree)
		//	assert.Nil(t, err)
		//
		//	x, err := store.GetVersions(&l, tree)
		//	assert.Nil(t, err)
		//
		//	assert.Equal(t, 2, len(x))
		//})

		//t.Run("Get absent Key", func(t *testing.T) {
		//	w := MakeValue("hello/world", nil)
		//
		//	err := store.Get(w, nil)
		//	assert.Nil(t, err)
		//	log.Printf("Absent w: %+v", w)
		//	assert.Equal(t, []byte{}, w.storage)
		//})
		//
		//t.Run("Get Valid Key", func(t *testing.T) {
		//	key := fmt.Sprintf("workspaces/%v/latest", wid)
		//	w := MakeValue(key, nil)
		//	err := store.Get(w, nil)
		//
		//	assert.Nil(t, err)
		//	log.Printf("Present w: %+v", w)
		//	assert.Equal(t, []uint8([]byte{}), w.storage)
		//})

		t.Run("Get Keys", func(t *testing.T) {
			prefix := "workspaces/"
			separator := "/"

			keys, err := store.GetKeys(prefix, separator)
			assert.Nil(t, err)

			for _, k := range keys {
				splits := strings.Split(k, "/")
				assert.Equal(t, 3, len(splits))
			}
		})

		t.Run("Save and Get Key", func(t *testing.T) {
			key := GenerateUuid()
			val := GenerateUuid()

			w := MakeValue(key, []byte(val))
			err := store.Save(w, nil)

			assert.Nil(t, err)

			gerr := store.Get(w, nil)
			assert.Nil(t, gerr)
			assert.Equal(t, val, string(w.storage))
		})
	})
}

func TestTree(t *testing.T) {
	t.Run("Test Trees", func(t *testing.T) {
		t.Run("Empty Tree", func(t *testing.T) {
			x := MakeTree()
			assert.Equal(t, x.MakePath(), "unknown")
		})

		t.Run("One node", func(t *testing.T) {
			x := MakeTree("a")
			assert.Equal(t, x.MakePath(), "a")
		})

		t.Run("Even node", func(t *testing.T) {
			x := MakeTree("a", "b")
			assert.Equal(t, x.MakePath(), "a/b")
		})

		t.Run("Odd Nodes", func(t *testing.T) {
			x := MakeTree("a", "b", "c")
			assert.Equal(t, x.MakePath(), "a/b/c")
		})
	})
}

package main

import (
	"testing"

	"github.com/tsocial/tskv/storage"

	"github.com/magiconair/properties/assert"
)

func TestValue(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		tr := storage.MakeDir("a", "b")
		v := MakeFile("k", []byte("b"))

		nb := []byte("b2")

		t.Run("return path in a tree", func(t *testing.T) {
			assert.Equal(t, v.Path(tr), "a/b/k")
		})

		t.Run("Write changes bytes", func(t *testing.T) {

			if err := v.Write(nb); err != nil {
				t.Error(err)
			}

			assert.Equal(t, v.content, nb)
		})

		t.Run("Read returns the bytes", func(t *testing.T) {
			b, err := v.Read()
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, b, nb)
		})

		t.Run("Name isn't lying", func(t *testing.T) {
			assert.Equal(t, v.Name(), "k")
		})
	})
}

func TestStorageDriver(t *testing.T) {
	bucket := storage.GenerateUuid()

	store := storage.MakeBoltStore(bucket, "/tmp/"+bucket)
	if err := store.Setup(); err != nil {
		panic(err)
	}

	testKey := storage.GenerateUuid()
	oldValue := storage.GenerateUuid()
	newValue := storage.GenerateUuid()

	oldTag := "v1"
	newTag := "v2"

	t.Run("Set a value", func(t *testing.T) {
		t.Run("Should save as custom tag", func(t *testing.T) {
			createFile(store, testKey, oldTag, []byte(oldValue))
			val := getFile(store, testKey)

			assert.Equal(t, string(val), oldValue)
		})
	})

	t.Run("Get a value", func(t *testing.T) {
		t.Run("Should get the value of specified tag", func(t *testing.T) {
			val := getFile(store, testKey)
			assert.Equal(t, string(val), oldValue)
		})
	})

	t.Run("List tags", func(t *testing.T) {
		t.Run("Should return all tags for the given name", func(t *testing.T) {
			tags := listVersions(store, testKey)
			assert.Equal(t, tags, []string{"latest", oldTag})
		})
	})

	t.Run("Test Rollback ", func(t *testing.T) {
		t.Run("Should rollbackVersion the latest value to the specified tag value", func(t *testing.T) {
			createFile(store, testKey, newTag, []byte(newValue))
			val := getFile(store, testKey)

			assert.Equal(t, string(val), newValue)

			rollbackVersion(store, testKey, oldTag)
			oldVal := getFile(store, testKey)

			assert.Equal(t, string(oldVal), oldValue)
		})
	})
}

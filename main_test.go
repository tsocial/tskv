package main

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestValue(t *testing.T) {
	t.Run("Value", func(t *testing.T) {
		tr := MakeTree("a", "b")
		v := MakeValue("k", []byte("b"))

		nb := []byte("b2")

		t.Run("return path in a tree", func(t *testing.T) {
			assert.Equal(t, v.MakePath(tr), "a/b/k")
		})

		t.Run("Unmarshal changes bytes", func(t *testing.T) {

			if err := v.Unmarshal(nb); err != nil {
				t.Error(err)
			}

			assert.Equal(t, v.storage, nb)
		})

		t.Run("Marshal returns the bytes", func(t *testing.T) {
			b, err := v.Marshal()
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, b, nb)
		})

		t.Run("Key isn't lying", func(t *testing.T) {
			assert.Equal(t, v.Key(), "k")
		})
	})
}

func TestStorageDriver(t *testing.T) {
	c := MakeConsulStore()
	if err := c.Setup(); err != nil {
		panic(err)
	}

	testKey := MakeVersion()
	oldValue := MakeVersion()
	newValue := MakeVersion()

	oldTag := "v1"
	newTag := "v2"

	t.Run("Set a value", func(t *testing.T) {
		t.Run("Should save as custom tag", func(t *testing.T) {
			setKey(c, testKey, oldTag, []byte(oldValue))
			val := getKey(c, testKey)

			assert.Equal(t, string(val), oldValue)
		})
	})

	t.Run("Get a value", func(t *testing.T) {
		t.Run("Should get the value of specified tag", func(t *testing.T) {
			val := getKey(c, testKey)
			assert.Equal(t, string(val), oldValue)
		})
	})

	t.Run("List tags", func(t *testing.T) {
		t.Run("Should return all tags for the given key", func(t *testing.T) {
			tags := listVersions(c, testKey)
			assert.Equal(t, tags, []string{"latest", oldTag})
		})
	})

	t.Run("Test Rollback ", func(t *testing.T) {
		t.Run("Should rollback the latest value to the specified tag value", func(t *testing.T) {
			setKey(c, testKey, newTag, []byte(newValue))
			val := getKey(c, testKey)

			assert.Equal(t, string(val), newValue)

			rollback(c, testKey, oldTag)
			oldVal := getKey(c, testKey)

			assert.Equal(t, string(oldVal), oldValue)
		})
	})
}

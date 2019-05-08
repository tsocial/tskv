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
	//var consulAddr string
	//c := MakeConsulStore(consulAddr)

	//testKey := ""
	//testValue := "value"

	t.Run("Set a value", func(t *testing.T) {
		t.Run("Tag defaults to timestamp", func(t *testing.T) {
			//c.SaveKey()
		})

		t.Run("Should save as custom tag", func(t *testing.T) {

		})
	})

	t.Run("Get a value", func(t *testing.T) {
		t.Run("Should return empty value for non-existent key", func(t *testing.T) {

		})

		t.Run("Should get the value of specified tag", func(t *testing.T) {

		})
	})

	t.Run("Test Rollback ", func(t *testing.T) {
		t.Run("Should raise error when rolling back to non-existent tag value", func(t *testing.T) {

		})

		t.Run("Should rollback the latest value to the specified tag value", func(t *testing.T) {

		})
	})

	t.Run("List tags", func(t *testing.T) {
		t.Run("Should raise error with non-existent key", func(t *testing.T) {

		})

		t.Run("Should return all tags for the given key", func(t *testing.T) {

		})
	})
}

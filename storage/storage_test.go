package storage

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestTree(t *testing.T) {
	t.Run("Test Trees", func(t *testing.T) {
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

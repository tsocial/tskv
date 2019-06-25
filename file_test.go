package tskv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFile(t *testing.T) {
	t.Run("Make a file", func(t *testing.T) {
		f := MakeFile("hello", nil)

		t.Run("Should have the same name", func(t *testing.T) {
			assert.Equal(t, f.Name(), "hello")
		})

		t.Run("uTime has no Impact", func(t *testing.T) {
			f.UTime("ok")
		})

		t.Run("Not compressed", func(t *testing.T) {
			assert.Equal(t, f.IsCompressed(), false)
		})

		t.Run("Read returns empty bytes", func(t *testing.T) {
			b, err := f.Read()
			assert.Nil(t, err)
			assert.Empty(t, b)
		})

		t.Run("Write allows byes to be written", func(t *testing.T) {
			if err := f.Write([]byte("hello")); err != nil {
				t.Error(err)
			}

			b, err := f.Read()
			assert.Nil(t, err)
			assert.Equal(t, string(b), "hello")

			if err := f.Write(nil); err != nil {
				t.Error(err)
			}

			b2, err := f.Read()
			assert.Nil(t, err)
			assert.Empty(t, b2)
		})
	})
}

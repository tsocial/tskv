package tskv

import (
	"path"
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

func (w *File) Path(d *Dir) string {
	return path.Join(d.Path(), w.name)
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

package storage

import (
	"testing"
)

func TestMemory(t *testing.T) {
	bucket := RandString(8)

	s := MakeBoltStore(bucket, "/tmp/"+bucket)
	s.Setup()
	defer s.DeleteKeys(bucket)

	tests := MakeTestStore(s)
	tests(t)
}

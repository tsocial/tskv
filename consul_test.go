// +build integration

package tskv

import (
	"os"
	"testing"
)

func TestConsul(t *testing.T) {
	s := MakeConsulStore(os.Getenv("TSKV_CONSUL_ADDR"))
	s.Setup()

	tests := MakeTestStore(s)
	tests(t)
}

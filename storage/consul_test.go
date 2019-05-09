// +build integration

package storage

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	//Seed Random number generator.
	rand.Seed(time.Now().UnixNano())

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	store = MakeConsulStore(os.Getenv("TSKV_CONSUL_ADDR"))
	store.Setup()

	os.Exit(m.Run())
}

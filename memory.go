package tskv

import (
	"bytes"
	"fmt"
	"log"
	"path"
	"strings"
	"time"

	bolt "github.com/etcd-io/bbolt"
	"github.com/pkg/errors"
)

// MakeBoltStore returns a new BoltStore for a given path.
func MakeBoltStore(bucket, path string) *BoltStore {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	return &BoltStore{db: db, bucket: []byte(bucket)}
}

// BoltStore is an in-memory implementation of the Storage driver.
// Should not be used for anything other than testing.
type BoltStore struct {
	db     *bolt.DB
	bucket []byte
}

// GetKeys gets all the keys under a given Prefix
func (e *BoltStore) GetKeys(prefix string, separator string) ([]string, error) {
	keys := map[string]bool{}
	err := e.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(e.bucket).Cursor()

		p := []byte(prefix)
		for k, _ := c.Seek(p); k != nil && bytes.HasPrefix(k, p); k, _ = c.Next() {
			splitByKey := strings.SplitAfter(string(k), prefix)
			split := strings.Split(splitByKey[1], separator)
			if len(split) == 2 {
				keys[splitByKey[0]+split[0]+separator] = true
			}
		}

		return nil
	})

	akeys := make([]string, 0, len(keys))
	for k := range keys {
		akeys = append(akeys, k)
	}

	return akeys, err
}

// Get gets the latest version of a Name.
// Refer to GetVersion for more internal details.
func (e *BoltStore) Get(f FileHandler, dir *Dir) error {
	return e.GetVersion(f, dir, Latest)
}

// GetVersion gets the specific version of a Name.
// Raises error if Name is absent.
func (e *BoltStore) GetVersion(f FileHandler, dir *Dir, version string) error {
	// Same as getKey
	p := f.Name()
	if dir != nil {
		p = path.Join(f.Path(dir), version)
	}

	return e.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(e.bucket)
		bs := b.Get([]byte(p))
		//if bs == nil {
		//	return errors.Errorf("Missing Name %v", p)
		//}

		if err := f.Write(bs); err != nil {
			return errors.Wrap(err, "Cannot unmarshal data into Reader")
		}
		return nil
	})
}

//GetVersions returns an array of all versions that are available for a given key.
func (e *BoltStore) GetVersions(f FileHandler, dir *Dir) ([]string, error) {
	keys := []string{}
	err := e.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Bucket(e.bucket).Cursor()

		key := f.Path(dir)
		prefix := []byte(key)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			splitByKey := strings.SplitAfter(string(k), key+"/")
			for _, k2 := range splitByKey {
				if !strings.Contains(k2, "/") {
					keys = append(keys, k2)
				}
			}
		}

		return nil
	})

	return keys, err
}

// Save is an Internal method to save Any data under a hierarchy that follows revision control.
// Example: In a workspace staging, you wish to save a new layout called dc1
// saveRevision("staging", "layout", "dc1", {....}) will try to save the following structure
// workspace/layouts/dc1/latest
// workspace/layouts/dc1/new_timestamp
// NOTE: This is an atomic operation, so either everything is written or nothing is.
// The operation may take its own sweet time before a quorum write is guaranteed.
func (e *BoltStore) Save(source FileHandler, dir *Dir) error {
	ts := time.Now().UnixNano()
	return e.SaveTag(source, dir, fmt.Sprintf("%+v", ts))
}

func (e *BoltStore) SaveTag(source FileHandler, dir *Dir, ts string) error {
	b, err := source.Read()
	if err != nil {
		return errors.Wrap(err, "Cannot Read vars")
	}

	var items []string

	if dir == nil {
		items = []string{source.Name()}
	} else {
		p := source.Path(dir)
		items = []string{
			path.Join(p, Latest),
			path.Join(p, ts),
		}
	}

	if err := e.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(e.bucket)
		for _, elem := range items {
			if err := bucket.Put([]byte(elem), b); err != nil {
				return err
			}
		}
		return nil

	}); err != nil {
		return errors.New("Txn was rolled back. Weird, huh")
	}

	source.UTime(fmt.Sprintf("%v", ts))

	return nil
}

// DeleteKeys is actually DeleteBucket when in BoltDB
// It was just introduced for Consul designed tests.
func (e *BoltStore) DeleteKeys(prefix string) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(prefix))
	})
}

// Setup creates a new Bucket if it doesn't exist already.
// Ideally it should be deleted after every test.
func (e *BoltStore) Setup() error {
	return e.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(e.bucket); err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
}

// Teardown has not been implemented yet.
func (e *BoltStore) Teardown() error {
	return nil
}

// Lock tries to lock a key with a given value.
// As of now value doesnt matter, existence of zero length value is assumed as Lock.
func (e *BoltStore) Lock(key, s string) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(e.bucket)
		if len(bucket.Get([]byte(key))) == 0 {
			return bucket.Put([]byte(key), []byte(s))
		}
		return errors.Errorf("Name %v is already locked", key)
	})
}

// Unlock the key previously locked.
// Does not raise error if called multiple times.
func (e *BoltStore) Unlock(key string) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(e.bucket)
		return bucket.Delete([]byte(key))
	})
}

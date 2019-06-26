package tskv

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

func MakeConsulStore(addr ...string) *ConsulStore {
	return &ConsulStore{addr: addr}
}

type ConsulStore struct {
	addr   []string
	client *api.Client
}

func (e *ConsulStore) get(key string, f FileHandler) ([]byte, error) {
	b, _, err := e.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}

	if b == nil {
		return []byte{}, nil
	}

	// check if response is valid json
	var res interface{}
	if err := json.Unmarshal(b.Value, &res); err == nil {
		return b.Value, err
	}

	// When the content is gzipped
	if f.IsCompressed() {
		r, err := gzip.NewReader(bytes.NewReader(b.Value))
		if err != nil {
			return nil, fmt.Errorf("invalid gzip or json")
		}
		data, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}

		return data, nil
	}

	return b.Value, err
}

func (e *ConsulStore) gzip(unzipped []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(unzipped); err != nil {
		return nil, err
	}

	if err := gz.Flush(); err != nil {
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (e *ConsulStore) Get(f FileHandler, dir *Dir) error {
	return e.GetVersion(f, dir, Latest)
}

func (e *ConsulStore) GetKeys(prefix string, separator string) ([]string, error) {
	l, _, err := e.client.KV().Keys(prefix, separator, nil)
	return l, err
}

func (e *ConsulStore) GetVersion(f FileHandler, dir *Dir, version string) error {
	// Same as getKey
	p := f.Name()
	if dir != nil {
		p = path.Join(f.Path(dir), version)
	}

	// Get the vars for the layout.
	b, err := e.get(p, f)
	if err != nil {
		return errors.Wrapf(err, "Cannot fetch object for %v", p)
	}

	//if b == nil || len(b) == 0 {
	//	return errors.Errorf("Missing Name %v", p)
	//}

	if err := f.Write(b); err != nil {
		return errors.Wrap(err, "Cannot unmarshal data into Reader")
	}

	return nil
}

func (e *ConsulStore) GetVersions(f FileHandler, dir *Dir) ([]string, error) {
	key := f.Path(dir)
	l, _, err := e.client.KV().Keys(key, "", nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot list %v", key)
	}

	var keys []string
	for _, k := range l {
		splitByKey := strings.SplitAfter(k, key+"/")
		for _, k2 := range splitByKey {
			if !strings.Contains(k2, "/") {
				keys = append(keys, k2)
			}
		}
	}

	return keys, nil
}

// Internal method to save Any data under a hierarchy that follows revision control.
// Example: In a workspace staging, you wish to save a new layout called dc1
// saveRevision("staging", "layout", "dc1", {....}) will try to save the following structure
// workspace/layouts/dc1/latest
// workspace/layouts/dc1/new_timestamp
// NOTE: This is an atomic operation, so either everything is written or nothing is.
// The operation may take its own sweet time before a quorum write is guaranteed.
func (e *ConsulStore) Save(source FileHandler, dir *Dir) error {
	ts := time.Now().UnixNano()
	return e.SaveTag(source, dir, fmt.Sprintf("%+v", ts))
}

func (e *ConsulStore) SaveTag(f FileHandler, dir *Dir, ts string) error {
	b, err := f.Read()
	if err != nil {
		return errors.Wrap(err, "Cannot Read vars")
	}

	items := []string{f.Name()}

	if dir != nil {
		p := f.Path(dir)
		items = []string{
			path.Join(p, Latest),
			path.Join(p, ts),
		}
	}

	session := GenerateUuid()

	lock, err := e.client.LockKey(path.Join(f.Name(), "lock"))
	if err != nil {
		return errors.Wrap(err, "Cannot Lock key")
	}
	defer lock.Unlock()

	var gz = b
	if f.IsCompressed() {
		gz, err = e.gzip(b)
		if err != nil {
			return err
		}
	}

	// Create a Tx Chain of Ops.
	ops := api.KVTxnOps{}
	for _, elem := range items {
		ops = append(ops, &api.KVTxnOp{
			Verb:    api.KVSet,
			Key:     string(elem),
			Value:   gz,
			Session: session,
		})
	}

	if ok, _, _, err := e.client.KV().Txn(ops, nil); err != nil {
		return errors.Wrap(err, "Cannot save Consul Transaction")
	} else if !ok {
		return errors.New("Txn was rolled back. Weird, huh!")
	}

	f.UTime(fmt.Sprintf("%v", ts))

	return nil
}

func (e *ConsulStore) DeleteKeys(prefix string) error {
	_, err := e.client.KV().DeleteTree(prefix+"/", &api.WriteOptions{})
	return err
}

func (e *ConsulStore) Setup() error {
	conf := api.DefaultConfig()
	if len(e.addr) > 0 {
		conf.Address = e.addr[0]
	}

	client, err := api.NewClient(conf)
	if err != nil {
		return err
	}

	e.client = client
	return nil
}

func (e *ConsulStore) Teardown() error {
	return nil
}

func (e *ConsulStore) Lock(key, s string) error {
	ok, _, err := e.client.KV().CAS(&api.KVPair{
		Key:         path.Join("lock", key),
		ModifyIndex: 0,
		CreateIndex: 0,
		Value:       []byte(s),
	}, nil)

	if err != nil {
		return err
	}

	if !ok {
		return errors.New("Cannot write Lock")
	}

	return nil
}

func (e *ConsulStore) Unlock(key string) error {
	key = path.Join("lock", key)
	_, err := e.client.KV().Delete(key, nil)
	if err != nil {
		return err
	}

	return nil
}

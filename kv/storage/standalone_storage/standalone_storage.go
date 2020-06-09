package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{db: engine_util.CreateDB("standalone", conf)}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return NewStandAloneStorageReader(s.db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, mod := range batch {
			switch mod.Data.(type) {
			case storage.Put:
				err := txn.Set(engine_util.KeyWithCF(mod.Cf(), mod.Key()), mod.Value())
				if err != nil {
					return err
				}
			case storage.Delete:
				err := txn.Delete(engine_util.KeyWithCF(mod.Cf(), mod.Key()))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

type StandAloneStorageReader struct {
	db *badger.DB
	txn *badger.Txn
}

func NewStandAloneStorageReader(db *badger.DB) *StandAloneStorageReader {
	return &StandAloneStorageReader{db: db}
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(sr.db, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	return val, nil
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := sr.db.NewTransaction(false)
	sr.txn = txn

	return engine_util.NewCFIterator(cf, txn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}

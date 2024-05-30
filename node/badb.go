package node

import (
	arma "arma/pkg"
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type BatchAttestationDB struct {
	db     *leveldb.DB
	logger arma.Logger
}

func NewBatchAttestationDB(path string, logger arma.Logger) (*BatchAttestationDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	return &BatchAttestationDB{db: db, logger: logger}, err
}

func (db *BatchAttestationDB) Close() {
	db.db.Close()
}

func (db *BatchAttestationDB) List() ([][]byte, []uint64) {
	iter := db.db.NewIterator(&util.Range{}, nil)
	defer iter.Release()

	var digests [][]byte
	var epochs []uint64

	for iter.Next() {
		key := iter.Key()
		if key[0] == 0 {
			digests = append(digests, key[1:])
			epochs = append(epochs, binary.BigEndian.Uint64(iter.Value()))
		}
	}

	return digests, epochs
}

func (db *BatchAttestationDB) Exists(digest []byte) bool {
	_, err := db.db.Get(makeDigestKey(digest), nil)
	return err == nil
}

func (db *BatchAttestationDB) Put(digest [][]byte, epoch []uint64) {
	batch := new(leveldb.Batch)
	for i := 0; i < len(digest); i++ {
		epochBuff := make([]byte, 8)
		binary.BigEndian.PutUint64(epochBuff, epoch[i])

		batch.Put(makeDigestKey(digest[i]), epochBuff)
		batch.Put(makeEpochKey(digest[i], epochBuff), []byte{0})
	}

	if err := db.db.Write(batch, nil); err != nil {
		db.logger.Panicf("Failed updating database: %v", err)
	}
}

func (db *BatchAttestationDB) Clean(epochToDelete uint64) {
	iter := db.db.NewIterator(util.BytesPrefix([]byte{1}), nil)
	defer iter.Release()

	batch := new(leveldb.Batch)

	for iter.Next() {
		epochKey := iter.Key()
		epochBuff := epochKey[1:]
		epoch := binary.BigEndian.Uint64(epochBuff[:8])
		digest := epochBuff[8:]
		if epoch >= epochToDelete {
			continue
		}

		batch.Delete(makeDigestKey(digest))
		batch.Delete(epochKey)
	}

	db.db.Write(batch, nil)
}

func makeDigestKey(digest []byte) []byte {
	buff := make([]byte, len(digest)+1)
	copy(buff[1:], digest)
	return buff
}

func makeEpochKey(digest []byte, epoch []byte) []byte {
	buff := make([]byte, len(digest)+len(epoch)+1)
	buff[0] = 1
	copy(buff[1:], epoch)
	copy(buff[len(epoch)+1:], digest)
	return buff
}

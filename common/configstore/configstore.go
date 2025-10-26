/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configstore

import (
	"fmt"
	"sort"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/filerepo"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	fileSuffix           = "config.pb.bin"
	fileNameSeparator    = "-"
	baseFileNameTemplate = "block" + fileNameSeparator + "%d"
)

// Store holds a set of numbered blocks, typically config blocks.
// Blocks can be added, listed, and retrieved.
// A convenience method allows fetching the last block.
type Store struct {
	fileRepo *filerepo.Repo
}

// NewStore initializes a new config store at the storeDir.
// All file system operations on the returned file repo are thread safe.
func NewStore(storeDir string) (*Store, error) {
	repo, err := filerepo.New(storeDir, fileSuffix)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to create config store")
	}

	s := &Store{
		fileRepo: repo,
	}

	return s, nil
}

// Add adds a block to the store. One cannot add a block with the same block number twice.
func (s *Store) Add(block *common.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	if block.Header == nil {
		return errors.New("block header is nil")
	}

	content, err := proto.Marshal(block)
	if err != nil {
		return errors.Wrap(err, "failed to marshal block")
	}

	fileName := fmt.Sprintf(baseFileNameTemplate, block.Header.Number)

	err = s.fileRepo.Save(fileName, content)
	if err != nil {
		return errors.Wrapf(err, "failed to add block %d", block.GetHeader().GetNumber())
	}

	return nil
}

// GetByNumber retrieves a block by its number.
func (s *Store) GetByNumber(num uint64) (*common.Block, error) {
	fileName := fmt.Sprintf(baseFileNameTemplate, num)
	return s.readByName(fileName)
}

// ListBlocks retrieves an array of all blocks, sorted by number.
func (s *Store) ListBlocks() ([]*common.Block, error) {
	fileNames, err := s.fileRepo.List()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list blocks")
	}

	var blocks []*common.Block
	for i, fileName := range fileNames {
		block, err := s.readByName(fileName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed reading block %s, %d out of %d; list: %v", fileName, i+1, len(fileNames), fileNames)
		}

		blocks = append(blocks, block)
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].GetHeader().GetNumber() < blocks[j].GetHeader().GetNumber()
	})

	return blocks, nil
}

func (s *Store) readByName(fileName string) (*common.Block, error) {
	baseName := s.fileRepo.FileToBaseName(fileName)
	content, err := s.fileRepo.Read(baseName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed reading block %s", fileName)
	}

	block := &common.Block{}
	err = proto.Unmarshal(content, block)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal block %s", fileName)
	}

	return block, nil
}

// ListBlockNumbers retrieves a sorted array of all block numbers.
func (s *Store) ListBlockNumbers() ([]uint64, error) {
	fileNames, err := s.fileRepo.List()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list blocks")
	}

	var nums []uint64
	for i, fileName := range fileNames {
		block, err := s.readByName(fileName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed reading block %s, %d out of %d; list: %v", fileName, i+1, len(fileNames), fileNames)
		}
		nums = append(nums, block.GetHeader().GetNumber())
	}

	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	return nums, nil
}

// Last retrieves the last (highest numbered) block.
func (s *Store) Last() (*common.Block, error) {
	nums, err := s.ListBlockNumbers()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list block numbers")
	}

	if len(nums) == 0 {
		return nil, errors.New("config store is empty")
	}

	return s.GetByNumber(nums[len(nums)-1])
}

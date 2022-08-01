package storage

import (
	. "github.com/huandu/go-assert"
	"strings"
	"testing"
)

func TestCreatePartitionMeta(t *testing.T) {
	partitionMeta := CreatePartitionMeta(1, 1)
	AssertNotEqual(t, partitionMeta, nil)
	Assert(t, partitionMeta.PartitionId > 20220801)
	AssertEqual(t, partitionMeta.MaxBlockNum, 1)
	AssertEqual(t, partitionMeta.MinBlockNum, 1)
	AssertEqual(t, partitionMeta.Level, 0)
}

func TestGetDirectorName(t *testing.T) {
	partitionMeta := CreatePartitionMeta(1, 1)
	Assert(t, len(partitionMeta.GetDirectorName()) >= 14)
	Assert(t, strings.Contains(partitionMeta.GetDirectorName(), "_"))
	AssertEqual(t, strings.Count(partitionMeta.GetDirectorName(), "_"), 3)
}

func TestMergeDirectory(t *testing.T) {
	partitionMeta1 := CreatePartitionMeta(1, 1)
	partitionMeta2 := CreatePartitionMeta(2, 2)
	partitionMeta3 := partitionMeta1.MergeDirectory(partitionMeta2)
	AssertNotEqual(t, partitionMeta3, nil)
	Assert(t, partitionMeta3.PartitionId > 20220801)
	AssertEqual(t, partitionMeta3.PartitionId, partitionMeta1.PartitionId)
	AssertEqual(t, partitionMeta3.PartitionId, partitionMeta2.PartitionId)
	AssertEqual(t, partitionMeta3.MaxBlockNum, 2)
	AssertEqual(t, partitionMeta3.MinBlockNum, 1)
	AssertEqual(t, partitionMeta3.Level, 1)
}

func TestMergePartition(t *testing.T) {
}

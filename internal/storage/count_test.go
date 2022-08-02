package storage

import (
	. "github.com/huandu/go-assert"
	"testing"
)

func TestMergeCount(t *testing.T) {
	c1 := &PartitionRawCount{
		partitionRawCount: 1,
	}
	c2 := &PartitionRawCount{
		partitionRawCount: 2,
	}
	c3 := c1.MergeCount(c2)
	AssertNotEqual(t, c3, nil)
	AssertEqual(t, c3.partitionRawCount, int64(3))
	AssertEqual(t, c3.ToString(), "3")
}

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

func TestToString(t *testing.T) {
	partitionMeta := CreatePartitionMeta(1, 1)
	Assert(t, len(partitionMeta.ToString()) >= 14)
	Assert(t, strings.Contains(partitionMeta.ToString(), "_"))
	AssertEqual(t, strings.Count(partitionMeta.ToString(), "_"), 3)
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
	p1 := &Partition{
		PartitionMeta: CreatePartitionMeta(1, 1),
		Columns: &Columns{formatVersion: 1, columns: []*ColumnItem{
			{name: "id", columnType: "UInt32"},
			{name: "sku_id", columnType: "String"},
			{name: "total_amount", columnType: "Decimal(16,2)"},
			{name: "create_time", columnType: "DateTime"},
		}},
		Count: &PartitionRawCount{partitionRawCount: 100},
	}
	p2 := &Partition{
		PartitionMeta: CreatePartitionMeta(2, 2),
		Columns: &Columns{formatVersion: 1, columns: []*ColumnItem{
			{name: "id", columnType: "UInt32"},
			{name: "sku_id", columnType: "String"},
			{name: "total_amount", columnType: "Decimal(16,2)"},
			{name: "create_time", columnType: "DateTime"},
		}},
		Count: &PartitionRawCount{partitionRawCount: 900},
	}
	p3 := p1.MergePartition(p2)
	AssertNotEqual(t, p3, nil)
	Assert(t, p3.PartitionMeta.PartitionId > 20220801)
	AssertEqual(t, p3.PartitionMeta.PartitionId, p1.PartitionMeta.PartitionId)
	AssertEqual(t, p3.PartitionMeta.PartitionId, p2.PartitionMeta.PartitionId)
	AssertEqual(t, p3.PartitionMeta.MaxBlockNum, 2)
	AssertEqual(t, p3.PartitionMeta.MinBlockNum, 1)
	AssertEqual(t, p3.PartitionMeta.Level, 1)
	AssertEqual(t, p3.Columns, p1.Columns)
	AssertEqual(t, p3.Count.partitionRawCount, int64(1000))
}

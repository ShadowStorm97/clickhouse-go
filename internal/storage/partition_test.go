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
	bin1 := []*Bin{{[]*CompressBlock{
		{CompressionMethod: 1, CompressionSize: 10000, UnCompressedSize: 65536, CompressedData: []byte("1")},
		{CompressionMethod: 1, CompressionSize: 12000, UnCompressedSize: 65536, CompressedData: []byte("2")},
		{CompressionMethod: 1, CompressionSize: 8000, UnCompressedSize: 65536, CompressedData: []byte("3")},
	}}}
	p1.Bin = &BinContainer{BaseFilePointer: "/usr/clickhouse-go/" + p1.PartitionMeta.ToString() + "/", Bin: bin1}
	items1 := []*SparseIndexItem{{
		Val: 1,
		Mark: &MarkItem{
			Bin:                 bin1[0],
			CompressBlockOffset: 0,
			ExtractBlockOffset:  0,
		}},
	}
	p1.Primary = &PrimaryIndex{SparseIndexItem: items1}

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
	bin2 := []*Bin{{[]*CompressBlock{
		{CompressionMethod: 1, CompressionSize: 13000, UnCompressedSize: 65536, CompressedData: []byte("4")},
		{CompressionMethod: 1, CompressionSize: 15000, UnCompressedSize: 65536, CompressedData: []byte("5")},
		{CompressionMethod: 1, CompressionSize: 6000, UnCompressedSize: 65536, CompressedData: []byte("6")},
	}}}
	p2.Bin = &BinContainer{BaseFilePointer: "/usr/clickhouse-go/" + p2.PartitionMeta.ToString() + "/", Bin: bin2}
	items2 := []*SparseIndexItem{{
		Val: 4,
		Mark: &MarkItem{
			Bin:                 bin2[0],
			CompressBlockOffset: 1,
			ExtractBlockOffset:  0,
		}},
	}
	p2.Primary = &PrimaryIndex{SparseIndexItem: items2}
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
	Assert(t, len([]rune(p3.Bin.BaseFilePointer)) >= 34)
	AssertEqual(t, len(p3.Primary.SparseIndexItem), 2)
	AssertEqual(t, p3.Primary.SparseIndexItem[0].Val, 1)
	AssertEqual(t, p3.Primary.SparseIndexItem[1].Val, 4)
}

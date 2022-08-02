package storage

import "strconv"

type PartitionRawCount struct {
	partitionRawCount int64 //当前数据分区目录下数据的总行数
}

func (p *PartitionRawCount) MergeCount(p1 *PartitionRawCount, ) *PartitionRawCount {
	return &PartitionRawCount{
		partitionRawCount: p.partitionRawCount + p1.partitionRawCount,
	}
}

func (p *PartitionRawCount) ToString() string {
	return strconv.FormatInt(p.partitionRawCount, 10)
}

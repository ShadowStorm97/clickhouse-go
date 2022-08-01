package storage

import (
	"fmt"
	"github.com/clickhouse-go/math"
	"strconv"
	"time"
)

type Partition struct {
	//分区元信息
	PartitionMeta *PartitionMeta
	//分区内数据存储
	CheckSums string
	Columns   string
	Count     string
	Primary   Index
	Bin       []Bin
	Mark      []Mark
	Mark2     []Mark2
	Partition string
	SkipIndex SkipIndex
	SkipMark  SkipMark
}

type PartitionMeta struct {
	PartitionId int //分区ID 例: 20220801
	MinBlockNum int //分区内最小块号
	MaxBlockNum int //分区内最大块号
	Level       int //分区层级
}

func CreatePartitionMeta(minBlockNum, MaxBlockNum int) *PartitionMeta {
	now := time.Now()
	year, month, day := now.Format("2006"), now.Format("01"), now.Format("02")
	PartitionId, err := strconv.Atoi(year + month + day) //fixme this is a fixed implementation
	if err != nil {
		panic(err)
	}
	return &PartitionMeta{
		PartitionId: PartitionId,
		MinBlockNum: minBlockNum,
		MaxBlockNum: MaxBlockNum,
		Level:       0,
	}
}

func (pt *PartitionMeta) GetDirectorName() string {
	if pt == nil {
		return ""
	}
	return fmt.Sprintf("%d_%d_%d_%d",
		pt.PartitionId,
		pt.MinBlockNum,
		pt.MaxBlockNum,
		pt.Level)
}

func (pt *PartitionMeta) MergeDirectory(pt1 *PartitionMeta) (pt2 *PartitionMeta) {
	return &PartitionMeta{
		PartitionId: pt.PartitionId,
		MinBlockNum: math.IntMin(pt.MinBlockNum, pt1.MinBlockNum),
		MaxBlockNum: math.IntMax(pt.MaxBlockNum, pt1.MaxBlockNum),
		Level:       math.IntMax(pt.Level, pt1.Level) + 1,
	}
}

func (p *Partition) MergePartition(p1 *Partition) (p2 *Partition) {
	return &Partition{}
}

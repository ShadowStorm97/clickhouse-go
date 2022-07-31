package storage

type Partition struct {
	CheckSums string
	Columns string
	Count string
	Primary Index
	Bin []Bin
	Mark []Mark
	Mark2 []Mark2
	Partition string
	SkipIndex SkipIndex
	SkipMark SkipMark
}

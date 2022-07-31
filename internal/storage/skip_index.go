package storage

type SkipIndexItem struct {
	MinMax MinMax
}

type SkipIndex struct {
	IndexGranularity int
	SkipIndex []SkipIndexItem
}

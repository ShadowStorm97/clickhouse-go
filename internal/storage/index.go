package storage

// SparseIndexItem 稀疏索引item项
type SparseIndexItem struct {
	IndexVal struct{}
}

// Index 一级索引,稀疏索引
type Index struct {
	SparseIndex []SparseIndexItem
}

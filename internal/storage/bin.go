package storage

type BinContainer struct {
	BaseFilePointer string
	Bin             []*Bin
}

type Bin struct {
	CompressBlock []*CompressBlock
}

func (b *BinContainer) MergeBinContainer(b1 *BinContainer) *BinContainer {
	//todo 依赖mark merge
	return b
}

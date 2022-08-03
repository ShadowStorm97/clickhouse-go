package storage

type MarkItem struct {
	Bin                 *Bin  //数据指针,指向mark标记的bin对象
	CompressBlockOffset int32 //压缩文件中的偏移量(在哪个压缩块中)
	ExtractBlockOffset  int32 //解压缩块中的偏移量(在某个具体压缩块中,被解压后的偏移量)
}

type Mark struct {
	MarkItem []*MarkItem
}

type MarkContainer struct {
	Mark []*Mark
}

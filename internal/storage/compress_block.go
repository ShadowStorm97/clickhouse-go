package storage

const (
	minCompressBlockSize = 65536
	maxCompressBlockSize = 1048576
)


type CompressBlock struct {
	//头
	CompressionMethod uint8		//压缩方法
	CompressionSize   uint32	//数据压缩后字节大小
	UnCompressedSize  uint32	//数据压缩前字节大小
	//数据
	CompressedData	[]byte		//压缩后数据
}

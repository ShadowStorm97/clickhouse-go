package storage

type SparseIndexItem struct {
	Val  int       //todo 这里写死了主键类型是int,后面可以改成其他类型
	Mark *MarkItem //每个主键索引对应的mark对象的指针
}

// PrimaryIndex 一级索引,稀疏索引
type PrimaryIndex struct {
	SparseIndexItem []*SparseIndexItem
}

func (p *PrimaryIndex) MergePrimary(p1 *PrimaryIndex) *PrimaryIndex {
	return &PrimaryIndex{SparseIndexItem: Merge(p.SparseIndexItem, p1.SparseIndexItem)}
}

// Merge 归并排序核心,将两个有序数组做归并
func Merge(arr1, arr2 []*SparseIndexItem) []*SparseIndexItem {
	temp, i, j := make([]*SparseIndexItem, 0), 0, 0
	//如果两个数组都有值，则依次取首位做比较
	for len(arr1) > 0 && len(arr2) > 0 {
		if i >= len(arr1) || j >= len(arr2) {
			break
		}
		if arr1[i].Val > arr2[j].Val {
			temp = append(temp, arr2[j])
			j++
		} else {
			temp = append(temp, arr1[i])
			i++
		}
	}
	//程序执行到这里时,一定有一方数组被掏空了
	for i < len(arr1) {
		temp = append(temp, arr1[i])
		i++
	}
	for j < len(arr2) {
		temp = append(temp, arr2[j])
		j++
	}
	return temp
}

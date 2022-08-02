package storage

import (
	"strconv"
)

type Columns struct {
	formatVersion int           //格式化版本
	columns       []*ColumnItem //列信息
}

type ColumnItem struct {
	name       string //列名
	columnType string //列类型
}

func (c *Columns) ToString() (str string) {
	if c.formatVersion < 1 || c.columns == nil || len(c.columns) == 0 {
		return ""
	}
	str = "columns format version: " + strconv.Itoa(c.formatVersion) + "\n"
	str += strconv.Itoa(len(c.columns)) + " columns \n"
	for _, column := range c.columns {
		str += "`" + column.name + "` " + column.columnType + "\n"
	}
	return str
}

func (c *Columns) MergeColumns(c1 *Columns) *Columns {
	if c.columns == nil || len(c.columns) == 0 ||
		c1.columns == nil || len(c1.columns) == 0 ||
		len(c.columns) != len(c1.columns) ||
		diffColumnItems(unionColumnItems(c.columns, c1.columns), c.columns) {
		return &Columns{}
	}
	c1Set := make(map[string]string, 0)
	for _, column := range c.columns {
		c1Set[column.name] = column.columnType
	}
	for _, column := range c1.columns {
		if _, ok := c1Set[column.name]; !ok {
			return &Columns{}
		}
	}
	return c
}

func unionColumnItems(s1, s2 []*ColumnItem) []*ColumnItem {
	s3 := make([]*ColumnItem, 0, len(s1)+len(s2)+8)
	s3 = append(s1, s2...)
	s4 := make([]*ColumnItem, 0)
	set := make(map[string]struct{}, 0)
	for _, v1 := range s3 {
		if _, ok := set[v1.name+v1.columnType]; !ok {
			s4 = append(s4, v1)
			set[v1.name+v1.columnType] = struct{}{}
		}
	}
	return s4
}

func diffColumnItems(s1, s2 []*ColumnItem) bool {
	if s1 == nil || s2 == nil {
		return true
	}
	if len(s1) != len(s2) {
		return true
	}
	for i, v1 := range s1 {
		if v1.name != s2[i].name ||
			v1.columnType != s2[i].columnType {
			return true
		}
	}
	return false
}

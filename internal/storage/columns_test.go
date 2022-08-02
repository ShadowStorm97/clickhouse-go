package storage

import (
	. "github.com/huandu/go-assert"
	"testing"
)

func TestMergeColumnsOK(t *testing.T) {

	columns1 := &Columns{formatVersion: 1, columns: []*ColumnItem{
		{name: "id", columnType: "UInt32"},
		{name: "sku_id", columnType: "String"},
		{name: "total_amount", columnType: "Decimal(16,2)"},
		{name: "create_time", columnType: "DateTime"},
	}}

	columns2 := &Columns{formatVersion: 1, columns: []*ColumnItem{
		{name: "id", columnType: "UInt32"},
		{name: "sku_id", columnType: "String"},
		{name: "total_amount", columnType: "Decimal(16,2)"},
		{name: "create_time", columnType: "DateTime"},
	}}

	columns := columns1.MergeColumns(columns2)

	AssertNotEqual(t, columns, nil)
	AssertEqual(t, columns.formatVersion, 1)
	AssertNotEqual(t, columns.columns, nil)
	AssertEqual(t, len(columns.columns), 4)
	AssertEqual(t, columns.ToString(),
		"columns format version: 1\n"+
			"4 columns \n"+
			"`id` UInt32\n"+
			"`sku_id` String\n"+
			"`total_amount` Decimal(16,2)\n"+
			"`create_time` DateTime\n")

}

func TestMergeColumnsFail(t *testing.T) {

	columns1 := &Columns{formatVersion: 1, columns: []*ColumnItem{
		{name: "id", columnType: "UInt32"},
		{name: "sku_id", columnType: "String"},
		{name: "total_amount", columnType: "Decimal(16,2)"},
		{name: "create_time", columnType: "DateTime"},
	}}

	columns2 := &Columns{formatVersion: 1, columns: []*ColumnItem{
		{name: "id", columnType: "UInt32"},
		{name: "sku_id", columnType: "String"},
		{name: "total_amount", columnType: "Decimal(16,2)"},
		{name: "hello", columnType: "DateTime"},
	}}

	columnsFail1 := columns1.MergeColumns(columns2)
	AssertNotEqual(t, columnsFail1, nil)
	AssertEqual(t, columnsFail1.formatVersion, 0)
	AssertEqual(t, columnsFail1.columns, nil)
	AssertEqual(t, columnsFail1.ToString(), "")

	columns3 := &Columns{formatVersion: 1, columns: []*ColumnItem{
		{name: "id", columnType: "UInt32"},
		{name: "sku_id", columnType: "String"},
		{name: "total_amount", columnType: "Decimal(16,2)"},
		{name: "create_time", columnType: "DateTime"},
	}}

	columns4 := &Columns{formatVersion: 1, columns: []*ColumnItem{
		{name: "id", columnType: "UInt32"},
		{name: "sku_id", columnType: "Int32"},
		{name: "total_amount", columnType: "Decimal(16,2)"},
		{name: "create_time", columnType: "DateTime"},
	}}

	columnsFail2 := columns3.MergeColumns(columns4)
	AssertNotEqual(t, columnsFail2, nil)
	AssertEqual(t, columnsFail2.formatVersion, 0)
	AssertEqual(t, columnsFail2.columns, nil)
	AssertEqual(t, columnsFail2.ToString(), "")
}
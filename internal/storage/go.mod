module storage

go 1.14

replace (
	github.com/clickhouse-go/math => ../math
)

require (
	github.com/clickhouse-go/math v0.0.0-00010101000000-000000000000
	github.com/huandu/go-assert v1.1.5
)



## 编译测试
```
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrcoordinator.go pg-*.txt

go run mrworker.go wc.so
```
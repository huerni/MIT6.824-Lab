## Lab3A

### 没有网络故障和Leader替换


### 存在网络故障的情况下

如果领导者在将条目提交到 Raft 日志后失败，则 Clerk 可能不会收到回复，因此可能会将请求重新发送到另一个领导者。每次调用 Clerk.Put() 或 Clerk.Append() 应该只有一次执行，因此你需要确保重新发送不会导致服务器执行请求两次。  

您的解决方案需要处理一个已经调用了Start()来处理Clerk的RPC的leader，在请求被提交到日志之前失去了领导权的情况。  

**一种方法是让服务器检测它是否已经失去了领导地位，通过注意到在Start()返回的索引处出现了不同的请求，或者Raft的term已经改变。**  
> Your scheme for duplicate detection should free server memory quickly, for example by having each RPC imply that the client has seen the reply for its previous RPC. It's OK to assume that a client will make only one call into a Clerk at a time.   




客户端怎么找到Leader  
1. 不断的随机，直到找到leader
2. 当客户端请求non-Leader时，该结点提供Leader的信息，使客户端转向Leader请求

如何避免客户端查询到过期信息  
1. Leader在选举成功时提交一个non-Op的日志，确保所有条目已经提交
> 判断leader是否是刚选举，如果刚选举，将之前已经提交的所有msg从chan中读出  
2. Leader在返回客户端只读操作结果时，需要先向追随者发送心跳信息，已确认自己的领导地位(即超过半数返回心跳)  

在不稳定的情况下，怎么避免服务器重复接收客户端的同一请求  
1. 给每一次操作赋予一个独立的序列号，来唯一标识每一次操作  
我的做法是服务端用map保存每一次的操作和其结果，在每次请求raft前判断map中是否已有序列号，如果有，直接返回

Leader切换时，msg怎么处理？？

## submit
```
go test -run 3A
```
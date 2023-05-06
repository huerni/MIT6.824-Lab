## lab2A 领导人选举
首选服务启动时判断是否需要重新选举：  
1. 追随者阶段  在指定时间内是否接收到leader的心跳
    - 没有接收，开始进行选举，身份转为候选者  
    - 接收到了，继续担任追随者，继续接收  
2. 候选者阶段  给自己投票，然后通知其他人
    - 赢得选举。率先给其他人发心跳检测，防止其他人再进行选举  
    - 输了选举。别人动作快，先发了心跳检测，并且任期一致，于是承认别人是领导者，我变为追随者；否则，拒收心跳，我还有资格竞选，仍然是候选者  
    - 没输没赢。选票平分，没有票数高的人。该任期没有领导者，所有候选者直接进入下一任期的选举。但这可能会造成一个问题，就是大家势均力敌，一直没有领导者出现，导致选举无限期执行下去。
    > 解决办法是使每位候选者的选举超时时间随机，可以理解为每个人的能力随机，这样能力强的自然称为领导者。Raft论文有讲述最开始用的是排名方法，排名高的一定能比排名低的称为领导者，但如果排名高的服务器很容易出现故障，可能造成频繁重新进行选举。得出结论还是随机方法优势更明显。  
3. 领导者阶段  空闲时间不断向其他服务器发送心跳检测，巩固自己的领导地位

## lab2B  日志复制
选举完Leader后，客户端发送命令给Leader  
> Leader选举后，需要初始化所有服务器的nextIndex和matchIndex，为发送日志做准备  
1. Leader首先添加进自己的logs中，然后向每个Follower并行发送AppendEntry rpc 复制该条目  
2. 当超过半数Follower成功复制时，进行提交  

commit与applied  
如果在大多数服务器在同步成功，则将其提交。提交后即持久化，代表将其用于状态机是安全的。所以是先提交，然后再将其用于状态机。  

不能直接截断，可能会接收到过时的AppendEntries RPC，此时follower的日志多于args的日志
> The if here is crucial. If the follower has all the entries the leader sent, the follower MUST NOT truncate its log. Any elements following the entries sent by the leader MUST be kept. This is because we could be receiving an outdated AppendEntries RPC from the leader, and truncating the log would mean “taking back” entries that we may have already told the leader that we have in our log.

## lab2C 日志持久化


## lab2C 日志压缩


## 测试
```
go test -race
go test -run 2A
go test -run 2B
go test -run 2C
go test -run 2D
for i in {0..10}; do go test -run 2B; done
```

## 参考资料
https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md
http://thesecretlivesofdata.com/raft/
https://juejin.cn/post/6844903665275404295
https://knowledge-sharing.gitbooks.io/raft/content/chapter5.html
https://thesquareplanet.com/blog/students-guide-to-raft/
https://gist.github.com/Zrealshadow/9e4a8e213bb9eca5b5ce2e985b396f7c
https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/
https://www.codercto.com/a/81179.html

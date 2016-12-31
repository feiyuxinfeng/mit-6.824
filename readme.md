# Mit6.824
[![Build Status](https://travis-ci.org/yuyang0/mit-6.824.svg?branch=master)](https://travis-ci.org/yuyang0/mit-6.824)

## lab1 MapReduce
## lab2 Raft

   1. RPC需要指定超时时间,leader要有机制来跟踪当前的各peer的状态，如果发现peer超
      时，那么就将该peer标记为`fail`,在确定leader是不是有效的时候，不仅要检查
      `state`,还要检查处于`fail`状态的peer是否超过了半数。
   2. 因为RPC是一个阻塞操作,为了性能所以没有加锁,因此每个RPC完成后,都要检查当前
      的term,state是否和发起rpc的时候一致,并根据结果做相应的处理.
   3. 每一个Term都应该重新初始化election timeout, 这样可以避免无法选主的场景,eg:
      3台机器,挂了一台,剩下A和B, B的日志比A的新,A的election timeout比B的小, 这种
      情况如果不每一个term都重新选择election timeout的话,那基本上不可能完成选主.
      这是一个test中遇到的场景.
   4. 日志数组的第一个元素会被忽略,因为go的slice从0开始,但是raft的paper要求index
      从1开始.但是这个被忽略的元素不能为nil,这主要是为了应付gob
   5. 向applyCh写入的时候,不要使用异步,因为那样会打乱顺序
   6. `AppendEntries`中不能删掉index比 `args.PrevLogIndex + len(args.Entries)`
      大的日志项, 因为这些日志项很可能是携带了更多日志的RPC创建的,因为网络等的原
      因,这些RPC可能会先于当前RPC到达. **总之，必须保证RPC是幂等的**,但是有一种
      情况例外：那就是如果发现日志冲突，那么就一定要删掉 `args.PrevLogIndex +
      len(args.Entries)`之后的日志, 因为出现了冲突，那么肯定这些日志不是当前的
      `leader`产生。
   7. 在将日志apply的时候必须要检查， `args.PrevLogIndex+len(rf.Entries)` 与
      `args.LeaderCommit` 的值，你只能apply二者中的较小值，原因是：如果follower
      有一个不一致的日志项，这个日志项已经被集群commit，但是heartbeat比携带正确
      日志的RPC先到达，那么你不检查这个条件的话就会将这个不一致的日志apply。

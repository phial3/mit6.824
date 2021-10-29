# mit6.824

## 课程简介
mit的分布式课程，主要分两个部分。lab1是实现map reduce。2,3,4都是raft的实现课程，从实现一个简单的raft的选举和日志复制到实现一个数据分片的分布式的kv存储。
课程设计非常好(当然这也归功于raft的模块化设计),由浅入深，非常棒。
[课程地址](https://pdos.csail.mit.edu/6.824/)

## 相关资料
[raft-extend](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)

论文原文的简化版，字字珠玑，建议大家一定一定要反复研读(我自己反复看了不下5遍)，特别figure2，非常严谨，要通过实现基本是要完全按照figure2的写法才能实现，没有任何支线可走。

[students-guide](https://thesquareplanet.com/blog/students-guide-to-raft/)

MIT的实验指导，一些实验过程中的坑基本都有提到，非常实用

[线性一致性和 Raft](https://segmentfault.com/a/1190000016762033)

LAB3就会涉及到线性一致性的topic，这对于理解整个分布式也是至关重要的。另外关于read log的其他优化



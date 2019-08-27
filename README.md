# Distributed TCC Transaction Framework - micro-tcc (1.3.5.RELEASE)

[![Maven](https://img.shields.io/badge/endpoint.svg?url=https://github.com/mytcctransaction/micro-tcc)](https://github.com/mytcctransaction/micro-tcc)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mytcctransaction/micro-tcc/master/LICENSE)

## Modules
1. micro-tcc-tc: *Distributed Transaction Client*
2. micro-tcc-common: *Commons*   
## Summary
    micro-tcc 是基于Zookeeper（协调者）+Redis 分布式事务中间件，支持SpringCloud、Dubbo、RestTemplate
    micro-tcc 支持事务同步和异步调用方式，发生异常的事务会定时自动恢复，如果超过最大恢复次数，建议手动恢复
    Zookeeper 作为分布式事务协调者，它负责协调各个子系统的事务状态和事务确认、提交、回滚
    redis 作为事务的存储方式

## The Authority
Website: [https://github.com/mytcctransaction/micro-tcc](https://github.com/mytcctransaction/micro-tcc)  
Statistics: [Leave your company messages](https://github.com/mytcctransaction/micro-tcc)  
QQ 群：246539015 (Hot) 
作者 QQ:306750639
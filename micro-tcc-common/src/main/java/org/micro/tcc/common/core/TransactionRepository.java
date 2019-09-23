package org.micro.tcc.common.core;


import java.util.Date;
import java.util.List;

/**
*@author jeff.liu
*@desc   事务日志接口
*@date 2019/8/27
*/
public interface TransactionRepository {


    /**
     * 创建事务组
     * @param transaction
     * @return
     */
    boolean createGroupMemberForClientCancel(Transaction transaction);


    boolean createRollbackOrder(Transaction transaction);

    /**
     * 创建事务组
     * @param transaction
     * @return
     */
    boolean createGroupMember(Transaction transaction);

    /**
     * 事务是否在取消中
     * @param transaction
     * @return
     */
    boolean isCanceling(Transaction transaction);

    boolean createConfirmOrder(Transaction transaction);

    boolean update(Transaction transaction);

    /**
     * 删除事务
     * @param transaction
     * @return
     */
    boolean delete(Transaction transaction);

    /**
     * 判断幂等性
     * @param transaction
     * @return
     */
    boolean judgeIdempotent(Transaction transaction);



    /**
     * 查找事务by id
      * @param xid
     * @return
     */
    Transaction findByGroupId(TransactionXid xid);

    /**
     * 分布式锁加锁
     * @param lockKey
     * @param requestId
     * @param expireTime
     * @return
     */
    boolean tryGetDistributedLock(String lockKey, String requestId, long expireTime);

    /**
     * 释放分布式锁
     * @param lockKey
     * @param requestId
     * @return
     */
    boolean releaseDistributedLock(String lockKey, String requestId);

    /**
     * 查找事务
     * @param gid
     * @return
     */
    Transaction findByGroupId(String gid);

    /**
     * 查找所有事务
     * @return
     */
    List<String> findTransactionGroupAll();

    /**
     * 查找所有事务
     * @param date
     * @return
     */
    List<Transaction> findAll(Date date);
}

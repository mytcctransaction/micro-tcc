package org.micro.tcc.common.core;


import java.util.Date;
import java.util.List;

/**
*@author jeff.liu
*@desc   事务日志接口
*@date 2019/8/27
*/
public interface TransactionRepository {

    boolean create(Transaction transaction);

    boolean createGroupMemberForCancel(Transaction transaction);

    boolean createGroupMember(Transaction transaction);

    boolean update(Transaction transaction);

    boolean updateGroupTransaction(Transaction transaction);

    boolean delete(Transaction transaction);

    boolean deleteGroup(Transaction transaction);

    Transaction findByGroupId(TransactionXid xid);

    Transaction findByGroupId(String gid);

    List<String> findTransactionGroupAll();

    List<Transaction> findAll(Date date);
}

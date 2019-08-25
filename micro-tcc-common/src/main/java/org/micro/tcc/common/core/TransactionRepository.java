package org.micro.tcc.common.core;


import java.util.Date;
import java.util.List;


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

    List<Transaction> findAll(Date date);
}

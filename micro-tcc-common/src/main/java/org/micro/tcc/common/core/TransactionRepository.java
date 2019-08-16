package org.micro.tcc.common.core;


import java.util.Date;
import java.util.List;


public interface TransactionRepository {

    int create(Transaction transaction);

    int update(Transaction transaction);

    int delete(Transaction transaction);

    Transaction findByGroupId(TransactionXid xid);

    Transaction findByGroupId(String gid);

    List<Transaction> findAll(Date date);
}

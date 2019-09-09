package org.micro.tcc.common.core;



import org.micro.tcc.common.constant.Model;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.constant.TransactionType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
*@author jeff.liu
*@desc   分布式事务
*@date 2019/8/27
*/
public class Transaction implements Serializable {

    private static final long serialVersionUID = 776032890445193092L;

    private boolean asyncConfirm =false;

    private boolean asyncCancel=false;

    private int model= Model.AT.value();

    private Class rollbackFor=Throwable.class;


    /**
     * 全局id
     */
    private TransactionXid xid;

    /**
     * 事务状态
     */
    private TransactionStatus status;

    /**
     * 事务类型
     */
    private TransactionType transactionType;

    /**
     * 重试次数
     */
    private volatile int retriedCount = 0;

    private Date createTime = new Date();

    private Date lastUpdateTime = new Date();

    private long version = 1;

    /**
     * 参与者
     */
    private List<TransactionMember> participants = new ArrayList<TransactionMember>();

    private Map<String, Object> attachments = new ConcurrentHashMap<String, Object>();

    public Transaction() {

    }

    public Transaction(TransactionXid xid,TransactionStatus status) {
        this.xid=xid;
        this.status=status;
    }


    public Transaction(TccTransactionContext transactionContext) {
        this.xid = transactionContext.getXid();
        this.status = TransactionStatus.TRY;
        this.transactionType = TransactionType.BRANCH;
    }

    public Transaction(TransactionType transactionType) {
        this.xid = new TransactionXid();
        this.status = TransactionStatus.TRY;
        this.transactionType = transactionType;
    }

    public Transaction(Object uniqueIdentity,TransactionType transactionType) {

        this.xid = new TransactionXid(uniqueIdentity);
        this.status = TransactionStatus.TRY;
        this.transactionType = transactionType;
    }

    public void addingParticipants(TransactionMember participant) {
        participants.add(participant);
    }

    public int getModel() {
        return model;
    }


    public Class getRollbackFor() {
        return rollbackFor;
    }

    public void setRollbackFor(Class rollbackFor) {
        this.rollbackFor = rollbackFor;
    }
    public void setModel(int model) {
        this.model = model;
    }
    public TransactionXid getTransactionXid() {
        return xid;
    }
    public TransactionStatus getStatus() {
        return status;
    }

    public boolean getAsyncConfirm() {
        return asyncConfirm;
    }

    public void setAsyncConfirm(boolean asyncConfirm) {
        this.asyncConfirm = asyncConfirm;
    }

    public boolean getAsyncCancel() {
        return asyncCancel;
    }

    public void setAsyncCancel(boolean asyncCancel) {
        this.asyncCancel = asyncCancel;
    }

    public List<TransactionMember> getParticipants() {
        return participants;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void changeStatus(TransactionStatus status) {
        this.status = status;
    }


    public void commit() {

        for (TransactionMember participant : participants) {
            participant.commit();
        }
    }

    public void rollback() {
        for (TransactionMember participant : participants) {
            participant.rollback();
        }
    }

    public int getRetriedCount() {
        return retriedCount;
    }

    public void addRetriedCount() {
        this.retriedCount++;
    }

    public void resetRetriedCount(int retriedCount) {
        this.retriedCount = retriedCount;
    }

    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        this.version++;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date date) {
        this.lastUpdateTime = date;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void updateTime() {
        this.lastUpdateTime = new Date();
    }


}

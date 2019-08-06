package org.micro.tcc.common.core;



import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.constant.TransactionType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Transaction implements Serializable {


    private static final long serialVersionUID = 776032890445193092L;
    private String asyncConfirm="false";

    private String asyncCancel="false";

    private TransactionGid xid;

    private TransactionStatus status;

    private TransactionType transactionType;

    private volatile int retriedCount = 0;

    private Date createTime = new Date();

    private Date lastUpdateTime = new Date();

    private long version = 1;

    private List<TransactionMember> participants = new ArrayList<TransactionMember>();

    private Map<String, Object> attachments = new ConcurrentHashMap<String, Object>();

    public Transaction() {

    }

    public Transaction(TccTransactionContext transactionContext) {
        this.xid = transactionContext.getXid();
        this.status = TransactionStatus.TRY;
        this.transactionType = TransactionType.BRANCH;
    }

    public Transaction(TransactionType transactionType) {
        this.xid = new TransactionGid();
        this.status = TransactionStatus.TRY;
        this.transactionType = transactionType;
    }

    public Transaction(Object uniqueIdentity,TransactionType transactionType) {

        this.xid = new TransactionGid(uniqueIdentity);
        this.status = TransactionStatus.TRY;
        this.transactionType = transactionType;
    }

    public void enlistParticipant(TransactionMember participant) {
        participants.add(participant);
    }


//    public Xid getXid() {
//        return xid.clone();
//    }

    public TransactionGid getTransactionXid() {
        return xid;
    }
    public TransactionStatus getStatus() {
        return status;
    }

    public String getAsyncConfirm() {
        return asyncConfirm;
    }

    public void setAsyncConfirm(String asyncConfirm) {
        this.asyncConfirm = asyncConfirm;
    }

    public String getAsyncCancel() {
        return asyncCancel;
    }

    public void setAsyncCancel(String asyncCancel) {
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

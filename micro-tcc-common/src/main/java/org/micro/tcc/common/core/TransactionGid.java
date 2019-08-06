package org.micro.tcc.common.core;


import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class TransactionGid implements Serializable {


    private static final long serialVersionUID = 5549076002296308508L;
    private int formatId = 1;

    private String globalTccTransactionId;




    /**
     * 生成全局唯一id
     */
    public TransactionGid() {
        String gid=UUID.randomUUID().toString();
        globalTccTransactionId=gid;

    }
    public String getGlobalTccTransactionId() {
        return globalTccTransactionId;
    }

    public void setGlobalTccTransactionId(String globalTccTransactionId) {
        this.globalTccTransactionId = globalTccTransactionId;
    }


    public TransactionGid(Object uniqueIdentity) {
        UUID branchUuid = null;
        if (uniqueIdentity instanceof UUID) {
            branchUuid = (UUID) uniqueIdentity;
        } else {
            try {
                branchUuid = UUID.fromString(uniqueIdentity.toString());
            } catch (IllegalArgumentException e) {

                byte[] bytes = uniqueIdentity.toString().getBytes();

                if (bytes.length > 16) {
                    throw new IllegalArgumentException("UniqueIdentify is illegal, the value is :" + uniqueIdentity.toString());
                }

                branchUuid = UUID.nameUUIDFromBytes(bytes);
            }
        }


    }


    public TransactionGid(String globalTransactionId) {
        this.globalTccTransactionId = globalTransactionId;

    }
    public TransactionGid(String globalTransactionId, byte[] branchQualifier) {
        this.globalTccTransactionId = globalTransactionId;

    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(globalTccTransactionId);

        return stringBuilder.toString();
    }

}



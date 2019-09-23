package org.micro.tcc.tc.util;


import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.exception.TccSystemErrorException;
import org.micro.tcc.common.serializer.ObjectSerializer;
import org.micro.tcc.common.util.ByteUtils;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 *@author jeff.liu
 *   事务序列化工具
 * date 2019/7/31
 */
public class TransactionSerializer {

    public static Map<byte[], byte[]> serialize(ObjectSerializer serializer, Transaction transaction) {

        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();

        map.put("global_transaction_id".getBytes(), transaction.getTransactionXid().getGlobalTccTransactionId().getBytes());
      /*  map.put("ASYNC_CONFIRM".getBytes(), transaction.getAsyncConfirm().getBytes());
        map.put("ASYNC_CANCEL".getBytes(), transaction.getAsyncCancel().getBytes());*/
        map.put("status".getBytes(), ByteUtils.intToBytes(transaction.getStatus().value()));
        map.put("transaction_type".getBytes(), ByteUtils.intToBytes(transaction.getTransactionType().value()));
        map.put("retry_count".getBytes(), ByteUtils.intToBytes(transaction.getRetriedCount()));
        map.put("create_time".getBytes(), DateFormatUtils.format(transaction.getCreateTime(), "yyyy-MM-dd HH:mm:ss").getBytes());
        map.put("update_time".getBytes(), DateFormatUtils.format(transaction.getLastUpdateTime(), "yyyy-MM-dd HH:mm:ss").getBytes());
        map.put("version".getBytes(), ByteUtils.longToBytes(transaction.getVersion()));
        map.put("content".getBytes(), serializer.serialize(transaction));
        map.put("content_text".getBytes(), JSON.toJSONString(transaction).getBytes());
        return map;
    }

    public static Transaction deserialize(ObjectSerializer serializer, Map<byte[], byte[]> map1) {

        Map<String, byte[]> propertyMap = new HashMap<String, byte[]>();

        for (Map.Entry<byte[], byte[]> entry : map1.entrySet()) {
            propertyMap.put(new String(entry.getKey()), entry.getValue());
        }

        byte[] content = propertyMap.get("content");
        Transaction transaction = (Transaction) serializer.deserialize(content);
        transaction.changeStatus(TransactionStatus.valueOf(ByteUtils.bytesToInt(propertyMap.get("status"))));
        transaction.resetRetriedCount(ByteUtils.bytesToInt(propertyMap.get("retry_count")));

        try {
            transaction.setLastUpdateTime(DateUtils.parseDate(new String(propertyMap.get("update_time")), "yyyy-MM-dd HH:mm:ss"));
        } catch (ParseException e) {
            throw new TccSystemErrorException(e);
        }
        transaction.setVersion(ByteUtils.bytesToLong(propertyMap.get("version")));
        return transaction;
    }

}

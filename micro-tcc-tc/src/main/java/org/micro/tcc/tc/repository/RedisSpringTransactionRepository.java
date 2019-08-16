package org.micro.tcc.tc.repository;

import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.core.TransactionRepository;
import org.micro.tcc.common.core.TransactionXid;
import org.micro.tcc.common.serializer.KryoPoolSerializer;
import org.micro.tcc.common.serializer.ObjectSerializer;
import org.micro.tcc.tc.util.TransactionSerializer;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.micro.tcc.tc.component.SpringContextAware;

import java.io.IOException;
import java.util.*;

/**
 *@author jeff.liu
 *   描述
 * date 2019/7/31
 */
@Slf4j
public class RedisSpringTransactionRepository implements TransactionRepository {


    private RedisTemplate redisTemplate;

    public static RedisSpringTransactionRepository redisSpringTransactionRepository=new RedisSpringTransactionRepository();

    private int reCoverCount=1000;
    public static RedisSpringTransactionRepository getInstance(){
        return redisSpringTransactionRepository;
    }
    public RedisSpringTransactionRepository(){
        if(redisTemplate==null)
        redisTemplate= (RedisTemplate)SpringContextAware.getBean("redisTemplate");
    }
    private ObjectSerializer serializer = new KryoPoolSerializer();

    private int converResult(boolean b){
        if(b)return 1;
        return 0;
    }
    @Override
    public List<Transaction> findAll(Date date) {
        return pageList(Constant.getTransactionMapKey());
    }
    public List<Transaction> pageList(String key) {
        List<Transaction> list=new ArrayList();
        Cursor<Map.Entry<String, Map<byte[], byte[]>>> cursor=null;
        try{
            cursor = redisTemplate.opsForHash().scan(key, ScanOptions.scanOptions().match("*").count(reCoverCount).build());
            while (cursor.hasNext()) {
                Map.Entry<String, Map<byte[], byte[]>> next= cursor.next();
                Map<byte[], byte[]> value = next.getValue();
                Transaction transaction= TransactionSerializer.deserialize(serializer,value);
                list.add(transaction);
            }
        }catch (Throwable e){
            log.error("TCC:redis 游标操作异常",e);
        }finally{
            if(null!=cursor){
                try {
                    cursor.close();
                } catch (IOException e) {
                    log.error(e.getMessage(),e);
                }
            }

        }

        return list;

    }
    private String getTransactionXid(Transaction transaction){
        String transactionXid=transaction.getTransactionXid().getGlobalTccTransactionId();
        return transactionXid;
    }
    @Override
    public int create(Transaction transaction) {
        Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        boolean b=redisTemplate.opsForHash().putIfAbsent(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);
        return converResult(b);

    }

    @Override
    public int update(Transaction transaction) {
        Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);

        redisTemplate.opsForHash().put(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);
      /*  Map<byte[], byte[]> transMap2=(Map<byte[], byte[]> )redisTemplate.opsForHash().get(Constant.getTransactionMapKey(),getTransactionXid(transaction));
        Transaction transaction2= TransactionSerializer.deserialize(serializer,transMap2);*/

        return converResult(true);
    }

    @Override
    public int delete(Transaction transaction) {
        //Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        redisTemplate.opsForHash().delete(Constant.getTransactionMapKey(),getTransactionXid(transaction));
        return converResult(true);
    }

    @Override
    public Transaction findByGroupId(TransactionXid xid) {
        //String transactionXid=UUID.nameUUIDFromBytes(xid.getGlobalTransactionId()).toString();
        String transactionXid=xid.getGlobalTccTransactionId();
        Map<byte[], byte[]> transMap=(Map<byte[], byte[]>)redisTemplate.opsForHash().get(Constant.getTransactionMapKey(),transactionXid);
        Transaction transaction= TransactionSerializer.deserialize(serializer,transMap);
        return transaction;
    }

    @Override
    public Transaction findByGroupId(String gid) {

        Map<byte[], byte[]> transMap=(Map<byte[], byte[]>)redisTemplate.opsForHash().get(Constant.getTransactionMapKey(),gid);
        if (null != transMap) {
          Transaction transaction = TransactionSerializer.deserialize(serializer, transMap);
          return transaction;
        }else{
            return null;
        }
    }


}

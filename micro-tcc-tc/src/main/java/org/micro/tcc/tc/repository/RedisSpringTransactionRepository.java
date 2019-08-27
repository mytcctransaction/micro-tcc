package org.micro.tcc.tc.repository;

import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.core.TransactionRepository;
import org.micro.tcc.common.core.TransactionXid;
import org.micro.tcc.common.serializer.KryoPoolSerializer;
import org.micro.tcc.common.serializer.ObjectSerializer;
import org.micro.tcc.common.util.ByteUtils;
import org.micro.tcc.common.util.CoordinatorUtils;
import org.micro.tcc.tc.util.TransactionSerializer;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.*;
import org.micro.tcc.tc.component.SpringContextAware;

import java.io.IOException;
import java.util.*;

/**
 *@author jeff.liu
 *   redis 分布式事务日志保存，用于异常恢复
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
        if(redisTemplate==null){
            redisTemplate= (RedisTemplate)SpringContextAware.getBean("redisTemplate");
        }

    }
    private ObjectSerializer serializer = new KryoPoolSerializer();


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
    public boolean create(Transaction transaction) {
        createGroupMember(transaction);
        Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        boolean b=redisTemplate.opsForHash().putIfAbsent(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);

        return true;

    }

    @Override
    public boolean createGroupMemberForCancel(Transaction transaction) {
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        int pageNo=20;
        Set<String> appNameList=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
        for(String appName:appNameList){
            if(appName.indexOf(Constant.CONFIRM)!=-1){
                String key=CoordinatorUtils.getAppName(appName);
                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CANCEL,2);
            }

        }
      return true;


    }

    @Override
    public boolean createGroupMember(Transaction transaction) {
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.CONFIRM,1);
        return true;
    }

    @Override
    public boolean updateGroupTransaction(Transaction transaction) {
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        //Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
       /* RedisCallback<?> redisCallback =new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.listCommands().lRange()
                return null;
            }
        };
        redisTemplate.executePipelined(redisCallback);*/

        int pageNo=20;
        List<String> appNameList=redisTemplate.opsForList().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
        for(String appName:appNameList){
            Map<byte[], byte[]> transMap=(Map<byte[], byte[]> )redisTemplate.opsForHash().get(Constant.prefix+appName,groupId);
            Transaction transactionTemp=TransactionSerializer.deserialize(serializer,transMap);
            Map<byte[], byte[]> transMapTemp=TransactionSerializer.serialize(serializer,transactionTemp);
            redisTemplate.opsForHash().put(Constant.prefix+appName,getTransactionXid(transaction),transMapTemp);

        }
        return true;
    }



    @Override
    public boolean update(Transaction transaction) {
        Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        redisTemplate.opsForHash().put(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);
      /*  Map<byte[], byte[]> transMap2=(Map<byte[], byte[]> )redisTemplate.opsForHash().get(Constant.getTransactionMapKey(),getTransactionXid(transaction));
        Transaction transaction2= TransactionSerializer.deserialize(serializer,transMap2);*/

        return true;
    }

    @Override
    public boolean delete(Transaction transaction) {
        //Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        redisTemplate.opsForHash().delete(Constant.getTransactionMapKey(),getTransactionXid(transaction));
        switch (TransactionStatus.valueOf(transaction.getStatus().value())){
            case TRY:
                break;
            case CONFIRM:
                redisTemplate.opsForZSet().remove(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.CONFIRM);
                break;
            case CANCEL:
                redisTemplate.opsForZSet().remove(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.CONFIRM);
                redisTemplate.opsForZSet().remove(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.CANCEL);
                break;
            default:
                break;
        }

        return true;
    }

    @Override
    public boolean deleteGroup(Transaction transaction) {
        return true;
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

    @Override
    public List<String> findTransactionGroupAll() {

        RedisCallback<?> redisCallback =new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.scan(ScanOptions.scanOptions().match("*").count(reCoverCount).build());
                return null;
            }
        };
        redisTemplate.execute(redisCallback);
        return null;
    }


}

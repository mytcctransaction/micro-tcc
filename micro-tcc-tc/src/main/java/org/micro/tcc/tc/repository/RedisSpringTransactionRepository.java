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
 *   1，如果事务组处于TRY状态，说明TRY过程没有成功，事务会本地事务回滚，不用处理
 *   2，如果事务组处于CONFIRM，CANCEL 状态，事务定时恢复
 * date 2019/7/31
 */
@Slf4j
public class RedisSpringTransactionRepository implements TransactionRepository {


    private RedisTemplate redisTemplate;

    int pageNo=100;

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

    /**
     * 创建取消日志by用户手动取消
     * @param transaction
     * @return
     */
    @Override
    public boolean createGroupMemberForClientCancel(Transaction transaction) {
      return createGroupMemberForCancel(transaction);
    }

    /**
     * 创建取消日志by系统异常取消
     * @param transaction
     * @return
     */
    @Override
    public boolean createGroupMemberForCancel(Transaction transaction) {
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        Set<String> appNameList=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
        for(String appName:appNameList){
            if(appName.indexOf(Constant.TRY)!=-1 || appName.indexOf(Constant.CONFIRM)!=-1){
                String key=CoordinatorUtils.getAppName(appName);
                Double score=redisTemplate.opsForZSet().score(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CANCEL);
                if(null!=score){
                    double b=score.doubleValue();
                    if(b!=2){
                        redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CANCEL,2);
                    }
                }else {
                    redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CANCEL,2);
                }

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
        List<String> appNameList=redisTemplate.opsForList().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
        for(String appName:appNameList){
            Map<byte[], byte[]> transMap=(Map<byte[], byte[]> )redisTemplate.opsForHash().get(Constant.prefix+appName,groupId);
            Transaction transactionTemp=TransactionSerializer.deserialize(serializer,transMap);
            Map<byte[], byte[]> transMapTemp=TransactionSerializer.serialize(serializer,transactionTemp);
            redisTemplate.opsForHash().put(Constant.prefix+appName,getTransactionXid(transaction),transMapTemp);

        }
        return true;
    }

    /**
     * 判断事务是否在取消中
     * @param transaction
     * @return
     */
    @Override
    public boolean isCanceling(Transaction transaction){
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        Set<String> appNameList=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
        boolean isCanceling=false;
        for(String appName:appNameList){
            if(appName.indexOf(Constant.DELIMIT+Constant.CANCEL)!=-1){
                isCanceling= true;
                break;
            }

        }

        return isCanceling;
    }

    @Override
    public boolean update(Transaction transaction) {
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        switch (TransactionStatus.valueOf(transaction.getStatus().value())){
            case TRY:
                redisTemplate.opsForHash().put(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);
                break;
            case CONFIRM:
                Set<String> appNameList=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
                for(String appName:appNameList){
                    if(appName.indexOf(Constant.getAppName()+Constant.DELIMIT+Constant.TRY)!=-1||appName.indexOf(Constant.getAppName()+Constant.DELIMIT+Constant.CONFIRM)!=-1){
                        redisTemplate.opsForHash().put(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);
                        break;
                    }
                }
                break;
            case CANCEL:
                Set<String> appNameListCancel=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
                for(String appName:appNameListCancel){
                    if(appName.indexOf(Constant.getAppName()+Constant.DELIMIT+Constant.CANCEL)!=-1){
                        redisTemplate.opsForHash().put(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);
                        break;
                    }
                }
                break;
            default:
                break;
        }
        return true;
    }

    @Override
    public boolean delete(Transaction transaction) {
        //Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        redisTemplate.opsForHash().delete(Constant.getTransactionMapKey(),getTransactionXid(transaction));
        createTransactionLog(transaction);
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

    public void createTransactionLog(Transaction transaction){
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        String key="";
        switch (TransactionStatus.valueOf(transaction.getStatus().value())){
            case TRY:
                break;
            case CONFIRM:
                key=getHashMapLogkey(groupId,Constant.CONFIRM);
                redisTemplate.opsForHash().putIfAbsent(Constant.TRANSACTION_LOG,key,transMap);
                break;
            case CANCEL:
                key=getHashMapLogkey(groupId,Constant.CANCEL);
                redisTemplate.opsForHash().putIfAbsent(Constant.TRANSACTION_LOG,key,transMap);
                break;
            default:
                break;
        }
    }

    private String getHashMapLogkey(String groupId,String value){
        String key=Constant.getAppName()+Constant.DELIMIT+groupId+Constant.DELIMIT+value;
        return key;
    }

    /**
     * 判断幂等性
     * @param transaction
     * @return
     */
    @Override
    public boolean judgeIdempotent(Transaction transaction){
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String key="";
        switch (TransactionStatus.valueOf(transaction.getStatus().value())){
            case TRY:
                break;
            case CONFIRM:
                key=getHashMapLogkey(groupId,Constant.CONFIRM);
                return redisTemplate.opsForHash().hasKey(Constant.TRANSACTION_LOG,key);
            case CANCEL:
                key=getHashMapLogkey(groupId,Constant.CANCEL);
                return redisTemplate.opsForHash().hasKey(Constant.TRANSACTION_LOG,key);
            default:
                break;
        }
        return false;

    }

    @Override
    public boolean deleteGroup(Transaction transaction) {
        return true;
    }

    @Override
    public Transaction findByGroupId(TransactionXid xid) {
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

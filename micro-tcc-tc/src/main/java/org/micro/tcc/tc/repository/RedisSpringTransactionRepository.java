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
import org.micro.tcc.tc.component.SpringContextAware;
import org.micro.tcc.tc.util.TransactionSerializer;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *@author jeff.liu
 *   redis 分布式事务日志保存，用于异常恢复
 *   1，如果事务组处于TRY状态，说明TRY过程没有成功，事务会本地事务回滚，不用处理
 *   2，如果事务组处于CONFIRM，CANCEL 状态，事务定时恢复
 *
 *   事务log ：hset, key 格式：micro-tcc-demo-springcloud-service-c#20190920372375385247191042#CONFIRM ,value: Transaction
 *   事务group ：zset, TRANSACTION_GROUP:20190920372381698593263616, key格式：micro-tcc-demo-springcloud-service-b#CONFIRM ,value :score
 *   事务：hset ,TCC:micro-tcc-demo-springcloud-service-a,key ：20190920372381698593263616 value:Transaction
 * date 2019/7/31
 */
@Slf4j
public class RedisSpringTransactionRepository implements TransactionRepository {


    private RedisTemplate redisTemplate;

    private final int pageNo=100;

    private static final Long RELEASE_SUCCESS = 1L;

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

    public String getTransactionXid(Transaction transaction){
        String transactionXid=transaction.getTransactionXid().getGlobalTccTransactionId();
        return transactionXid;
    }



    /**
     * 创建取消日志by用户手动取消
     * @param transaction
     * @return
     */
    @Override
    public boolean createGroupMemberForClientCancel(Transaction transaction) {
      return createGroupMember(transaction);
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

    /**
     * 向每个事务组成员添加提交或者取消记录
     * @param transaction
     * @return
     */
    @Override
    public boolean createConfirmOrder(Transaction transaction){
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        Set<String> appNameList=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
        for(String appName:appNameList){
            String key=CoordinatorUtils.getAppName(appName);
            switch (TransactionStatus.valueOf(transaction.getStatus().value())){
                case CONFIRM:
                    if(appName.indexOf(Constant.CONFIRM)!=-1 ){
                        saveTransaction(transaction,key,groupId);
                    }
                    break;
                case CANCEL:
                    if(appName.indexOf(Constant.CANCEL)!=-1 ){
                        saveTransaction(transaction,key,groupId);
                    }
                    break;
                default:
                    break;
            }
        }
        return true;
    }

    private void saveTransaction(Transaction transaction,String key,String groupId){
        Map<byte[], byte[]> origMap=(Map<byte[], byte[]>) redisTemplate.opsForHash().get(Constant.prefix+key,groupId);
        if(origMap!=null){
            origMap.put("status".getBytes(), ByteUtils.intToBytes(transaction.getStatus().value()));
            redisTemplate.opsForHash().put(Constant.prefix+key,getTransactionXid(transaction),origMap);
        }
    }
    /**
     * 向每个事务组成员添加取消事务记录
     * @param transaction
     * @return
     */
    @Override
    public boolean createRollbackOrder(Transaction transaction){
        return createConfirmOrder(transaction);
    }

    /**
     * 创建事务组
     * @param transaction
     * @return
     */
    @Override
    public boolean createGroupMember(Transaction transaction) {
        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        Map<byte[], byte[]> transMap= TransactionSerializer.serialize(serializer, transaction);
        switch (TransactionStatus.valueOf(transaction.getStatus().value())){
            case TRY:
                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.TRY,1);
                redisTemplate.opsForHash().put(Constant.getTransactionMapKey(),getTransactionXid(transaction),transMap);
                break;
            case CONFIRM:
                Set<String> appNameList=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
                for(String appName:appNameList){
                    if(appName.indexOf(Constant.TRY)!=-1 ){
                        String key=CoordinatorUtils.getAppName(appName);
                        Double score=redisTemplate.opsForZSet().score(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CONFIRM);
                        if(null!=score){
                            double b=score.doubleValue();
                            if(b!=2){
                                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CONFIRM,2);
                            }
                        }else {
                            redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CONFIRM,2);
                        }

                    }
                }
                break;
            case CANCEL:
                Set<String> appNameListOfCancel=redisTemplate.opsForZSet().range(Constant.TRANSACTION_GROUP+groupId,0,pageNo);
                for(String appName:appNameListOfCancel){
                    if(appName.indexOf(Constant.TRY)!=-1 || appName.indexOf(Constant.CONFIRM)!=-1){
                        String key=CoordinatorUtils.getAppName(appName);
                        Double score=redisTemplate.opsForZSet().score(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CANCEL);
                        if(null!=score){
                            double b=score.doubleValue();
                            if(b!=3){
                                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CANCEL,3);
                            }
                        }else {
                            redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,key+Constant.DELIMIT+Constant.CANCEL,3);
                        }

                    }
                }
                break;
            default:
                break;

        }
        return true;
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
        //redisTemplate.opsForHash().delete(Constant.getTransactionMapKey(),getTransactionXid(transaction));
        createTransactionLog(transaction);
        switch (TransactionStatus.valueOf(transaction.getStatus().value())){
            case TRY:
                break;
            case CONFIRM:
                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.TRY,0);
                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.CONFIRM,0);
                break;
            case CANCEL:
                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.TRY,0);
                //redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.CONFIRM,0);
                redisTemplate.opsForZSet().add(Constant.TRANSACTION_GROUP+groupId,Constant.getAppName()+Constant.DELIMIT+Constant.CANCEL,0);
                break;
            default:
                break;
        }

        return true;
    }

    public void createTransactionLog(Transaction transaction){
        if(Constant.IDEMPOTENT.equals(Constant.IDEMPOTENT_TRUE)){
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
                boolean isExistConfirm= redisTemplate.opsForHash().hasKey(Constant.TRANSACTION_LOG,key);
                key=getHashMapLogkey(groupId,Constant.CANCEL);
                boolean isExitCancel= redisTemplate.opsForHash().hasKey(Constant.TRANSACTION_LOG,key);
                if(isExistConfirm || isExitCancel){
                    return true;
                }else {
                    return false;
                }
            case CANCEL:
                key=getHashMapLogkey(groupId,Constant.CANCEL);
                return redisTemplate.opsForHash().hasKey(Constant.TRANSACTION_LOG,key);
            default:
                break;
        }
        return false;

    }


    @Override
    public Transaction findByGroupId(TransactionXid xid) {
        String transactionXid=xid.getGlobalTccTransactionId();
        Map<byte[], byte[]> transMap=(Map<byte[], byte[]>)redisTemplate.opsForHash().get(Constant.getTransactionMapKey(),transactionXid);
        Transaction transaction= TransactionSerializer.deserialize(serializer,transMap);
        return transaction;
    }


    /**
     * 尝试获取分布式锁 true：设置锁成功，false： 已有锁，不成功
     * @param lockKey 锁
     * @param requestId 请求标识
     * @param expireTime 超期时间
     * @return 是否获取成功
     */
    @Override
    public  boolean tryGetDistributedLock(String lockKey, String requestId, long expireTime) {
        boolean result = redisTemplate.opsForValue().setIfAbsent(lockKey,requestId,expireTime,TimeUnit.SECONDS);
        return result;
    }

    /**
     * 释放分布式锁
     * @param lockKey 锁
     * @param requestId 请求标识
     * @return 是否释放成功
     */
    @Override
    public  boolean releaseDistributedLock(String lockKey, String requestId) {

        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        RedisScript<String> SET_NX_WITH_EXPIRE_SCRIPT = new DefaultRedisScript<>(script, String.class);
        Object result = redisTemplate.execute(SET_NX_WITH_EXPIRE_SCRIPT, Collections.singletonList(lockKey), Collections.singletonList(requestId));
        if (RELEASE_SUCCESS.equals(result)) {
            return true;
        }
        return false;

    }
    @Override
    public Transaction findByGroupId(String gid) {
        Map<byte[], byte[]> transMap=(Map<byte[], byte[]>)redisTemplate.opsForHash().get(Constant.getTransactionMapKey(),gid);
//        byte[] statusByte=transMap.get("status".getBytes());
//        int statusInt=ByteUtils.bytesToInt(statusByte);
        if (null != transMap) {
          Transaction transaction = TransactionSerializer.deserialize(serializer, transMap);
          //transaction.changeStatus(TransactionStatus.valueOf(statusInt));
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

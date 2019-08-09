package org.micro.tcc.tc.repository;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.core.TransactionRepository;
import org.micro.tcc.common.core.TransactionGid;
import org.micro.tcc.common.serializer.KryoPoolSerializer;
import org.micro.tcc.common.serializer.ObjectSerializer;
import org.micro.tcc.tc.util.TransactionSerializer;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.query.SortQueryBuilder;
import org.micro.tcc.tc.component.SpringContextAware;

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
        String key="";
        String subKey="";

        return null;
    }
    public <T> List<T> sortPageList(String key,String subKey,String by,boolean isDesc,boolean isAlpha,int off,int num) throws  Exception{
        SortQueryBuilder<String> builder = SortQueryBuilder.sort(key);
        //builder.by(subKey+"*->"+by);
        //builder.get("#");
        //将按照字幕顺序进行排序
        //builder.alphabetical(isAlpha);
        if(isDesc)
            builder.order(SortParameters.Order.DESC);
        builder.limit(off, num);
        List<String> cks = redisTemplate.sort(builder.build());
        List<T> result = new ArrayList<T>();
        for (String ck : cks) {
            //得到项目对象 by(subKey+ck);
        }

        return result;
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
        //redisTemplate.opsForHash().delete(Constant.getTransactionMapKey(),getTransactionXid(transaction));
        return converResult(true);
    }

    @Override
    public Transaction findByGroupId(TransactionGid xid) {
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

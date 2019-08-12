package org.micro.tcc.tc.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.micro.tcc.common.core.TccTransactionContext;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.core.TransactionGid;
import org.micro.tcc.common.exception.NoExistedTransactionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;


import java.util.List;

/**
 * zookeeper-connection
 *  提供的CuratorWatcher接口实现
 *  jeff.liu
 *  2018-04-28 13:40
 **/
@Slf4j
public class CoordinatorWatcher implements ApplicationRunner {


    private static String zkIp;
    @Value("${micro.tcc.coordinator.ip}")
    public void setZkIp(String zkIp) {
        this.zkIp = zkIp;
    }

    private static final String nodePath = "/DistributedTransaction";

    private TransactionManager transactionManager=null;
    private static CuratorFramework client;
    private TransactionManager getTransactionManager(){
        if(transactionManager==null){
            return TransactionManager.getInstance();
        }
        return transactionManager;
    }

    public void start()throws Exception{
        RetryPolicy retryPolicy  = new ExponentialBackoffRetry(1000,3);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkIp)
                .sessionTimeoutMs(3000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        /**
         * 创建会话
         * */
        client.start();

        // PathChildrenCache: 监听数据节点的增删改，可以设置触发的事件
        final PathChildrenCache childrenCache = new PathChildrenCache(client, nodePath, true);
        /**
         * StartMode: 初始化方式
         * POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         */
        childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        // 列出子节点数据列表，需要使用BUILD_INITIAL_CACHE同步初始化模式才能获得，异步是获取不到的
        List<ChildData> childDataList = childrenCache.getCurrentData();
    /*    log.info("当前节点的子节点详细数据列表：");
        for (ChildData childData : childDataList) {
            log.info("\t* 子节点路径：" + new String(childData.getPath()) + "，该节点的数据为：" + new String(childData.getData()));
        }*/
        // 添加事件监听器
        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                // 通过判断event type的方式来实现不同事件的触发
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {  // 子节点初始化时触发

                    log.info("TCC:子节点初始化成功");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {  // 添加子节点时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 添加成功，");
                    log.info("TCC:zk该子节点的数据为：" + new String(event.getData().getData()));
                    //process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {  // 删除子节点时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 删除成功，");
                    log.info("TCC:zk该子节点的数据为：" + new String(event.getData().getData()));
                    process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));

                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {  // 修改子节点数据时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 数据更新成功，");
                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 新的数据为：" + new String(event.getData().getData()));
                    process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));
                }
            }
        });



    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        start();
    }
    private void process(String gid,String status){
        Transaction transaction = null;
        try {
            if(!getTransactionManager().isExitGlobalTransaction(gid)){
                log.info("TCC:服务器不存在globalTransactionId:{}",gid);
                return;
            }
            int _status = Integer.parseInt(status);
            TransactionGid transactionGid = new TransactionGid();
            transactionGid.setGlobalTccTransactionId(gid);
            TccTransactionContext transactionContext = new TccTransactionContext(transactionGid, _status);
            switch (TransactionStatus.valueOf(_status)) {
                case TRY:
                    break;
                case CONFIRM:
                    try {
                        transaction = getTransactionManager().propagationExistStart(transactionContext);
                        boolean asyncConfirm = false;
                        if ("true".equals(transaction.getAsyncConfirm())) {
                            asyncConfirm = true;
                        }
                        getTransactionManager().commit(null, asyncConfirm);
                    } catch (Throwable excepton) {
                        log.error(excepton.getMessage(),excepton);
                        //主调用方commit失败，修改状态为cancel，次调用方需要全部回滚
                        if(null==transaction)break;
                        transaction.changeStatus(TransactionStatus.CANCEL);
                        CoordinatorWatcher.modify(transaction);

                    }finally{
                        getTransactionManager().cleanAfterCompletion(transaction);
                    }
                    break;
                case CANCEL:
                    try {
                        transaction = getTransactionManager().propagationExistStart(transactionContext);
                        boolean asyncCancel = false;
                        if ("true".equals(transaction.getAsyncConfirm())) {
                            asyncCancel = true;
                        }
                        getTransactionManager().rollback(null, asyncCancel);
                    } catch (NoExistedTransactionException exception) {

                    }finally{
                        getTransactionManager().cleanAfterCancel(transaction);
                    }
                    break;
            }

        }catch (Throwable t){
            log.error("TCC:分布式协管理器发生错误：",t);
        }
    }

    public static boolean add(Transaction transaction) throws Exception {
        int status=transaction.getStatus().value();
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        client.create().forPath(nodePath+"/"+globalTccTransactionId,(status+"").getBytes());
        return true;
    }

    public static boolean modify(Transaction transaction) throws Exception{
        int status=transaction.getStatus().value();
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        client.setData().forPath(nodePath+"/"+globalTccTransactionId,(status+"").getBytes());
        return true;
    }


}
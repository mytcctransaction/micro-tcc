package org.micro.tcc.tc.component;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.constant.TransactionType;
import org.micro.tcc.common.core.TccTransactionContext;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.core.TransactionXid;
import org.micro.tcc.common.exception.NoExistedTransactionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;


import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        CoordinatorWatcher.zkIp = zkIp;
    }

    private static final String nodePath = "/DistributedTransaction";
    ExecutorService pool = Executors.newCachedThreadPool();
    private final static String delimit="#";
    private TransactionManager transactionManager=null;
    private static CuratorFramework client;
    TreeCache treeCache=null;
    private TransactionManager getTransactionManager(){
        if(transactionManager==null){
            return TransactionManager.getInstance();
        }
        return transactionManager;
    }

    public void start()throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

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

        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState state) {
                if (state == ConnectionState.LOST) {
                    //连接丢失
                    log.info("TCC:lost session with zookeeper");
                    treeCache.close();
                } else if (state == ConnectionState.CONNECTED) {
                    //连接新建
                    log.info("TCC:connected with zookeeper");
                    try {
                        addTreeWatch();
                    } catch (Exception e) {
                        log.error(e.getMessage(),e);
                    }
                } else if (state == ConnectionState.RECONNECTED) {
                    log.info("TCC:reconnected with zookeeper");
                    try {
                        addTreeWatch();
                    } catch (Exception e) {
                        log.error(e.getMessage(),e);
                    }
                }
            }
        });

    }
    public void stop() {
        client.close();
    }

    private void addPathChildWatch() throws Exception{
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
        //List<ChildData> childDataList = childrenCache.getCurrentData();
    /*    log.info("当前节点的子节点详细数据列表：");
        for (ChildData childData : childDataList) {
            log.info("\t* 子节点路径：" + new String(childData.getPath()) + "，该节点的数据为：" + new String(childData.getData()));
        }*/
       // 添加事件监听器
       childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                // 通过判断event type的方式来实现不同事件的触发
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {  // 子节点初始化时触发

                    log.info("TCC:子节点初始化成功");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {  // 添加子节点时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 添加成功，");
                    //log.info("TCC:zk该子节点的数据为：" + new String(event.getData().getData()));
                    process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {  // 删除子节点时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 删除成功，");
                    //log.info("TCC:zk该子节点的数据为：" + new String(event.getData().getData()));
                    //process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));

                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {  // 修改子节点数据时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 数据更新成功，");
                    //log.info("TCC:zk子节点：" + event.getData().getPath() + " 新的数据为：" + new String(event.getData().getData()));
                    process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));
                }
            }
        },pool);

    }

    private String getNodeGroupId(String nodePath){
        String[] path=nodePath.split("\\"+delimit);
        if(path.length==2){
            return path[1];
        }
        return "";
    }
    private void addTreeWatch() throws Exception{
        //设置节点的cache
        treeCache =TreeCache.newBuilder(client,nodePath).setCacheData(true).build();
        //设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if(data !=null){
                    switch (event.getType()) {
                        case NODE_ADDED:
                            log.info("NODE_ADDED : "+ data.getPath() +"  数据:"+ new String(data.getData()));

                            process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));
                            break;
                        case NODE_REMOVED:
                            //log.info("NODE_REMOVED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                           // process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));
                            break;
                        case NODE_UPDATED:

                            log.info("NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));
                            break;
                        case INITIALIZED:
                            
                            break;
                        default:
                            break;
                    }
                }else{
                    log.info( "data is null : "+ JSON.toJSONString(event));
                }
            }
        },pool);
        //开始监听
        treeCache.start();

    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        start();
    }

    private void transactionStart(){
        String appPath=nodePath+"/"+ Constant.getAppName();
        //  String appPath=nodePath+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId+"/";
        for(String key: treeCache.getCurrentChildren(appPath).keySet()){
            String childrenPath=appPath+"/"+key;
            Map<String, ChildData> childDataMap= treeCache.getCurrentChildren(childrenPath);
            for(String k:childDataMap.keySet()){
                if(k.indexOf(delimit+Constant.CONFIRM)!=-1){
                    String groupId=getNodeGroupId(k);
                    ChildData childData=childDataMap.get(k);
                    String status=new String(childData.getData());
                    process(groupId,status);
                }else if(k.indexOf(delimit+Constant.CANCEL)!=-1){
                    String groupId=getNodeGroupId(k);
                    ChildData childData=childDataMap.get(k);
                    String status=new String(childData.getData());
                    process(groupId,status);
                }
            }
        }
    }
    private void process(String groupId,String status){
        Transaction transaction = null;
        try {
            if(!getTransactionManager().isExitGlobalTransaction(groupId)){
                log.info("TCC:服务器不存在globalTransactionId:{}",groupId);
                return;
            }
            int _status = Integer.parseInt(status);
            TransactionXid transactionXid = new TransactionXid();
            transactionXid.setGlobalTccTransactionId(groupId);
            TccTransactionContext transactionContext = new TccTransactionContext(transactionXid, _status);
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
                        if(null==transaction){
                            break;
                        }
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
                        CoordinatorWatcher.deleteDataNode(transaction);
                    }
                    break;
                default:
                    break;
            }

        }catch (Throwable t){
            log.error("TCC:分布式协管理器发生错误：",t);
        }
    }

    public static boolean add(Transaction transaction) throws Exception {
        int status=transaction.getStatus().value();
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=nodePath+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId+"/";
        switch (TransactionType.valueOf(transaction.getTransactionType().value())){
            case ROOT:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+delimit+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            case BRANCH:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+delimit+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            default:
                break;
        }


        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodePath+"/"+globalTccTransactionId,(status+"").getBytes());

        return true;
    }

    public static boolean modify(Transaction transaction) throws Exception{
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=nodePath+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId+"/";
        int status=transaction.getStatus().value();
        switch (TransactionStatus.valueOf(status)) {
            case TRY:
                break;
            case CONFIRM:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+delimit+Constant.CONFIRM,(String.valueOf(status)).getBytes());
                break;
            case CANCEL:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+delimit+Constant.CANCEL,(String.valueOf(status)).getBytes());
                break;
            default:
                break;
        }

        return true;
    }
    private static void deleteDataNode(Transaction transaction) throws Exception{
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=nodePath+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId+"/";
    /*    Stat stat = client.checkExists().forPath(path);
        log.info("deleteNode : "+stat);*/
        client.delete().deletingChildrenIfNeeded().inBackground().forPath(appPath);
    }



}
package org.micro.tcc.tc.component;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
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
 *  zookeeper-分布式事务管理器
 *
 *  设计思路：
 *  1，当主事务发起方发出提交指令，所有子系统执行提交，成功提交会删除zk节点上指令
 *  2，当某子系统发生异常，发出回滚指令，所有子系统执行回滚，成功回滚会删除zk节点上的指令
 *  3，当主事务发起方发出的指令还存在zk上，说明事务没有正常结束，程序会在启动时候查找异常指令，恢复事务
 *
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

    private static final String TRANSACTION_PATH = "/DistributedTransaction";

    private static final String TRANSACTION_GROUP_PATH = "/DistributedTransaction/DistributedTransactionGroup";
    
    ExecutorService pool = Executors.newCachedThreadPool();
    private final static String DELIMIT="#";
    private TransactionManager transactionManager=null;
    private static CuratorFramework client;
    TreeCache treeCache=null;
    //private static  CoordinatorWatcher coordinatorWatcher=new CoordinatorWatcher();

    public static CoordinatorWatcher getInstance(){
        CoordinatorWatcher coordinatorWatcher= SpringContextAware.getBean(CoordinatorWatcher.class);
        return coordinatorWatcher;
    }

    private TransactionManager getTransactionManager(){
        if(transactionManager==null){
            return TransactionManager.getInstance();
        }
        return transactionManager;
    }

    public void start()throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000*60, 10);

        client = CuratorFrameworkFactory.builder()
                .connectString(zkIp)
                .sessionTimeoutMs(1000*30)
                .connectionTimeoutMs(1000*6)
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
        final PathChildrenCache childrenCache = new PathChildrenCache(client, TRANSACTION_PATH, true);
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
                    getTransactionManager().process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {  // 删除子节点时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 删除成功，");
                    //log.info("TCC:zk该子节点的数据为：" + new String(event.getData().getData()));
                    //process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));

                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {  // 修改子节点数据时触发

                    log.info("TCC:zk子节点：" + event.getData().getPath() + " 数据更新成功，");
                    //log.info("TCC:zk子节点：" + event.getData().getPath() + " 新的数据为：" + new String(event.getData().getData()));
                    getTransactionManager().process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()));
                }
            }
        },pool);

    }

    private String getNodeGroupId(String TRANSACTION_PATH){
        String[] path=TRANSACTION_PATH.split("\\"+DELIMIT);
        if(path.length==2){
            return path[0];
        }
        return "";
    }
    private void addTreeWatch() throws Exception{
        //设置节点的cache
        treeCache =TreeCache.newBuilder(client,TRANSACTION_PATH).setCacheData(true).build();
        //设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                String dataVal=data==null?"":new String(data.getData());
                if(data !=null && !StringUtils.isEmpty(dataVal)){
                    switch (event.getType()) {
                        case NODE_ADDED:
                            log.info("TCC:NODE_ADDED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            getTransactionManager().process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));

                            break;
                        case NODE_REMOVED:
                            log.info("TCC:NODE_REMOVED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                           // process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()))
                            break;
                        case NODE_UPDATED:
                            log.info("TCC:NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            getTransactionManager().process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));

                            break;
                        case INITIALIZED:
                            log.info("TCC:NODE_INITIALIZED : "+ data.getPath() +"  数据:"+ new String(data.getData()));

                            break;
                        default:
                            break;
                    }
                }else{
                    log.info( "TCC:data is null : "+ JSON.toJSONString(event));
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

    /**
     * 遍历zk 事务节点，把没有监控到watch的事务节点重新执行
     * 主要修复zk watch机制缺陷
     */
    private void processTransactionStart(){
        String appPath=TRANSACTION_PATH+"/"+ Constant.getAppName();
        // appPath=TRANSACTION_PATH+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId#**;
        log.info("TCC:开始处理zookeeper分布式事务");
        for(String key: treeCache.getCurrentChildren(appPath).keySet()){
            String childrenPath=appPath+"/"+key;
            Map<String, ChildData> childDataMap= treeCache.getCurrentChildren(childrenPath);
            for(String k:childDataMap.keySet()){
                if(k.indexOf(DELIMIT+Constant.CONFIRM)!=-1){
                    String groupId=getNodeGroupId(k);
                    ChildData childData=childDataMap.get(k);
                    String status=new String(childData.getData());
                    getTransactionManager().process(groupId,status);
                }else if(k.indexOf(DELIMIT+Constant.CANCEL)!=-1){
                    String groupId=getNodeGroupId(k);
                    ChildData childData=childDataMap.get(k);
                    String status=new String(childData.getData());
                    getTransactionManager().process(groupId,status);
                }
            }
        }
        log.info("TCC:结束处理zookeeper分布式事务");
    }


    /**
     * try阶段 生成事务组，格式：key[事务组/groupId/appName1] ,value[appName]
     * 生成事务，格式：key[事务/appName/groupId/groupId#** ] ,value [status]
     * @param transaction
     * @return
     * @throws Exception
     */
    public  boolean add(Transaction transaction) throws Exception {
        int status=transaction.getStatus().value();
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=TRANSACTION_PATH+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId;
        switch (TransactionType.valueOf(transaction.getTransactionType().value())){
            case ROOT:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId+"/"+Constant.getAppName(),(Constant.getAppName()).getBytes());
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+DELIMIT+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            case BRANCH:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId+"/"+Constant.getAppName(),(Constant.getAppName()).getBytes());
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+DELIMIT+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            default:
                break;
        }


        //client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(TRANSACTION_PATH+"/"+globalTccTransactionId,(status+"").getBytes());

        return true;
    }

    public  boolean modify(Transaction transaction) throws Exception{
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        //String appPath=TRANSACTION_PATH+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId;
        int status=transaction.getStatus().value();
        switch (TransactionStatus.valueOf(status)) {
            case TRY:
                break;
            case CONFIRM:
                //key[事务组/groupId/appName1]
                for(String key: treeCache.getCurrentChildren(TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId).keySet()){
                    String appPath=TRANSACTION_PATH+"/"+ key+"/" +globalTccTransactionId+"/"+globalTccTransactionId;
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+DELIMIT+Constant.CONFIRM,(String.valueOf(status)).getBytes());
                }
                break;
            case CANCEL:
                for(String key: treeCache.getCurrentChildren(TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId).keySet()) {
                    String appPath=TRANSACTION_PATH+"/"+ key+"/" +globalTccTransactionId+"/"+globalTccTransactionId;
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                            forPath(appPath+DELIMIT+Constant.CANCEL,(String.valueOf(status)).getBytes());
                }

                break;
            default:
                break;
        }

        return true;
    }

    /**
     * //key[事务组/groupId/appName1]
     * @param transaction
     * @throws Exception
     */
    public  void deleteDataNode(Transaction transaction) throws Exception{
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=TRANSACTION_PATH+"/"+ Constant.getAppName()+"/" +globalTccTransactionId;
    /*    Stat stat = client.checkExists().forPath(path);
        log.info("deleteNode : "+stat);*/
        client.delete().deletingChildrenIfNeeded().inBackground().forPath(appPath);
        log.info("TCC:delete zk path:"+appPath);
    }

    /**
     * [事务/appName/groupId/groupId#** ] , value [status]
     * //key[事务组/groupId/appName1]
     * @param transaction
     * @throws Exception
     */
    public  void deleteDataNodeForConfirm(Transaction transaction) throws Exception{
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=TRANSACTION_PATH+"/"+ Constant.getAppName()+"/" +globalTccTransactionId;
    /*    Stat stat = client.checkExists().forPath(path);
        log.info("deleteNode : "+stat);*/
        client.delete().deletingChildrenIfNeeded().inBackground().forPath(appPath);
        log.info("TCC:delete zk path:"+appPath);
    }

}
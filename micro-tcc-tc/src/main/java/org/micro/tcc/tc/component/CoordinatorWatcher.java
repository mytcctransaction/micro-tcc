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


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    ExecutorService nodePool = Executors.newCachedThreadPool();
    private final static String DELIMIT="#";
    private TransactionManager transactionManager=null;
    private static CuratorFramework client;


    PathChildrenCache childrenCache=null;
    private static  CoordinatorWatcher coordinatorWatcher=new CoordinatorWatcher();

    public static CoordinatorWatcher getInstance(){
        CoordinatorWatcher cWatcher= SpringContextAware.getBean(CoordinatorWatcher.class);
        if(cWatcher==null){
            cWatcher= coordinatorWatcher;
        }
        return cWatcher;
    }

    private TransactionManager getTransactionManager(){
        if(transactionManager==null){
            return TransactionManager.getInstance();
        }
        return transactionManager;
    }

    public void start()throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000*60, 50);

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
                    if(null!=childrenCache){
                        try {
                            childrenCache.close();
                        } catch (IOException e) {
                            log.error(e.getMessage(),e);
                        }
                    }

                } else if (state == ConnectionState.CONNECTED) {
                    //连接新建
                    log.info("TCC:connected with zookeeper");
                    try {
                        addPathChildWatch();
                    } catch (Exception e) {
                        log.error(e.getMessage(),e);
                    }
                } else if (state == ConnectionState.RECONNECTED) {
                    log.info("TCC:reconnected with zookeeper");
                    try {
                        addPathChildWatch();
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
        childrenCache = new PathChildrenCache(client, TRANSACTION_PATH, true,false,nodePool);
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
                    log.debug("TCC:子节点初始化成功");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {  // 添加子节点时触发
                    log.debug("TCC:zk子节点：" + event.getData().getPath() + " 添加成功");
                    getTransactionManager().process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {  // 删除子节点时触发
                    log.debug("TCC:zk子节点：" + event.getData().getPath() + " 删除成功");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {  // 修改子节点数据时触发
                    log.debug("TCC:zk子节点：" + event.getData().getPath() + " 数据更新成功");
                    getTransactionManager().process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));
                }
            }
        },pool);

    }

    private String getNodeGroupId(String transactionPath){
        String[] path=transactionPath.split("\\"+DELIMIT);
        if(path.length==2){
            return path[0];
        }
        return "";
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        start();
    }

    /**
     * 遍历zk 事务节点，把没有监控到watch的事务节点重新执行
     * 主要修复zk watch机制缺陷
     */
    public void processTransactionStart(){
        try{

           /*   future.get(1000*3, TimeUnit.SECONDS);
            if(!future.isDone()){
                return;
            }*/
            log.debug("TCC:开始定时处理zookeeper事件");
            String appGroupPath=TRANSACTION_GROUP_PATH;
            List<String> list=client.getChildren().forPath(appGroupPath);
            for(String key:list){
                List<String> childList=null;
                try{
                    childList=client.getChildren().forPath(appGroupPath+"/"+key);
                }catch (Exception e){

                }
                if(childList==null || childList.size()==0){
                    client.delete().inBackground().forPath(appGroupPath+"/"+key);
                    client.delete().inBackground().forPath(TRANSACTION_PATH+"/"+key+DELIMIT+Constant.CONFIRM);
                    client.delete().inBackground().forPath(TRANSACTION_PATH+"/"+key+DELIMIT+Constant.CANCEL);
                }

            }
            log.debug("TCC:结束定时处理zookeeper事件");
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }

    }

    private void jobTransactionProcess(String k,Map<String, ChildData> childDataMap){
        String groupId=getNodeGroupId(k);
        ChildData childData=childDataMap.get(k);
        //String status=new String(childData.getData());
        String status="";
        if(null!=childData){
            if(childData.getData()!=null){
                status=new String(childData.getData());
            }

        }
        if(StringUtils.isEmpty(groupId)|| StringUtils.isEmpty(status)){
            return;
        }
        getTransactionManager().syncProcess(groupId,status);
    }

    /**
     * try阶段 生成事务组，格式：key[事务组/groupId/groupId#appName1] ,value[appName]
     * 生成事务，格式：key[事务/appName/groupId#** ] ,value [status]
     * @param transaction
     * @return
     * @throws Exception
     */
    public  boolean add(Transaction transaction) throws Exception {
        int status=transaction.getStatus().value();
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=TRANSACTION_PATH+"/" +globalTccTransactionId+DELIMIT;
        String appGroupPath=TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId+"/"+globalTccTransactionId+DELIMIT+Constant.getAppName();
        switch (TransactionType.valueOf(transaction.getTransactionType().value())){
            case ROOT:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appGroupPath,(Constant.getAppName()).getBytes());
                //client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(appPath+DELIMIT+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            case BRANCH:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appGroupPath,(Constant.getAppName()).getBytes());
               // client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId+"/"+Constant.getAppName(),(Constant.getAppName()).getBytes());
                //client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(appPath+DELIMIT+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            default:
                break;
        }
        return true;
    }

    public  boolean modify(Transaction transaction) throws Exception{
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appPath=TRANSACTION_PATH+"/" +globalTccTransactionId+DELIMIT;
        int status=transaction.getStatus().value();
        switch (TransactionStatus.valueOf(status)) {
            case TRY:
                break;
            case CONFIRM:

                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+Constant.CONFIRM,(String.valueOf(status)).getBytes());
                break;
            case CANCEL:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(appPath+Constant.CANCEL,(String.valueOf(status)).getBytes());
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
        String appGroupPath=TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId+"/"+globalTccTransactionId+DELIMIT+Constant.getAppName();
        client.delete().deletingChildrenIfNeeded().inBackground().forPath(appGroupPath);
        //processTransactionStart();
        log.info("TCC:delete zk path:"+appGroupPath);
    }

    /**
     * [事务/appName/groupId/groupId#** ] , value [status]
     * //key[事务组/groupId/appName1]
     * @param transaction
     * @throws Exception
     */
    public  void deleteDataNodeForConfirm(Transaction transaction) throws Exception{
        String globalTccTransactionId=transaction.getTransactionXid().getGlobalTccTransactionId();
        String appGroupPath=TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId+"/"+globalTccTransactionId+DELIMIT+Constant.getAppName();
        client.delete().deletingChildrenIfNeeded().inBackground().forPath(appGroupPath);
        //processTransactionStart();
        log.info("TCC:delete zk path:"+appGroupPath);
    }
    public static void main(String[] args){
        try {
          Thread.sleep(2000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
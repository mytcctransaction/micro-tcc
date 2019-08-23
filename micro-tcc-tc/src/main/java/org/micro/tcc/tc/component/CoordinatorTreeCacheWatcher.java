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
public class CoordinatorTreeCacheWatcher implements ApplicationRunner {


    private static String zkIp;
    @Value("${micro.tcc.coordinator.ip}")
    public void setZkIp(String zkIp) {
        CoordinatorTreeCacheWatcher.zkIp = zkIp;
    }

    private static final String TRANSACTION_PATH = "/DistributedTransaction";

    private static final String TRANSACTION_GROUP_PATH = "/DistributedTransaction/DistributedTransactionGroup";

    ExecutorService pool = Executors.newCachedThreadPool();
    private final static String DELIMIT="#";
    private TransactionManager transactionManager=null;
    private static CuratorFramework client;

    private static volatile long timeMillis=System.currentTimeMillis();
    TreeCache treeCache=null;
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
                    if(null!=treeCache){
                        treeCache.close();
                    }

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

    private String getNodeGroupId(String TRANSACTION_PATH){
        String[] path=TRANSACTION_PATH.split("\\"+DELIMIT);
        if(path.length==2){
            return path[0];
        }
        return "";
    }
    private void addTreeWatch() throws Exception{
        //设置节点的cache
        treeCache =TreeCache.newBuilder(client,TRANSACTION_PATH).setCacheData(false).setExecutor(pool).build();
        //设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {

                ChildData data = event.getData();
                String dataVal="";
                if(null!=data){
                    if(data.getData()!=null){
                        dataVal=new String(data.getData());
                    }

                }
                if(data !=null && !StringUtils.isEmpty(dataVal)){
                    String temp=event.getData().getPath();
                    if(temp.indexOf(Constant.getAppName())==-1){
                        return;
                    }
                    switch (event.getType()) {
                        case NODE_ADDED:
                            log.debug("TCC:NODE_ADDED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            getTransactionManager().process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));
                            break;
                        case NODE_REMOVED:
                            log.debug("TCC:NODE_REMOVED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            // process(ZKPaths.getNodeFromPath(event.getData().getPath()),new String(event.getData().getData()))
                            break;
                        case NODE_UPDATED:
                            log.debug("TCC:NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            getTransactionManager().process(getNodeGroupId(ZKPaths.getNodeFromPath(event.getData().getPath())),new String(event.getData().getData()));
                        case INITIALIZED:
                            log.debug("TCC:NODE_INITIALIZED : "+ data.getPath() +"  数据:"+ new String(data.getData()));

                            break;
                        default:
                            break;
                    }
                }else{
                    log.debug( "TCC:zk node data : "+ JSON.toJSONString(event));
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
    public void processTransactionStart(Future future){
        try{
            future.get(1000*3, TimeUnit.SECONDS);
            if(!future.isDone()){
                return;
            }
            String appPath=TRANSACTION_PATH+"/"+ Constant.getAppName();
            // appPath=TRANSACTION_PATH+"/"+ Constant.getAppName()+"/" +globalTccTransactionId+"/"+globalTccTransactionId#**;
            log.debug("TCC:开始定时处理zookeeper事件");
            if(treeCache.getCurrentChildren(appPath)!=null){
                for(String key: treeCache.getCurrentChildren(appPath).keySet()){
                    String childrenPath=appPath+"/"+key;
                    Map<String, ChildData> childDataMap= treeCache.getCurrentChildren(childrenPath);
                    if(childDataMap!=null && childDataMap.keySet()!=null){
                        for(String k:childDataMap.keySet()){
                            if(k.indexOf(DELIMIT+Constant.CONFIRM)!=-1){
                                jobTransactionProcess(k,childDataMap);
                            }else if(k.indexOf(DELIMIT+Constant.CANCEL)!=-1){
                                jobTransactionProcess(k,childDataMap);
                            }
                        }
                    }

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
                //client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(appPath+DELIMIT+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            case BRANCH:
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).
                        forPath(TRANSACTION_GROUP_PATH+"/"+globalTccTransactionId+"/"+Constant.getAppName(),(Constant.getAppName()).getBytes());
                //client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(appPath+DELIMIT+Constant.TRY,(String.valueOf(status)).getBytes());
                break;
            default:
                break;
        }
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
        client.delete().deletingChildrenIfNeeded()/*.inBackground()*/.forPath(appPath);
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
        client.delete().deletingChildrenIfNeeded()/*.inBackground()*/.forPath(appPath);
        log.info("TCC:delete zk path:"+appPath);
    }
    public static void main(String[] args){
        try {
            long a=System.currentTimeMillis();
            Thread.sleep(2000);
            long b=System.currentTimeMillis();
            System.out.println("time:"+(b-a));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
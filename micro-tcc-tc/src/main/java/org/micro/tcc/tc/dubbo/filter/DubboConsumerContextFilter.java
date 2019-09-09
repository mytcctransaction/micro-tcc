package org.micro.tcc.tc.dubbo.filter;

import java.util.HashMap;
import java.util.Map;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.tc.component.TransactionManager;


/**
*@author jeff.liu
*   dubbo 消费者拦截器
* date 2019/8/5
*/
@Activate
@Slf4j
public class DubboConsumerContextFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        log.debug("TCC: Consumer filter ");
        Map<String, String> context = new HashMap<>();
        //处理group id
        Transaction transaction= TransactionManager.getInstance().getCurrentTransaction();
        if(null==transaction)return invoker.invoke(invocation);

        String groupId=transaction.getTransactionXid().getGlobalTccTransactionId();
        context.put(Constant.GLOBAL_TCCTRANSACTION_ID, groupId);
        context.put(Constant.TCCTRANSACTION_STATUS,String.valueOf(transaction.getStatus().value()));
        RpcContext.getContext().setAttachments(context);
        return invoker.invoke(invocation);
    }
}




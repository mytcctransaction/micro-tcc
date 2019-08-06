package org.micro.tcc.tc.dubbo.filter;

import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.constant.TransactionType;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.tc.component.TransactionManager;

/**
*@author jeff.liu
*@desc   描述
*@date 2019/8/5
*/
@Activate
@Slf4j
public class DubboProviderContextFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 处理group id
        log.info("TCC:Dubbo provider filter begin");
        String groupId = RpcContext.getContext().getAttachment(Constant.GLOBAL_TCCTRANSACTION_ID);
        String status = RpcContext.getContext().getAttachment(Constant.TCCTRANSACTION_STATUS);
        if(StringUtils.isNotEmpty(groupId)&&StringUtils.isNotEmpty(status)){

            Transaction transaction=new Transaction(TransactionType.BRANCH);
            transaction.getTransactionXid().setGlobalTccTransactionId(groupId);
            transaction.getStatus().setId(Integer.parseInt(status));
            TransactionManager.getInstance().registerTransactionTrace(transaction);
        }
        return invoker.invoke(invocation);
    }

}


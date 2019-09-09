package org.micro.tcc.tc.http;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.tc.component.TransactionManager;

import java.io.IOException;

/**
 * @author jeff.liu
 * rest 拦截器 ，传递group id
 *  date 2019/8/6 10:18
 */
@Slf4j
public class HttpClientRequestInterceptor implements HttpRequestInterceptor {


    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
        log.debug("TCC:HttpClientRequestInterceptor ");
        HttpCoreContext adapt = HttpCoreContext.adapt(httpContext);
        HttpHost targetHost = adapt.getTargetHost();
        Transaction transaction = TransactionManager.getInstance().getCurrentTransaction();
        if(null==transaction)return ;
        String groupId = transaction.getTransactionXid().getGlobalTccTransactionId();

        httpRequest.addHeader(Constant.GLOBAL_TCCTRANSACTION_ID, groupId);
        httpRequest.addHeader(Constant.TCCTRANSACTION_STATUS, String.valueOf(transaction.getStatus().value()));
    }


}

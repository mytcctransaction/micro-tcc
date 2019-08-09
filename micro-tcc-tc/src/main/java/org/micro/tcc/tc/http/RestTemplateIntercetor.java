package org.micro.tcc.tc.http;

import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.tc.component.TransactionManager;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

/**
 * @author jeff.liu
 *
 *  date 2019/8/6 10:25
 */
@Slf4j
public class RestTemplateIntercetor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(HttpRequest httpRequest, byte[] bytes, ClientHttpRequestExecution clientHttpRequestExecution) throws IOException {
        log.info("TCC: RestTemplateIntercetor begin");
        Transaction transaction = TransactionManager.getInstance().getCurrentTransaction();
        if(null==transaction)return clientHttpRequestExecution.execute(httpRequest,bytes);
        String groupId = transaction.getTransactionXid().getGlobalTccTransactionId();
        HttpHeaders headers = httpRequest.getHeaders();
        headers.add(Constant.GLOBAL_TCCTRANSACTION_ID, groupId);
        headers.add(Constant.TCCTRANSACTION_STATUS, String.valueOf(transaction.getStatus().value()));
        return clientHttpRequestExecution.execute(httpRequest,bytes);
    }
}

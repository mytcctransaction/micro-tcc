package org.micro.tcc.tc.http;

import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.tc.component.TransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * @author jeff.liu
 *  RestTemplate 拦截器，传递group id
 *  date 2019/8/6 10:25
 */
@Slf4j
public class RestTemplateInterceptor implements ClientHttpRequestInterceptor {


    @Override
    public ClientHttpResponse intercept(HttpRequest httpRequest, byte[] bytes, ClientHttpRequestExecution clientHttpRequestExecution) throws IOException {
        log.debug("TCC: RestTemplateIntercetor ");
        Transaction transaction = TransactionManager.getInstance().getCurrentTransaction();
        if(null==transaction)return clientHttpRequestExecution.execute(httpRequest,bytes);
        String groupId = transaction.getTransactionXid().getGlobalTccTransactionId();
        HttpHeaders headers = httpRequest.getHeaders();
        headers.add(Constant.GLOBAL_TCCTRANSACTION_ID, groupId);
        headers.add(Constant.TCCTRANSACTION_STATUS, String.valueOf(transaction.getStatus().value()));
        return clientHttpRequestExecution.execute(httpRequest,bytes);
    }
}

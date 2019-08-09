package org.micro.tcc.tc.interceptor;

import org.micro.tcc.common.constant.Constant;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.constant.TransactionType;
import org.micro.tcc.tc.component.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Enumeration;


/**
 *@author jeff.liu
 *   描述
 * date 2019/7/31
 */
public class WebControllerAspect extends HandlerInterceptorAdapter {

    private static Logger log = LoggerFactory.getLogger(WebControllerAspect.class);
    


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        try{
            log.info("TCC:WebControllerAspect begin");
            Enumeration<String> headerNames = request.getHeaderNames();
            Transaction transaction=new Transaction(TransactionType.BRANCH);
            int count=0;
            if (headerNames != null) {
                while (headerNames.hasMoreElements()) {
                    String name = headerNames.nextElement();
                    String headerValue = request.getHeader(name);
                    /** 遍历请求头里面的属性字段，将logId和token添加到新的请求头中转发到下游服务 */
                    if (Constant.GLOBAL_TCCTRANSACTION_ID.equalsIgnoreCase(name)) {
                        log.info("TCC:添加自定义请求头key:" + name + ",value:" + headerValue);
                        transaction.getTransactionXid().setGlobalTccTransactionId(headerValue);
                        count++;
                    }
                    if (Constant.TCCTRANSACTION_STATUS.equalsIgnoreCase(name)) {
                        log.info("TCC:添加自定义请求头key:" + name + ",value:" + headerValue);
                        transaction.getStatus().setId(Integer.parseInt(headerValue));
                        count++;
                    }
                    if(count>=2){
                        break;
                    }
                }
            }
            if(count==2){
                TransactionManager.getInstance().registerTransactionTrace(transaction);
            }

        }catch(Exception e){
            log.warn("globalTccTransactionId interceptor fail！",e);
        }
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        super.postHandle(request, response, handler, modelAndView);
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        super.afterCompletion(request, response, handler, ex);
    }

    @Override
    public void afterConcurrentHandlingStarted(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        super.afterConcurrentHandlingStarted(request, response, handler);
    }

}

package com.weichai.knowledge.utils;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 错误处理器
 * 用于统一处理和记录系统错误
 */
@Slf4j
@Component
public class ErrorHandler {

    /**
     * 记录错误日志
     *
     * @param errorType 错误类型
     * @param fileId    文件ID
     * @param step      执行步骤
     * @param errorMsg  错误信息
     * @param params    相关参数
     */
    public void logError(int errorType, String fileId, String step, String errorMsg, Map<String, Object> params) {
        try {
            // 设置MDC上下文
            MDC.put("errorType", String.valueOf(errorType));
            MDC.put("fileId", fileId);
            MDC.put("step", step);
            MDC.put("timestamp", LocalDateTime.now().toString());
            
            // 构建详细错误信息
            StringBuilder errorDetails = new StringBuilder();
            errorDetails.append(String.format("[错误类型:%d] [文件ID:%s] [步骤:%s]", 
                errorType, fileId, step));
            errorDetails.append(String.format(" 错误信息: %s", errorMsg));
            
            if (params != null && !params.isEmpty()) {
                errorDetails.append(" 参数详情: ");
                params.forEach((key, value) -> 
                    errorDetails.append(String.format("%s=%s ", key, value)));
            }
            
            // 记录到日志文件
            log.error(errorDetails.toString());
            
            // 这里可以扩展为发送到监控系统、数据库等
            // 例如：发送到Kafka、写入错误表、发送告警等
            
        } catch (Exception e) {
            log.error("记录错误日志时发生异常: {}", e.getMessage(), e);
        } finally {
            // 清理MDC
            MDC.remove("errorType");
            MDC.remove("fileId");
            MDC.remove("step");
            MDC.remove("timestamp");
        }
    }
    
    /**
     * 记录异常错误（包含完整堆栈信息）
     */
    public void logException(int errorType, String fileId, String step, Throwable exception, Map<String, Object> params) {
        try {
            // 设置MDC上下文
            MDC.put("errorType", String.valueOf(errorType));
            MDC.put("fileId", fileId);
            MDC.put("step", step);
            MDC.put("exceptionClass", exception.getClass().getSimpleName());
            
            // 获取完整堆栈信息
            String stackTrace = getStackTrace(exception);
            
            // 构建详细错误信息
            Map<String, Object> errorData = new HashMap<>();
            errorData.put("errorType", errorType);
            errorData.put("fileId", fileId);
            errorData.put("step", step);
            errorData.put("exceptionClass", exception.getClass().getName());
            errorData.put("exceptionMessage", exception.getMessage());
            errorData.put("stackTrace", stackTrace);
            if (params != null) {
                errorData.putAll(params);
            }
            
            log.error("异常详情: {}", errorData, exception);
            
        } catch (Exception e) {
            log.error("记录异常日志时发生异常: {}", e.getMessage(), e);
        } finally {
            // 清理MDC
            MDC.remove("errorType");
            MDC.remove("fileId");
            MDC.remove("step");
            MDC.remove("exceptionClass");
        }
    }
    
    /**
     * 获取异常堆栈信息
     */
    private String getStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * 记录警告日志
     */
    public void logWarning(String component, String operation, String message, Map<String, Object> params) {
        try {
            StringBuilder warningDetails = new StringBuilder();
            warningDetails.append(String.format("[组件:%s] [操作:%s] [时间:%s]", 
                component, operation, LocalDateTime.now()));
            warningDetails.append(String.format(" 警告信息: %s", message));
            
            if (params != null && !params.isEmpty()) {
                warningDetails.append(" 参数: ");
                params.forEach((key, value) -> 
                    warningDetails.append(String.format("%s=%s ", key, value)));
            }
            
            log.warn(warningDetails.toString());
            
        } catch (Exception e) {
            log.error("记录警告日志时发生异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 记录信息日志
     */
    public void logInfo(String component, String operation, String message, Map<String, Object> params) {
        try {
            StringBuilder infoDetails = new StringBuilder();
            infoDetails.append(String.format("[组件:%s] [操作:%s]", component, operation));
            infoDetails.append(String.format(" %s", message));
            
            if (params != null && !params.isEmpty()) {
                infoDetails.append(" 参数: ");
                params.forEach((key, value) -> 
                    infoDetails.append(String.format("%s=%s ", key, value)));
            }
            
            log.info(infoDetails.toString());
            
        } catch (Exception e) {
            log.error("记录信息日志时发生异常: {}", e.getMessage(), e);
        }
    }
} 
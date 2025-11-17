package com.weichai.knowledge.utils;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 增强日志工具类
 * 提供方法跟踪、参数记录、性能监控等功能
 */
@Slf4j
@Component
public class LoggingUtils {
    
    private static final String TRACE_ID_KEY = "traceId";
    private static final String METHOD_KEY = "method";
    private static final String PARAMS_KEY = "params";
    private static final String DURATION_KEY = "duration";
    private static final String ERROR_TYPE_KEY = "errorType";
    private static final String STEP_KEY = "step";
    
    private static final Logger MESSAGE_LOGGER = LoggerFactory.getLogger("MESSAGE_PROCESSOR");
    private static final Logger PERFORMANCE_LOGGER = LoggerFactory.getLogger("PERFORMANCE");
    
    // 存储方法执行开始时间
    private static final Map<String, Long> methodStartTimes = new ConcurrentHashMap<>();
    
    /**
     * 开始方法跟踪
     */
    public static String startMethodTrace(String className, String methodName, Object... params) {
        String traceId = generateTraceId();
        String methodSignature = className + "." + methodName;
        
        // 设置MDC上下文
        MDC.put(TRACE_ID_KEY, traceId);
        MDC.put(METHOD_KEY, methodSignature);
        
        // 记录参数
        if (params != null && params.length > 0) {
            StringBuilder paramStr = new StringBuilder();
            for (int i = 0; i < params.length; i++) {
                if (i > 0) paramStr.append(", ");
                paramStr.append("param").append(i).append("=");
                if (params[i] instanceof String || params[i] instanceof Number) {
                    paramStr.append(params[i]);
                } else {
                    paramStr.append(params[i] != null ? params[i].getClass().getSimpleName() : "null");
                }
            }
            MDC.put(PARAMS_KEY, paramStr.toString());
        }
        
        // 记录开始时间
        methodStartTimes.put(traceId, System.currentTimeMillis());
        
        log.debug("方法开始执行: {} 参数: {}", methodSignature, MDC.get(PARAMS_KEY));
        
        return traceId;
    }
    
    /**
     * 结束方法跟踪
     */
    public static void endMethodTrace(String traceId) {
        try {
            Long startTime = methodStartTimes.remove(traceId);
            if (startTime != null) {
                long duration = System.currentTimeMillis() - startTime;
                MDC.put(DURATION_KEY, String.valueOf(duration));
                
                String methodSignature = MDC.get(METHOD_KEY);
                
                log.debug("方法执行完成: {} 耗时: {}ms", methodSignature, duration);
                
                // 记录性能日志
                if (duration > 1000) { // 超过1秒的慢方法
                    PERFORMANCE_LOGGER.warn("慢方法警告: {} 耗时: {}ms", methodSignature, duration);
                }
            }
        } finally {
            clearMDC();
        }
    }
    
    /**
     * 记录方法异常
     */
    public static void logMethodError(String traceId, Throwable error, Object... contextParams) {
        try {
            MDC.put(ERROR_TYPE_KEY, error.getClass().getSimpleName());
            
            StringBuilder context = new StringBuilder();
            if (contextParams != null && contextParams.length > 0) {
                for (int i = 0; i < contextParams.length; i += 2) {
                    if (i + 1 < contextParams.length) {
                        context.append(contextParams[i]).append("=").append(contextParams[i + 1]).append(" ");
                    }
                }
            }
            
            String methodSignature = MDC.get(METHOD_KEY);
            
            log.error("方法执行异常: {} 错误: {} 上下文: {}", 
                methodSignature, error.getMessage(), context.toString(), error);
            
            // 清理开始时间记录
            methodStartTimes.remove(traceId);
        } finally {
            clearMDC();
        }
    }
    
    /**
     * 记录消息处理日志
     */
    public static void logMessageProcessing(String operation, String messageId, String step, 
                                          Map<String, Object> data, String result) {
        try {
            MDC.put(TRACE_ID_KEY, messageId);
            MDC.put(STEP_KEY, step);
            
            StringBuilder dataStr = new StringBuilder();
            if (data != null && !data.isEmpty()) {
                data.forEach((key, value) -> dataStr.append(key).append("=").append(value).append(" "));
            }
            
            MESSAGE_LOGGER.info("消息处理: {} 步骤: {} 数据: {} 结果: {}", 
                operation, step, dataStr.toString(), result);
        } finally {
            MDC.remove(STEP_KEY);
        }
    }
    
    /**
     * 记录消息处理错误
     */
    public static void logMessageError(String operation, String messageId, String step, 
                                     Throwable error, Map<String, Object> context) {
        try {
            MDC.put(TRACE_ID_KEY, messageId);
            MDC.put(STEP_KEY, step);
            MDC.put(ERROR_TYPE_KEY, error.getClass().getSimpleName());
            
            StringBuilder contextStr = new StringBuilder();
            if (context != null && !context.isEmpty()) {
                context.forEach((key, value) -> contextStr.append(key).append("=").append(value).append(" "));
            }
            
            MESSAGE_LOGGER.error("消息处理错误: {} 步骤: {} 错误: {} 上下文: {}", 
                operation, step, error.getMessage(), contextStr.toString(), error);
        } finally {
            MDC.remove(STEP_KEY);
            MDC.remove(ERROR_TYPE_KEY);
        }
    }
    
    /**
     * 执行带日志跟踪的方法
     */
    public static <T> T executeWithTrace(String className, String methodName, 
                                       Supplier<T> supplier, Object... params) {
        String traceId = startMethodTrace(className, methodName, params);
        try {
            return supplier.get();
        } catch (Exception e) {
            logMethodError(traceId, e);
            throw e;
        } finally {
            endMethodTrace(traceId);
        }
    }
    
    /**
     * 执行带日志跟踪的无返回值方法
     */
    public static void executeWithTrace(String className, String methodName, 
                                      Runnable runnable, Object... params) {
        String traceId = startMethodTrace(className, methodName, params);
        try {
            runnable.run();
        } catch (Exception e) {
            logMethodError(traceId, e);
            throw e;
        } finally {
            endMethodTrace(traceId);
        }
    }
    
    /**
     * 监控SQL查询性能
     */
    public static void logSqlPerformance(String sql, long executionTime, int resultCount) {
        if (executionTime > 500) { // 超过500ms的慢查询
            PERFORMANCE_LOGGER.warn("慢SQL查询: 耗时{}ms 结果数量:{} SQL: {}", 
                executionTime, resultCount, sql);
        } else {
            PERFORMANCE_LOGGER.debug("SQL查询: 耗时{}ms 结果数量:{}", executionTime, resultCount);
        }
    }
    
    /**
     * 记录业务指标
     */
    public static void logBusinessMetric(String metricName, Object value, Map<String, Object> tags) {
        try {
            StringBuilder tagStr = new StringBuilder();
            if (tags != null && !tags.isEmpty()) {
                tags.forEach((key, val) -> tagStr.append(key).append("=").append(val).append(" "));
            }
            
            PERFORMANCE_LOGGER.info("业务指标: {} 值: {} 标签: {}", metricName, value, tagStr.toString());
        } catch (Exception e) {
            log.warn("记录业务指标失败: {}", e.getMessage());
        }
    }
    
    /**
     * 生成追踪ID
     */
    private static String generateTraceId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }
    
    /**
     * 清理MDC
     */
    private static void clearMDC() {
        MDC.remove(TRACE_ID_KEY);
        MDC.remove(METHOD_KEY);
        MDC.remove(PARAMS_KEY);
        MDC.remove(DURATION_KEY);
        MDC.remove(ERROR_TYPE_KEY);
    }
    
    /**
     * 获取当前追踪ID
     */
    public static String getCurrentTraceId() {
        return MDC.get(TRACE_ID_KEY);
    }
    
    /**
     * 手动设置追踪ID
     */
    public static void setTraceId(String traceId) {
        MDC.put(TRACE_ID_KEY, traceId);
    }
} 
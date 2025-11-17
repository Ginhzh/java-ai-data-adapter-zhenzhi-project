package com.weichai.knowledge.config;

import com.weichai.knowledge.utils.LoggingUtils;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 日志记录切面
 * 自动记录方法执行、参数和异常信息
 */
@Slf4j
@Aspect
@Component
public class LoggingAspect {

    /**
     * 匹配Service层所有方法
     */
    @Pointcut("execution(* com.weichai.knowledge.service..*.*(..))")
    public void serviceLayer() {}

    /**
     * 匹配Controller层所有方法
     */
    @Pointcut("execution(* com.weichai.knowledge.controller..*.*(..))")
    public void controllerLayer() {}

    /**
     * 匹配消息处理相关方法
     */
    @Pointcut("execution(* com.weichai.knowledge.service..*.process*(..))")
    public void messageProcessing() {}

    /**
     * Service层方法拦截
     */
    @Around("serviceLayer()")
    public Object logServiceMethods(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMethodExecution(joinPoint, "SERVICE");
    }

    /**
     * Controller层方法拦截
     */
    @Around("controllerLayer()")
    public Object logControllerMethods(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMethodExecution(joinPoint, "CONTROLLER");
    }

    /**
     * 消息处理方法特殊拦截
     */
    @Around("messageProcessing()")
    public Object logMessageProcessingMethods(ProceedingJoinPoint joinPoint) throws Throwable {
        return logMessageProcessingExecution(joinPoint);
    }

    /**
     * 通用方法执行日志记录
     */
    private Object logMethodExecution(ProceedingJoinPoint joinPoint, String layer) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        String className = joinPoint.getTarget().getClass().getSimpleName();
        String methodName = method.getName();
        Object[] args = joinPoint.getArgs();

        // 开始方法跟踪
        String traceId = LoggingUtils.startMethodTrace(className, methodName, args);

        try {
            // 记录HTTP相关信息（如果是Controller）
            if ("CONTROLLER".equals(layer)) {
                logHttpRequestInfo(method, args);
            }

            // 执行方法
            Object result = joinPoint.proceed();

            // 记录返回结果（简化版）
            if (result != null) {
                log.debug("[{}] 方法返回: {} 结果类型: {}", 
                    layer, methodName, result.getClass().getSimpleName());
            }

            return result;

        } catch (Throwable throwable) {
            // 记录异常信息
            Map<String, Object> context = new HashMap<>();
            context.put("layer", layer);
            context.put("className", className);
            context.put("methodName", methodName);
            if (args != null && args.length > 0) {
                context.put("argCount", args.length);
                context.put("argTypes", Arrays.stream(args)
                    .map(arg -> arg != null ? arg.getClass().getSimpleName() : "null")
                    .toArray(String[]::new));
            }

            LoggingUtils.logMethodError(traceId, throwable, 
                "layer", layer, "argCount", args != null ? args.length : 0);
            
            throw throwable;
        } finally {
            LoggingUtils.endMethodTrace(traceId);
        }
    }

    /**
     * 消息处理方法专用日志记录 - 支持响应式方法
     */
    private Object logMessageProcessingExecution(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String className = joinPoint.getTarget().getClass().getSimpleName();
        String methodName = signature.getMethod().getName();
        Object[] args = joinPoint.getArgs();

        // 尝试提取消息ID
        String messageId = extractMessageId(args);
        String operation = className + "." + methodName;

        try {
            LoggingUtils.logMessageProcessing(operation, messageId, "START",
                buildMessageData(args), "开始处理");

            Object result = joinPoint.proceed();

            // 检查是否为响应式方法
            if (result instanceof reactor.core.publisher.Mono) {
                @SuppressWarnings("unchecked")
                reactor.core.publisher.Mono<Object> mono = (reactor.core.publisher.Mono<Object>) result;

                return mono
                    .doOnSuccess(actualResult -> {
                        LoggingUtils.logMessageProcessing(operation, messageId, "COMPLETE",
                            buildMessageData(args), "响应式处理完成");
                    })
                    .doOnError(error -> {
                        Map<String, Object> context = buildMessageData(args);
                        context.put("className", className);
                        context.put("methodName", methodName);
                        LoggingUtils.logMessageError(operation, messageId, "ERROR",
                            error, context);
                    });
            } else if (result instanceof reactor.core.publisher.Flux) {
                @SuppressWarnings("unchecked")
                reactor.core.publisher.Flux<Object> flux = (reactor.core.publisher.Flux<Object>) result;

                return flux
                    .doOnComplete(() -> {
                        LoggingUtils.logMessageProcessing(operation, messageId, "COMPLETE",
                            buildMessageData(args), "响应式流处理完成");
                    })
                    .doOnError(error -> {
                        Map<String, Object> context = buildMessageData(args);
                        context.put("className", className);
                        context.put("methodName", methodName);
                        LoggingUtils.logMessageError(operation, messageId, "ERROR",
                            error, context);
                    });
            } else {
                // 非响应式方法，立即记录完成
                LoggingUtils.logMessageProcessing(operation, messageId, "COMPLETE",
                    buildMessageData(args), "同步处理完成");
                return result;
            }

        } catch (Throwable throwable) {
            Map<String, Object> context = buildMessageData(args);
            context.put("className", className);
            context.put("methodName", methodName);

            LoggingUtils.logMessageError(operation, messageId, "ERROR",
                throwable, context);

            throw throwable;
        }
    }

    /**
     * 记录HTTP请求信息
     */
    private void logHttpRequestInfo(Method method, Object[] args) {
        try {
            // 检查是否是REST端点
            if (method.isAnnotationPresent(RequestMapping.class) ||
                method.isAnnotationPresent(GetMapping.class) ||
                method.isAnnotationPresent(PostMapping.class) ||
                method.isAnnotationPresent(PutMapping.class) ||
                method.isAnnotationPresent(DeleteMapping.class)) {

                Map<String, Object> httpInfo = new HashMap<>();
                httpInfo.put("endpoint", method.getName());
                
                // 记录请求参数
                if (args != null && args.length > 0) {
                    for (int i = 0; i < args.length; i++) {
                        Object arg = args[i];
                        if (arg != null) {
                            String paramType = arg.getClass().getSimpleName();
                            httpInfo.put("param" + i + "Type", paramType);
                            
                            // 记录简单类型的值
                            if (arg instanceof String || arg instanceof Number || arg instanceof Boolean) {
                                httpInfo.put("param" + i + "Value", arg.toString());
                            }
                        }
                    }
                }

                log.info("HTTP请求: {}", httpInfo);
            }
        } catch (Exception e) {
            log.warn("记录HTTP请求信息失败: {}", e.getMessage());
        }
    }

    /**
     * 从参数中提取消息ID
     */
    private String extractMessageId(Object[] args) {
        if (args == null || args.length == 0) {
            return "unknown";
        }

        // 尝试从Map参数中提取
        for (Object arg : args) {
            if (arg instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) arg;
                
                // 尝试常见的ID字段
                Object id = map.get("messageTaskId");
                if (id == null) id = map.get("fileId");
                if (id == null) id = map.get("id");
                if (id == null) id = map.get("taskId");
                
                if (id != null) {
                    return id.toString();
                }
            }
        }

        return "extract_failed";
    }

    /**
     * 构建消息数据
     */
    private Map<String, Object> buildMessageData(Object[] args) {
        Map<String, Object> data = new HashMap<>();
        
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                if (arg != null) {
                    data.put("arg" + i + "Type", arg.getClass().getSimpleName());
                    
                    if (arg instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) arg;
                        
                        // 记录关键字段
                        String[] keyFields = {"fileId", "systemName", "fileName", "messageTaskId"};
                        for (String key : keyFields) {
                            Object value = map.get(key);
                            if (value != null) {
                                data.put(key, value.toString());
                            }
                        }
                    } else if (arg instanceof String || arg instanceof Number) {
                        data.put("arg" + i + "Value", arg.toString());
                    }
                }
            }
        }
        
        return data;
    }
} 
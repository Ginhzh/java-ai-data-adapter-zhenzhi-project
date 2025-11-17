package com.weichai.knowledge.exception;

/**
 * API业务异常类
 * 用于统一处理业务逻辑中的错误情况
 */
public class ApiException extends RuntimeException {
    
    private final int errorCode;
    private final String traceId;
    
    public ApiException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
        this.traceId = generateTraceId();
    }
    
    public ApiException(String message, int errorCode, String traceId) {
        super(message);
        this.errorCode = errorCode;
        this.traceId = traceId != null ? traceId : generateTraceId();
    }
    
    public ApiException(String message, int errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.traceId = generateTraceId();
    }
    
    public int getErrorCode() {
        return errorCode;
    }
    
    public String getTraceId() {
        return traceId;
    }
    
    /**
     * 生成追踪ID
     */
    private String generateTraceId() {
        return "trace-" + System.currentTimeMillis() + "-" + 
               Thread.currentThread().getId();
    }
    
    /**
     * 快速创建400错误
     */
    public static ApiException badRequest(String message) {
        return new ApiException(message, 400);
    }
    
    /**
     * 快速创建500错误
     */
    public static ApiException internalError(String message) {
        return new ApiException(message, 500);
    }
    
    /**
     * 快速创建500错误（带原因）
     */
    public static ApiException internalError(String message, Throwable cause) {
        return new ApiException(message, 500, cause);
    }
} 
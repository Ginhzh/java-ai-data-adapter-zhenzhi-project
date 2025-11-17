package com.weichai.knowledge.exception;

/**
 * 参数验证异常
 * 用于处理参数验证失败的情况
 */
public class ValidationException extends ApiException {
    
    public ValidationException(String message) {
        super(message, 400);
    }
    
    public ValidationException(String message, String traceId) {
        super(message, 400, traceId);
    }
} 
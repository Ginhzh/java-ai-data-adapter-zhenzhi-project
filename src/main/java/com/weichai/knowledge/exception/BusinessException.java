package com.weichai.knowledge.exception;

/**
 * 业务逻辑异常
 * 用于处理业务规则违反的情况
 */
public class BusinessException extends ApiException {
    
    public BusinessException(String message) {
        super(message, 422); // 422 Unprocessable Entity
    }
    
    public BusinessException(String message, int errorCode) {
        super(message, errorCode);
    }
    
    public BusinessException(String message, String traceId) {
        super(message, 422, traceId);
    }
    
    /**
     * 资源已存在异常
     */
    public static BusinessException resourceExists(String resourceType, String identifier) {
        return new BusinessException(String.format("%s已存在: %s", resourceType, identifier), 409);
    }
    
    /**
     * 资源不存在异常
     */
    public static BusinessException resourceNotFound(String resourceType, String identifier) {
        return new BusinessException(String.format("%s不存在: %s", resourceType, identifier), 404);
    }
} 
package com.weichai.knowledge.exception;

/**
 * 外部API调用异常
 * 用于处理调用外部API时的错误情况
 */
public class ExternalApiException extends ApiException {
    
    private final String apiName;
    
    public ExternalApiException(String apiName, String message) {
        super(String.format("调用%s API失败: %s", apiName, message), 502);
        this.apiName = apiName;
    }
    
    public ExternalApiException(String apiName, String message, Throwable cause) {
        super(String.format("调用%s API失败: %s", apiName, message), 502, cause);
        this.apiName = apiName;
    }
    
    public ExternalApiException(String apiName, String message, int errorCode) {
        super(String.format("调用%s API失败: %s", apiName, message), errorCode);
        this.apiName = apiName;
    }
    
    public String getApiName() {
        return apiName;
    }
} 
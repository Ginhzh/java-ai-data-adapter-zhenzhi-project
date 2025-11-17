package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 通用API响应DTO
 * 使用Java Record提供不可变性和简洁性
 */
public record ApiResponse<T>(
    @JsonProperty("returnCode") Integer returnCode,
    @JsonProperty("returnMessage") String returnMessage,
    @JsonProperty("result") T result,
    @JsonProperty("traceId") String traceId,
    @JsonProperty("code") Integer code,
    @JsonProperty("msg") String msg,
    @JsonProperty("message") String message,
    @JsonProperty("data") T data,
    @JsonProperty("success") Boolean success
) {
    
    /**
     * 检查响应是否成功
     */
    public boolean isSuccess() {
        // 检查多种可能的成功标识
        return (returnCode != null && returnCode == 200) ||
               (code != null && code == 200) ||
               (success != null && success);
    }
    
    /**
     * 获取错误消息
     */
    public String getErrorMessage() {
        if (returnMessage != null) return returnMessage;
        if (message != null) return message;
        if (msg != null) return msg;
        return "未知错误";
    }
    
    /**
     * 获取结果数据
     */
    public T getResultData() {
        if (result != null) return result;
        return data;
    }
}
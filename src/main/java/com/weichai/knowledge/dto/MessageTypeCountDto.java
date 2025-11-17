package com.weichai.knowledge.dto;

/**
 * 消息类型统计DTO
 */
public class MessageTypeCountDto {
    private String messageType;
    private Long count;
    
    public MessageTypeCountDto() {}
    
    public MessageTypeCountDto(String messageType, Long count) {
        this.messageType = messageType;
        this.count = count;
    }
    
    // Getters and Setters
    public String getMessageType() {
        return messageType;
    }
    
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }
    
    public Long getCount() {
        return count;
    }
    
    public void setCount(Long count) {
        this.count = count;
    }
    
    @Override
    public String toString() {
        return "MessageTypeCountDto{" +
                "messageType='" + messageType + '\'' +
                ", count=" + count +
                '}';
    }
}
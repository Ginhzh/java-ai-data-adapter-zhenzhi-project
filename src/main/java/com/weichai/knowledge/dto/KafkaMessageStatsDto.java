package com.weichai.knowledge.dto;

/**
 * Kafka消息统计DTO
 */
public class KafkaMessageStatsDto {
    private String systemName;
    private String messageType;
    private Long count;
    
    public KafkaMessageStatsDto() {}
    
    public KafkaMessageStatsDto(String systemName, String messageType, Long count) {
        this.systemName = systemName;
        this.messageType = messageType;
        this.count = count;
    }
    
    // Getters and Setters
    public String getSystemName() {
        return systemName;
    }
    
    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }
    
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
        return "KafkaMessageStatsDto{" +
                "systemName='" + systemName + '\'' +
                ", messageType='" + messageType + '\'' +
                ", count=" + count +
                '}';
    }
}
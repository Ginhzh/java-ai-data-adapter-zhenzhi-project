package com.weichai.knowledge.dto;

/**
 * 知识库角色同步统计DTO
 */
public class KnowledgeRoleSyncStatsDto {
    private String systemName;
    private String messageType;
    private Long count;
    
    public KnowledgeRoleSyncStatsDto() {}
    
    public KnowledgeRoleSyncStatsDto(String systemName, String messageType, Long count) {
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
        return "KnowledgeRoleSyncStatsDto{" +
                "systemName='" + systemName + '\'' +
                ", messageType='" + messageType + '\'' +
                ", count=" + count +
                '}';
    }
}
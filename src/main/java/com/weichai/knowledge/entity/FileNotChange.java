package com.weichai.knowledge.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * FILE_NOT_CHANGE消息实体类
 * 对应Python中的FileNotChange模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileNotChange {
    
    @JsonProperty("messageType")
    private String messageType;
    
    @JsonProperty("messageTaskId")
    private String messageTaskId;
    
    @JsonProperty("messageDateTime")
    private String messageDateTime;
    
    @JsonProperty("fileId")
    private String fileId;
    
    @JsonProperty("fileMetadata")
    private FileMetadata fileMetadata;
    
    @JsonProperty("fileAddRoleList")
    private List<String> fileAddRoleList;
    
    @JsonProperty("fileDelRoleList")
    private List<String> fileDelRoleList;
    
    @JsonProperty("fileAddUserList")
    private List<String> fileAddUserList;
    
    @JsonProperty("fileDelUserList")
    private List<String> fileDelUserList;
    
    // 兼容性方法 - 保持向后兼容
    public List<String> getAddRoleList() {
        return fileAddRoleList;
    }
    
    public List<String> getDelRoleList() {
        return fileDelRoleList;
    }
    
    public List<String> getAddUserList() {
        return fileAddUserList;
    }
    
    public List<String> getDelUserList() {
        return fileDelUserList;
    }
    
    public String getSystemName() {
        return fileMetadata != null ? fileMetadata.getSystemName() : null;
    }
    
    public String getFileName() {
        return fileMetadata != null ? fileMetadata.getFileName() : null;
    }
    

    
    /**
     * 文件元数据内部类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileMetadata {
        
        @JsonProperty("fileId")
        private String fileId;
        
        @JsonProperty("format")
        private String format;
        
        @JsonProperty("fileName")
        private String fileName;
        
        @JsonProperty("filePath")
        private String filePath;
        
        @JsonProperty("fileType")
        private String fileType;
        
        @JsonProperty("createTime")
        private Long createTime;
        
        @JsonProperty("fileNumber")
        private String fileNumber;
        
        @JsonProperty("systemName")
        private String systemName;
        
        @JsonProperty("updateTime")
        private Long updateTime;
    }
} 
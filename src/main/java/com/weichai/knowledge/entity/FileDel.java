package com.weichai.knowledge.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.util.List;

/**
 * FILE_DEL消息实体类
 * 对应Python中的FileDel模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileDel {
    
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
    
    @JsonProperty("fileAddUserList")
    private List<String> fileAddUserList;
    
    @JsonProperty("fileDelRoleList")
    private List<String> fileDelRoleList;
    
    @JsonProperty("fileDelUserList")
    private List<String> fileDelUserList;
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileMetadata {
        @JsonProperty("fileId")
        private String fileId;
        
        @JsonProperty("format")
        private String format;
        
        @JsonProperty("creator")
        private String creator;
        
        @JsonProperty("updater")
        private String updater;
        
        @JsonProperty("version")
        private String version;
        
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
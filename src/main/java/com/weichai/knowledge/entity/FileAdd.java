package com.weichai.knowledge.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * FILE_ADD消息实体类
 * 对应Python中的FileAdd模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileAdd {
    
    @JsonProperty("messageType")
    private String messageType;
    
    @JsonProperty("messageTaskId")
    private String messageTaskId;
    
    @JsonProperty("messageDateTime")
    private String messageDateTime;
    
    @JsonProperty("systemName")
    private String systemName;
    
    @JsonProperty("fileMetadata")
    private FileMetadata fileMetadata;
    
    @JsonProperty("roleList")
    private List<String> roleList;
    
    @JsonProperty("userList")
    private List<String> userList;
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileMetadata {
        @JsonProperty("fileId")
        private String fileId;
        
        @JsonProperty("fileName")
        private String fileName;
        
        @JsonProperty("fileNumber")
        private String fileNumber;
        
        @JsonProperty("filePath")
        private String filePath;
        
        @JsonProperty("fileType")
        private String fileType;
        
        @JsonProperty("fileSize")
        private Long fileSize;
        
        @JsonProperty("version")
        private String version;
        
        @JsonProperty("systemName")
        private String systemName;
        
        @JsonProperty("additionalInfo")
        private Map<String, Object> additionalInfo;
    }
} 
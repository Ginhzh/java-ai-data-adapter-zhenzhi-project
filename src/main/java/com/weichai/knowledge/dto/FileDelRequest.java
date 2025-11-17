package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 文件删除请求DTO - 适配实际数据格式
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileDelRequest {
    
    @NotBlank(message = "文件ID不能为空")
    @JsonProperty("fileId")
    private String fileId;
    
    @JsonProperty("messageType")
    private String messageType;
    
    @Valid
    @NotNull(message = "文件元数据不能为空")
    @JsonProperty("fileMetadata")
    private FileMetadata fileMetadata;
    
    @JsonProperty("messageTaskId")
    private String messageTaskId;
    
    @JsonProperty("fileAddRoleList")
    private String[] fileAddRoleList;
    
    @JsonProperty("fileAddUserList")
    private String[] fileAddUserList;
    
    @JsonProperty("fileDelRoleList")
    private String[] fileDelRoleList;
    
    @JsonProperty("fileDelUserList")
    private String[] fileDelUserList;
    
    @JsonProperty("messageDateTime")
    private String messageDateTime;
    
    /**
     * 文件元数据 - 适配实际数据格式
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileMetadata {
        
        @JsonProperty("fileId")
        private String fileId;
        
        @NotBlank(message = "系统名称不能为空")
        @JsonProperty("systemName")
        private String systemName;
        
        @NotBlank(message = "文件编号不能为空")
        @JsonProperty("fileNumber")
        private String fileNumber;
        
        @NotBlank(message = "文件名称不能为空")
        @JsonProperty("fileName")
        private String fileName;
        
        @NotBlank(message = "文件路径不能为空")
        @JsonProperty("filePath")
        private String filePath;
        
        @JsonProperty("version")
        private String version;
        
        @JsonProperty("format")
        private String format;
        
        @JsonProperty("creator")
        private String creator;
        
        @JsonProperty("updater")
        private String updater;
        
        @JsonProperty("fileType")
        private String fileType;
        
        @JsonProperty("objectKey")
        private String objectKey;
        
        @JsonProperty("bucketName")
        private String bucketName;
        
        @JsonProperty("createTime")
        private Long createTime;
        
        @JsonProperty("updateTime")
        private Long updateTime;
        
        @JsonProperty("deletedTime")
        private Long deletedTime;
        
        @JsonProperty("description")
        private String description;
    }
}
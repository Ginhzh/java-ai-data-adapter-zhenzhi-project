package com.weichai.knowledge.dto;

import lombok.Data;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.NotEmpty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 文件添加请求DTO
 */
@Data
public class FileAddRequest {
    
    @Valid
    @NotNull(message = "文件元数据不能为空")
    private FileMetadata fileMetadata;
    
    // 支持字符串数组格式
    private List<String> fileAddRoleList;
    
    // 支持字符串数组格式
    private List<String> fileAddUserList;
    
    // 删除角色列表
    private List<String> fileDelRoleList;
    
    // 删除用户列表
    private List<String> fileDelUserList;
    
    private String messageTaskId;
    
    // 消息类型
    private String messageType;
    
    // 消息日期时间
    private String messageDateTime;
    
    // 文件ID (顶层)
    private String fileId;
    
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        
        if (fileMetadata != null) {
            map.put("fileMetadata", fileMetadata.toMap());
        }
        
        if (fileAddRoleList != null) {
            map.put("fileAddRoleList", fileAddRoleList);
        }
        
        if (fileAddUserList != null) {
            map.put("fileAddUserList", fileAddUserList);
        }
        
        if (fileDelRoleList != null) {
            map.put("fileDelRoleList", fileDelRoleList);
        }
        
        if (fileDelUserList != null) {
            map.put("fileDelUserList", fileDelUserList);
        }
        
        if (messageTaskId != null) {
            map.put("messageTaskId", messageTaskId);
        }
        
        if (messageType != null) {
            map.put("messageType", messageType);
        }
        
        if (messageDateTime != null) {
            map.put("messageDateTime", messageDateTime);
        }
        
        if (fileId != null) {
            map.put("fileId", fileId);
        }
        
        return map;
    }
    
    @Data
    public static class FileMetadata {
        @NotEmpty(message = "文件ID不能为空")
        private String fileId;
        
        @NotEmpty(message = "系统名称不能为空")
        private String systemName;
        
        @NotEmpty(message = "文件编号不能为空")
        private String fileNumber;
        
        @NotEmpty(message = "文件名称不能为空")
        private String fileName;
        
        @NotEmpty(message = "文件路径不能为空")
        private String filePath;
        
        private String version;
        private String format;
        private String fileType;
        private String creator;
        private String updater;
        private Long createTime;
        private Long updateTime;
        
        @NotEmpty(message = "存储桶名称不能为空")
        private String bucketName;
        
        @NotEmpty(message = "对象键不能为空")
        private String objectKey;
        
        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("fileId", fileId);
            map.put("systemName", systemName);
            map.put("fileNumber", fileNumber);
            map.put("fileName", fileName);
            map.put("filePath", filePath);
            map.put("version", version);
            map.put("format", format);
            map.put("fileType", fileType);
            map.put("creator", creator);
            map.put("updater", updater);
            map.put("createTime", createTime);
            map.put("updateTime", updateTime);
            map.put("bucketName", bucketName);
            map.put("objectKey", objectKey);
            return map;
        }
    }
    

}
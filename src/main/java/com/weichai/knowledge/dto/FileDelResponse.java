package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 文件删除响应DTO
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileDelResponse {
    
    /**
     * 处理状态：success/error
     */
    @JsonProperty("status")
    private String status;
    
    /**
     * 系统名称
     */
    @JsonProperty("system_name")
    private String systemName;
    
    /**
     * 文件ID
     */
    @JsonProperty("file_id")
    private String fileId;
    
    /**
     * 文件编号
     */
    @JsonProperty("file_number")
    private String fileNumber;
    
    /**
     * 文件名称
     */
    @JsonProperty("file_name")
    private String fileName;
    
    /**
     * 甄知文件ID
     */
    @JsonProperty("file_zhenzhi_id")
    private String fileZhenzhiId;
    
    /**
     * 版本
     */
    @JsonProperty("version")
    private String version;
    
    /**
     * 删除状态：1-成功，0-失败
     */
    @JsonProperty("is_delete")
    private Integer isDelete;
    
    /**
     * 处理消息
     */
    @JsonProperty("message")
    private String message;
    
    /**
     * 处理时间戳
     */
    @JsonProperty("timestamp")
    private String timestamp;
    
    /**
     * 创建成功响应
     */
    public static FileDelResponse success(String systemName, String fileId, String fileNumber, 
                                        String fileName, String fileZhenzhiId, String version) {
        FileDelResponse response = new FileDelResponse();
        response.setStatus("success");
        response.setSystemName(systemName);
        response.setFileId(fileId);
        response.setFileNumber(fileNumber);
        response.setFileName(fileName);
        response.setFileZhenzhiId(fileZhenzhiId);
        response.setVersion(version);
        response.setIsDelete(1);
        response.setMessage("文件删除处理成功");
        response.setTimestamp(LocalDateTime.now().toString());
        return response;
    }
    
    /**
     * 创建失败响应
     */
    public static FileDelResponse error(String systemName, String fileId, String fileNumber, 
                                      String fileName, String errorMessage) {
        FileDelResponse response = new FileDelResponse();
        response.setStatus("error");
        response.setSystemName(systemName);
        response.setFileId(fileId);
        response.setFileNumber(fileNumber);
        response.setFileName(fileName);
        response.setIsDelete(0);
        response.setMessage(errorMessage);
        response.setTimestamp(LocalDateTime.now().toString());
        return response;
    }
    
    /**
     * 创建简单错误响应
     */
    public static FileDelResponse simpleError(String message) {
        FileDelResponse response = new FileDelResponse();
        response.setStatus("error");
        response.setMessage(message);
        response.setTimestamp(LocalDateTime.now().toString());
        return response;
    }
}
package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 导入文件到知识库请求DTO
 * 使用Java Record提供不可变性和类型安全
 */
public record ImportToRepoRequest(
    @JsonProperty("spaceGuid") String spaceGuid,
    @JsonProperty("groupGuid") String groupGuid,
    @JsonProperty("repositoryGuid") String repositoryGuid,
    @JsonProperty("bucketName") String bucketName,
    @JsonProperty("objectKey") String objectKey,
    @JsonProperty("fileName") String fileName,
    @JsonProperty("extraField") String extraField
) {
    
    /**
     * 构造器，处理可选参数
     */
    public ImportToRepoRequest(String spaceGuid, String groupGuid, String repositoryGuid,
                              String bucketName, String objectKey, String fileName, String extraField) {
        this.spaceGuid = spaceGuid;
        this.groupGuid = groupGuid;
        this.repositoryGuid = repositoryGuid;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.fileName = fileName; // 可为null
        this.extraField = extraField; // 可为null
    }
    
    /**
     * 简化构造器，不包含可选字段
     */
    public ImportToRepoRequest(String spaceGuid, String groupGuid, String repositoryGuid,
                              String bucketName, String objectKey) {
        this(spaceGuid, groupGuid, repositoryGuid, bucketName, objectKey, null, null);
    }
} 
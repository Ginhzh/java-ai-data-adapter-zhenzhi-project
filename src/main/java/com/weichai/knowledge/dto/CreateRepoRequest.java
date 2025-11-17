package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 创建知识库请求DTO
 * 使用Java Record提供不可变性和类型安全
 */
public record CreateRepoRequest(
    @JsonProperty("groupGuid") String groupGuid,
    @JsonProperty("intro") String intro,
    @JsonProperty("name") String name,
    @JsonProperty("scope") Integer scope
) {
    
    /**
     * 构造器，提供默认值
     */
    public CreateRepoRequest(String groupGuid, String intro, String name, Integer scope) {
        this.groupGuid = groupGuid;
        this.intro = intro != null ? intro : "";
        this.name = name;
        this.scope = scope != null ? scope : 20;
    }
    
    /**
     * 简化构造器，使用默认scope
     */
    public CreateRepoRequest(String groupGuid, String intro, String name) {
        this(groupGuid, intro, name, 20);
    }
} 
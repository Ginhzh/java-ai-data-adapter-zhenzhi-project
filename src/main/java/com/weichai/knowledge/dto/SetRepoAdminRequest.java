package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * 设置知识库权限请求DTO
 * 用于添加或删除知识库成员权限
 */
public record SetRepoAdminRequest(
    @JsonProperty("repoId") String repoId,
    @JsonProperty("groupGuid") String groupGuid,
    @JsonProperty("action") String action,
    @JsonProperty("members") List<RepoMember> members,
    @JsonProperty("message") String message
) {
    
    /**
     * 构造器，自动生成消息
     */
    public SetRepoAdminRequest(String repoId, String groupGuid, String action, List<RepoMember> members) {
        this(repoId, groupGuid, action, members,
             ("add".equals(action) ? "添加" : "删除") + "知识库权限");
    }
} 
package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 知识库成员DTO
 * 用于表示知识库权限管理中的成员信息
 */
public record RepoMember(
    @JsonProperty("nickname") String nickname,
    @JsonProperty("memberType") Integer memberType,
    @JsonProperty("memberId") String memberId
) {
    
    /**
     * 创建用户成员
     */
    public static RepoMember createUser(String userId, String nickname) {
        return new RepoMember(
            nickname != null ? nickname : userId,
            1, // 1表示用户
            userId
        );
    }
    
    /**
     * 创建角色/群组成员  
     */
    public static RepoMember createRole(String roleId, String nickname) {
        return new RepoMember(
            nickname != null ? nickname : roleId,
            2, // 2表示如流群/角色
            roleId
        );
    }
} 
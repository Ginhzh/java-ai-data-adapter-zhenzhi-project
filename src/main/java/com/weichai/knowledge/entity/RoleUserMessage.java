package com.weichai.knowledge.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * 角色用户消息实体类
 * 对应Python中的RoleUserMessage模型
 * 用于处理ADD_ROLE、DEL_ROLE、ROLE_ADD_USER、ROLE_DEL_USER消息类型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RoleUserMessage {
    
    @JsonProperty("messageType")
    private String messageType;
    
    @JsonProperty("messageTaskId")
    private String messageTaskId;
    
    @JsonProperty("messageDateTime")
    private String messageDateTime;
    
    @JsonProperty("systemName")
    private String systemName;
    
    @JsonProperty("roleId")
    private String roleId;
    
    @JsonProperty("roleName")
    private String roleName;
    
    @JsonProperty("addUserList")
    private List<String> addUserList;
    
    @JsonProperty("delUserList")
    private List<String> delUserList;
    

    

} 
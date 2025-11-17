package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class VirtualGroupOpsStats {
    @JsonProperty("角色组创建")
    private Integer roleGroupCreate;
    
    @JsonProperty("角色组创建用户数")
    private Integer roleGroupCreateUsers;
    
    @JsonProperty("角色组删除")
    private Integer roleGroupDelete;
    
    @JsonProperty("角色新增记录数")
    private Integer roleAddRecords;
    
    @JsonProperty("角色新增用户数")
    private Integer roleAddUsers;
    
    @JsonProperty("角色删除记录数")
    private Integer roleDeleteRecords;
    
    @JsonProperty("角色删除用户数")
    private Integer roleDeleteUsers;
} 
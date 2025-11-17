package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KafkaMessageStats {
    @JsonProperty("文件新增消息数")
    private Integer fileAddMessages;
    
    @JsonProperty("文件删除消息数")
    private Integer fileDeleteMessages;
    
    @JsonProperty("文件更新消息数")
    private Integer fileUpdateMessages;
    
    @JsonProperty("角色组创建消息数")
    private Integer roleGroupCreateMessages;
    
    @JsonProperty("角色组删除消息数")
    private Integer roleGroupDeleteMessages;
    
    @JsonProperty("角色新增用户消息数")
    private Integer roleAddUserMessages;
    
    @JsonProperty("角色删除用户消息数")
    private Integer roleDeleteUserMessages;
    
    @JsonProperty("文件权限更新")
    private FilePermissionUserStats fileUpdate;
    
    @JsonProperty("角色用户变更")
    private RoleUserChangeStats roleUserChange;
} 
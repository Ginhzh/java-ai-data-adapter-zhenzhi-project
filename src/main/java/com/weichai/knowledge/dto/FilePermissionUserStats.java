package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FilePermissionUserStats {
    @JsonProperty("新增用户数")
    private Integer addUsers;
    
    @JsonProperty("删除用户数")
    private Integer deleteUsers;
} 
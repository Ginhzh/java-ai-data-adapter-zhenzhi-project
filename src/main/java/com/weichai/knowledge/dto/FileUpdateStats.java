package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FileUpdateStats {
    @JsonProperty("文件更新记录数")
    private Integer fileUpdateRecords;
    
    @JsonProperty("角色新增")
    private Integer roleAdd;
    
    @JsonProperty("角色删除")
    private Integer roleDelete;
    
    @JsonProperty("用户新增")
    private Integer userAdd;
    
    @JsonProperty("用户删除")
    private Integer userDelete;
} 
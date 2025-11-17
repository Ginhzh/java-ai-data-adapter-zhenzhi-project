package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FileOpsStats {
    @JsonProperty("新增")
    private Integer add;
    
    @JsonProperty("删除")
    private Integer delete;
} 
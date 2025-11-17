package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SystemStatistics {
    private String systemName;
    private Integer incrementalCount;
    private Integer addCount;
    private Integer updateCount;
    private Integer successCount;
    private Double successRate;
} 
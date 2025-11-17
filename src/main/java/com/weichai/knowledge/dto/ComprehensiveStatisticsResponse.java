package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ComprehensiveStatisticsResponse {
    private Object dateRange;
    private String systemName;
    private Object documentIngestionStatistics;
    private Object systemStatistics;
    private Object filePermissionStatistics;
    private Object roleUserStatistics;
} 
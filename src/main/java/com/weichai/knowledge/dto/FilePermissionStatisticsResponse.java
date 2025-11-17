package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class FilePermissionStatisticsResponse {
    private Object dateRange;
    private List<Object> dailyStatistics;
    private Object summaryStatistics;
} 
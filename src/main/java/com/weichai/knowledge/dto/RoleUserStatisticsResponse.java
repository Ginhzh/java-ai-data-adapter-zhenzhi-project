package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class RoleUserStatisticsResponse {
    private Object dateRange;
    private List<Object> dailyStatistics;
    private Object summaryStatistics;
} 
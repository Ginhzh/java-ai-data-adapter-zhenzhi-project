package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDate;
import java.util.List;

@Data
@Builder
public class SummaryStatisticsResponse {
    private LocalDate startDate;
    private LocalDate endDate;
    private Integer totalIncremental;
    private Integer totalSuccess;
    private Double successRate;
    private List<SystemStatistics> systemStatistics;
} 
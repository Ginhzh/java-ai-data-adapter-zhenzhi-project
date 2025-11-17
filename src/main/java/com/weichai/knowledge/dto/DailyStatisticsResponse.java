package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDate;
import java.util.List;

@Data
@Builder
public class DailyStatisticsResponse {
    private LocalDate startDate;
    private LocalDate endDate;
    private List<DailyStatistics> dailyStatistics;
} 
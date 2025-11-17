package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDate;

@Data
@Builder
public class DailyStatistics {
    private LocalDate date;
    private Integer addCount;
    private Integer updateCount;
    private Integer totalCount;
    private Integer successCount;
    private Double successRate;
}
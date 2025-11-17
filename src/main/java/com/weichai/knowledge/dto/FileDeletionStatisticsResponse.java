package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class FileDeletionStatisticsResponse {
    private Long total;
    private Long successCount;
    private Double successRate;
    private List<Object> deletionList;
    private List<Object> systemStatistics;
    private Object pagination;
} 
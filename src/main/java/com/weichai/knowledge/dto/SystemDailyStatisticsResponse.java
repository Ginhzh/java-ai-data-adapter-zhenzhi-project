package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class SystemDailyStatisticsResponse {
    @JsonProperty("日期")
    private String date;
    
    @JsonProperty("系统统计")
    private List<SystemDailyStats> systemStatistics;
    
    @JsonProperty("总计统计")
    private SystemDailyStats totalStatistics;
} 
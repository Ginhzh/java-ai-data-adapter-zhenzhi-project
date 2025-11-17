package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDate;
import java.util.List;

@Data
@Builder
public class DataNamesResponse {
    private Long total;
    private Integer limit;
    private Integer offset;
    private String systemName;
    private LocalDate startDate;
    private LocalDate endDate;
    private List<DataNameInfo> data;
} 
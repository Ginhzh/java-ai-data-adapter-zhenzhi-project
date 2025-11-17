package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class FailedIngestionResponse {
    private Long totalCount;
    private List<Object> failedList;
    private Object pagination;
} 
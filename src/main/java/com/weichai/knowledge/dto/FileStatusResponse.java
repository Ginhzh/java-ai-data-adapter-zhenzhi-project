package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FileStatusResponse {
    private String fileNumber;
    private Object kafkaStatus;
    private Object documentStatus;
    private String message;
} 
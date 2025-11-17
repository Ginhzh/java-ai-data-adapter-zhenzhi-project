package com.weichai.knowledge.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class DataNameInfo {
    private String id;
    private String systemName;
    private String messageDateTime;
    private String createdAt;
    private String fileName;
    private String filePath;
    private String fileNumber;
    private String error;
} 
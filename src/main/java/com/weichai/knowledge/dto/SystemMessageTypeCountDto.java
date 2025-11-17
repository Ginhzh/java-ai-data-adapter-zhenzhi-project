package com.weichai.knowledge.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 系统消息类型统计DTO
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemMessageTypeCountDto {
    private String systemName;
    private String messageType;
    private Long count;
}
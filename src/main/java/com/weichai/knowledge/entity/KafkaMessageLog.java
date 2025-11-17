package com.weichai.knowledge.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Kafka消息日志表
 */
@Table("kafka_message_logs")
@Data
@EqualsAndHashCode(callSuper = false)
public class KafkaMessageLog {
    
    @Id
    @Column("id")
    private String id;
    
    @Column("system_name")
    @JsonProperty("system_name")
    private String systemName;
    
    @Column("message_type")
    @JsonProperty("message_type")
    private String messageType;
    
    @Column("message_content")
    @JsonProperty("message_content")
    private JsonNode messageContent;
    
    @Column("message_datetime")
    @JsonProperty("message_datetime")
    private LocalDateTime messageDateTime;
    
    @Column("created_at")
    @JsonProperty("created_at")
    private LocalDateTime createdAt;
    
    @Column("file_number")
    @JsonProperty("file_number")
    private String fileNumber;
    
} 
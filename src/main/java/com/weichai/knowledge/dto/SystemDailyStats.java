package com.weichai.knowledge.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SystemDailyStats {
    @JsonProperty("系统名称")
    private String systemName;
    
    @JsonProperty("文件操作")
    private FileOpsStats fileOps;
    
    @JsonProperty("文件更新")
    private FileUpdateStats fileUpdate;
    
    @JsonProperty("虚拟组操作")
    private VirtualGroupOpsStats virtualGroupOps;
    
    @JsonProperty("Kafka消息统计")
    private KafkaMessageStats kafkaMessageStats;
} 
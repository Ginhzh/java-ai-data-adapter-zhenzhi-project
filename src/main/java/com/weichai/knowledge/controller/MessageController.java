package com.weichai.knowledge.controller;

import com.weichai.knowledge.entity.KafkaMessageLog;
import com.weichai.knowledge.repository.KafkaMessageLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消息处理控制器
 * 提供消息查询和监控接口
 */
@RestController
@RequestMapping("/api/messages")
public class MessageController {
    
    @Autowired
    private KafkaMessageLogRepository kafkaMessageLogRepository;
    
    /**
     * 获取最近的消息日志
     */
    @GetMapping("/recent")
    public Mono<Map<String, Object>> getRecentMessages(@RequestParam(defaultValue = "10") int limit) {
        return kafkaMessageLogRepository.findRecentLogs()
            .take(limit)
            .collectList()
            .map(logs -> {
                Map<String, Object> result = new HashMap<>();
                result.put("total", logs.size());
                result.put("messages", logs);
                result.put("timestamp", LocalDateTime.now());
                return result;
            });
    }
    
    /**
     * 根据系统名称查询消息
     */
    @GetMapping("/system/{systemName}")
    public Mono<Map<String, Object>> getMessagesBySystem(@PathVariable String systemName) {
        return kafkaMessageLogRepository.findBySystemName(systemName)
            .collectList()
            .map(logs -> {
                Map<String, Object> result = new HashMap<>();
                result.put("systemName", systemName);
                result.put("total", logs.size());
                result.put("messages", logs);
                result.put("timestamp", LocalDateTime.now());
                return result;
            });
    }
    
    /**
     * 根据消息类型查询消息
     */
    @GetMapping("/type/{messageType}")
    public Mono<Map<String, Object>> getMessagesByType(@PathVariable String messageType) {
        return kafkaMessageLogRepository.findByMessageType(messageType)
            .collectList()
            .map(logs -> {
                Map<String, Object> result = new HashMap<>();
                result.put("messageType", messageType);
                result.put("total", logs.size());
                result.put("messages", logs);
                result.put("timestamp", LocalDateTime.now());
                return result;
            });
    }
    
    /**
     * 获取消息统计信息
     */
    @GetMapping("/stats")
    public Mono<Map<String, Object>> getMessageStats() {
        return kafkaMessageLogRepository.count()
            .map(totalMessages -> {
                Map<String, Object> result = new HashMap<>();
                result.put("totalMessages", totalMessages);
                result.put("timestamp", LocalDateTime.now());
                result.put("status", "active");
                return result;
            });
    }
} 
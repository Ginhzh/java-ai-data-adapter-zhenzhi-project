package com.weichai.knowledge.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weichai.knowledge.config.ApplicationProperties;
import com.weichai.knowledge.entity.*;
import com.weichai.knowledge.repository.KafkaMessageLogRepository;
import com.weichai.knowledge.service.ReactiveKnowledgeAddService;
import com.weichai.knowledge.service.FileDelService;
import com.weichai.knowledge.service.FileNotChangeService;
import com.weichai.knowledge.service.RoleUserService;

import reactor.core.scheduler.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.UUID;

/**
 * Kafka消息处理器
 * 优化版本：按消息类型和处理特性拆分不同的监听器，避免慢任务阻塞快任务
 */
@Slf4j
@Service
public class MessageHandler {
    
    @Autowired
    private ApplicationProperties applicationProperties;
    
    @Autowired
    private KafkaMessageLogRepository kafkaMessageLogRepository;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private ReactiveKnowledgeAddService knowledgeAddService;
    
    @Autowired
    private FileDelService fileDelService;
    
    @Autowired
    private FileNotChangeService fileNotChangeService;
    
    @Autowired
    private RoleUserService roleUserService;
    
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 处理FILE_ADD消息（慢任务）
     * 单独监听器，低并发，避免阻塞其他消息
     */
    @KafkaListener(
        topics = "#{@applicationProperties.kafka.topicPrefix}FILE_ADD",
        groupId = "#{@applicationProperties.kafka.groupId}",
        concurrency = "1"
    )
    public void handleFileAddMessage(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset,
                                   Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        String logId = UUID.randomUUID().toString();
        
        log.info("收到FILE_ADD消息: topic={}, partition={}, offset={}, logId={}", topic, partition, offset, logId);
        
        try {
            // 解析消息
            Map<String, Object> messageData = parseMessage(message);
            
            // 记录Kafka消息日志（独立事务）
            recordKafkaMessage(logId, messageData, message);
            
            // 异步处理FILE_ADD消息
            processFileAddMessageAsync(messageData);
            
            // 快速确认消息
            acknowledgment.acknowledge();

            long processingTime = System.currentTimeMillis() - startTime;
            log.info("FILE_ADD消息已接收并提交异步处理，耗时: {}ms, logId: {} (注意：实际处理仍在进行中)", processingTime, logId);
            
        } catch (Exception e) {
            log.error("FILE_ADD消息处理失败: {}, logId: {}, topic: {}, partition: {}, offset: {}", 
                     e.getMessage(), logId, topic, partition, offset, e);
            
            // 记录失败统计
            recordFailureMetrics(topic, e);
            
            // 即使失败也确认消息，避免重复处理阻塞队列
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 处理文件操作消息（FILE_DEL, FILE_NOT_CHANGE）
     * 中等并发处理
     */
    @KafkaListener(
        topics = {
            "#{@applicationProperties.kafka.topicPrefix}FILE_DEL",
            "#{@applicationProperties.kafka.topicPrefix}FILE_NOT_CHANGE"
        },
        groupId = "#{@applicationProperties.kafka.groupId}",
        concurrency = "3"
    )
    public void handleFileOpsMessage(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset,
                                   Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        String logId = UUID.randomUUID().toString();
        
        log.info("收到文件操作消息: topic={}, partition={}, offset={}, logId={}", topic, partition, offset, logId);
        
        try {
            // 解析消息
            Map<String, Object> messageData = parseMessage(message);
            
            // 记录Kafka消息日志（独立事务）
            recordKafkaMessage(logId, messageData, message);
            
            // 异步处理文件操作消息
            processFileOpsMessageAsync(messageData);
            
            // 快速确认消息
            acknowledgment.acknowledge();
            
            long processingTime = System.currentTimeMillis() - startTime;
            log.info("文件操作消息快速处理完成，耗时: {}ms, logId: {} (已提交异步任务)", processingTime, logId);
            
        } catch (Exception e) {
            log.error("文件操作消息处理失败: {}, logId: {}, topic: {}, partition: {}, offset: {}", 
                     e.getMessage(), logId, topic, partition, offset, e);
            
            // 记录失败统计
            recordFailureMetrics(topic, e);
            
            // 即使失败也确认消息，避免重复处理阻塞队列
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 处理角色用户消息（快任务）
     * 高并发处理，快速响应
     */
    @KafkaListener(
        topics = {
            "#{@applicationProperties.kafka.topicPrefix}ADD_ROLE",
            "#{@applicationProperties.kafka.topicPrefix}DEL_ROLE",
            "#{@applicationProperties.kafka.topicPrefix}ROLE_ADD_USER",
            "#{@applicationProperties.kafka.topicPrefix}ROLE_DEL_USER"
        },
        groupId = "#{@applicationProperties.kafka.groupId}",
        concurrency = "3"
    )
    public void handleRoleUserMessage(@Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                    @Header(KafkaHeaders.OFFSET) long offset,
                                    Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        String logId = UUID.randomUUID().toString();
        
        log.info("收到角色用户消息: topic={}, partition={}, offset={}, logId={}", topic, partition, offset, logId);
        
        try {
            // 解析消息
            Map<String, Object> messageData = parseMessage(message);
            
            // 记录Kafka消息日志（独立事务）
            recordKafkaMessage(logId, messageData, message);
            
            // 异步处理角色用户消息
            processRoleUserMessageAsync(messageData);
            
            // 快速确认消息
            acknowledgment.acknowledge();
            
            long processingTime = System.currentTimeMillis() - startTime;
            log.info("角色用户消息快速处理完成，耗时: {}ms, logId: {} (已提交异步任务)", processingTime, logId);
            
        } catch (Exception e) {
            log.error("角色用户消息处理失败: {}, logId: {}, topic: {}, partition: {}, offset: {}", 
                     e.getMessage(), logId, topic, partition, offset, e);
            
            // 记录失败统计
            recordFailureMetrics(topic, e);
            
            // 即使失败也确认消息，避免重复处理阻塞队列
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 解析Kafka消息
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseMessage(String rawMessage) throws JsonProcessingException {
        Map<String, Object> messageData = objectMapper.readValue(rawMessage, Map.class);
        
        // 检查并添加缺失的fileType字段
        if (messageData.containsKey("fileMetadata")) {
            Map<String, Object> fileMetadata = (Map<String, Object>) messageData.get("fileMetadata");
            if (!fileMetadata.containsKey("fileType")) {
                fileMetadata.put("fileType", "unknown");
                log.info("已添加缺失的fileType字段");
            }
        }
        
        // 处理FILE_DEL消息的fileId字段提升
        if ("FILE_DEL".equals(messageData.get("messageType"))) {
            Map<String, Object> metadata = (Map<String, Object>) messageData.get("fileMetadata");
            if (!messageData.containsKey("fileId") && metadata != null && metadata.containsKey("fileId")) {
                messageData.put("fileId", metadata.get("fileId"));
                log.info("已将 fileId={} 提升到顶层", metadata.get("fileId"));
            }
        }
        
        return messageData;
    }
    
    /**
     * 记录Kafka消息到数据库
     * 使用独立事务，避免影响消息处理性能
     */
    @Transactional
    private void recordKafkaMessage(String logId, Map<String, Object> messageData, String rawMessage) {
        try {
            String systemName = getSystemName(messageData);
            String messageType = (String) messageData.getOrDefault("messageType", "unknown");
            LocalDateTime messageDateTime = parseMessageDateTime(messageData);
            
            // 提取fileNumber（如果存在）
            String fileNumber = null;
            if (messageData.containsKey("fileMetadata")) {
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> fileMetadata = (java.util.Map<String, Object>) messageData.get("fileMetadata");
                if (fileMetadata != null) {
                    fileNumber = (String) fileMetadata.getOrDefault("fileNumber", null);
                }
            }
            
            KafkaMessageLog kafkaLog = new KafkaMessageLog();
            kafkaLog.setId(logId);
            kafkaLog.setSystemName(systemName);
            kafkaLog.setMessageType(messageType);
            kafkaLog.setMessageContent(objectMapper.valueToTree(messageData));
            kafkaLog.setMessageDateTime(messageDateTime);
            kafkaLog.setFileNumber(fileNumber);
            
            kafkaMessageLogRepository.save(kafkaLog);
            log.info("已记录Kafka消息，ID: {}", logId);
            
        } catch (Exception e) {
            log.error("记录Kafka消息失败: {}", e.getMessage(), e);
            // 记录消息失败不应该影响消息处理
        }
    }
    
    /**
     * 获取系统名称
     */
    @SuppressWarnings("unchecked")
    private String getSystemName(Map<String, Object> messageData) {
        String systemName = (String) messageData.get("systemName");
        if ("unknown".equals(systemName) || systemName == null) {
            if (messageData.containsKey("fileMetadata")) {
                Map<String, Object> fileMetadata = (Map<String, Object>) messageData.get("fileMetadata");
                systemName = (String) fileMetadata.getOrDefault("systemName", "unknown");
            }
        }
        return systemName != null ? systemName : "unknown";
    }
    
    /**
     * 解析消息时间
     */
    private LocalDateTime parseMessageDateTime(Map<String, Object> messageData) {
        String messageDateTime = (String) messageData.get("messageDateTime");
        if (messageDateTime != null) {
            try {
                return LocalDateTime.parse(messageDateTime, DATE_TIME_FORMATTER);
            } catch (DateTimeParseException e) {
                log.warn("无法解析消息时间戳: {}", messageDateTime);
            }
        }
        return null;
    }
    
    /**
     * 异步处理FILE_ADD消息
     */
    private void processFileAddMessageAsync(Map<String, Object> messageData) throws Exception {
        String messageType = (String) messageData.get("messageType");
        String fileId = extractFileId(messageData);
        
        log.info("开始异步处理FILE_ADD消息，消息类型: {}, 文件ID: {}", messageType, fileId);
        
        try {
            log.info("🚀 开始异步执行文件添加任务，文件ID: {}", fileId);

            knowledgeAddService.processFileAddMessage(messageData)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    result -> {
                        if (result != null) {
                            String status = (String) result.get("status");
                            log.info("✅ 文件添加任务完全处理成功！消息类型: {}, 文件ID: {}, 最终结果: {}",
                                messageType, fileId, status != null ? status : "unknown");
                        } else {
                            log.error("❌ 文件添加任务返回null结果，消息类型: {}, 文件ID: {}", messageType, fileId);
                        }
                    },
                    error -> {
                        log.error("❌ 文件添加任务最终处理失败，消息类型: {}, 文件ID: {}, 错误: {}",
                            messageType, fileId, error.getMessage(), error);
                        Exception ex = (error instanceof Exception) ? (Exception) error : new Exception(error);
                        recordFailureMetrics("FILE_ADD", ex);
                    }
                );
        } catch (Exception e) {
            log.error("启动异步处理时发生异常，文件ID: {}, 错误: {}", fileId, e.getMessage(), e);
            throw e;
        }
    }

    private String extractFileId(Map<String, Object> messageData) {
        if (messageData.containsKey("fileMetadata")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fileMetadata = (Map<String, Object>) messageData.get("fileMetadata");
            return (String) fileMetadata.get("fileId");
        }
        return (String) messageData.get("fileId");
    }
    
    /**
     * 异步处理文件操作消息（FILE_DEL, FILE_NOT_CHANGE）
     */
    private void processFileOpsMessageAsync(Map<String, Object> messageData) throws Exception {
        String messageType = (String) messageData.get("messageType");
        log.info("开始异步处理文件操作消息，消息类型: {}", messageType);
        
        switch (messageType) {
            case "FILE_DEL":
                FileDel fileDel = objectMapper.convertValue(messageData, FileDel.class);
                fileDelService.processFileDelMessage(fileDel);
                log.info("已分发文件删除任务，消息类型: {}", messageType);
                break;
                
            case "FILE_NOT_CHANGE":
                FileNotChange fileNotChange = objectMapper.convertValue(messageData, FileNotChange.class);
                fileNotChangeService.processFileNotChangeMessage(fileNotChange);
                log.info("已分发文件无变化任务，消息类型: {}", messageType);
                break;
                
            default:
                log.error("文件操作监听器收到未知消息类型: {}", messageType);
                throw new IllegalArgumentException("未知的文件操作消息类型: " + messageType);
        }
    }
    
    /**
     * 异步处理角色用户消息
     */
    private void processRoleUserMessageAsync(Map<String, Object> messageData) throws Exception {
        String messageType = (String) messageData.get("messageType");
        log.info("开始异步处理角色用户消息，消息类型: {}", messageType);
        
        switch (messageType) {
            case "ADD_ROLE":
            case "DEL_ROLE":
            case "ROLE_ADD_USER":
            case "ROLE_DEL_USER":
                RoleUserMessage roleUserMessage = objectMapper.convertValue(messageData, RoleUserMessage.class);
                roleUserService.processRoleUserMessage(roleUserMessage);
                log.info("已分发角色用户任务，消息类型: {}", messageType);
                break;
                
            default:
                log.error("角色用户监听器收到未知消息类型: {}", messageType);
                throw new IllegalArgumentException("未知的角色用户消息类型: " + messageType);
        }
    }
    
    /**
     * 记录失败指标
     */
    private void recordFailureMetrics(String topic, Exception e) {
        try {
            log.warn("记录消息处理失败指标: topic={}, error={}", topic, e.getClass().getSimpleName());
            // 这里可以添加指标收集逻辑，如发送到监控系统
        } catch (Exception ex) {
            log.error("记录失败指标时出错: {}", ex.getMessage());
        }
    }
} 




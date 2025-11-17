package com.weichai.knowledge.service;

import com.weichai.knowledge.entity.FileNotChange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * FILE_NOT_CHANGE消息处理服务
 * 与现有的MessageHandler和KnowledgeRoleService集成
 */
@Slf4j
@Service
public class FileNotChangeService {
    
    @Autowired
    private KnowledgeRoleService knowledgeRoleService;
    
    /**
     * 处理FILE_NOT_CHANGE消息
     * 对应Python中的process_file_not_change_message方法
     * 
     * @param fileNotChange FILE_NOT_CHANGE消息实体
     * @return 处理结果的响应式流
     */
    public Mono<Map<String, Object>> processFileNotChangeMessage(FileNotChange fileNotChange) {
        log.info("开始处理FILE_NOT_CHANGE消息: fileId={}, fileName={}", 
                fileNotChange.getFileId(), fileNotChange.getFileName());
        
        return Mono.fromRunnable(() -> validateFileNotChangeMessage(fileNotChange))
            .then(knowledgeRoleService.processFileNotChangeMessage(fileNotChange))
            .doOnNext(result -> {
                String status = (String) result.get("status");
                if ("success".equals(status)) {
                    log.info("FILE_NOT_CHANGE消息处理成功: messageTaskId={}", 
                        fileNotChange.getMessageTaskId());
                } else {
                    log.error("FILE_NOT_CHANGE消息处理失败: {}, messageTaskId={}", 
                        result.get("message"), fileNotChange.getMessageTaskId());
                }
            })
            .doOnError(throwable -> {
                log.error("FILE_NOT_CHANGE消息处理发生异常: messageTaskId={}", 
                    fileNotChange.getMessageTaskId(), throwable);
            })
            .onErrorMap(e -> new RuntimeException("处理FILE_NOT_CHANGE消息失败: " + e.getMessage(), e));
    }
    
    /**
     * 验证FILE_NOT_CHANGE消息参数
     * 
     * @param fileNotChange 消息实体
     */
    private void validateFileNotChangeMessage(FileNotChange fileNotChange) {
        if (fileNotChange == null) {
            throw new IllegalArgumentException("FileNotChange消息不能为空");
        }
        
        if (fileNotChange.getMessageTaskId() == null || fileNotChange.getMessageTaskId().trim().isEmpty()) {
            throw new IllegalArgumentException("消息任务ID不能为空");
        }
        
        if (fileNotChange.getFileId() == null || fileNotChange.getFileId().trim().isEmpty()) {
            throw new IllegalArgumentException("文件ID不能为空");
        }
        
        if (fileNotChange.getSystemName() == null || fileNotChange.getSystemName().trim().isEmpty()) {
            throw new IllegalArgumentException("系统名称不能为空");
        }
        
        if (!"FILE_NOT_CHANGE".equals(fileNotChange.getMessageType())) {
            throw new IllegalArgumentException("消息类型必须是FILE_NOT_CHANGE");
        }
        
        // 检查是否有角色或用户变更
        boolean hasRoleChanges = (fileNotChange.getAddRoleList() != null && !fileNotChange.getAddRoleList().isEmpty()) ||
                                (fileNotChange.getDelRoleList() != null && !fileNotChange.getDelRoleList().isEmpty());
        
        boolean hasUserChanges = (fileNotChange.getAddUserList() != null && !fileNotChange.getAddUserList().isEmpty()) ||
                                (fileNotChange.getDelUserList() != null && !fileNotChange.getDelUserList().isEmpty());
        
        if (!hasRoleChanges && !hasUserChanges) {
            log.warn("FILE_NOT_CHANGE消息没有角色或用户变更内容: messageTaskId={}", 
                fileNotChange.getMessageTaskId());
        }
        
        log.info("FILE_NOT_CHANGE消息验证通过: messageTaskId={}, 角色变更={}, 用户变更={}", 
            fileNotChange.getMessageTaskId(), hasRoleChanges, hasUserChanges);
    }
} 
package com.weichai.knowledge.controller;

import com.weichai.knowledge.entity.FileNotChange;
import com.weichai.knowledge.service.KnowledgeRoleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * 知识库角色权限控制器
 * 提供文件权限变更的HTTP接口
 */
@Slf4j
@RestController
@RequestMapping("/api/knowledge/role")
@CrossOrigin(origins = "*")
public class KnowledgeRoleController {
    
    @Autowired
    private KnowledgeRoleService knowledgeRoleService;
    
    /**
     * 处理文件未变更消息
     * 
     * @param fileNotChange 文件未变更消息
     * @return 处理结果
     */
    @PostMapping("/file-not-change")
    public Mono<ResponseEntity<Map<String, Object>>> processFileNotChange(
            @Valid @RequestBody FileNotChange fileNotChange) {
        
        log.info("接收到FILE_NOT_CHANGE消息请求: messageTaskId={}, fileId={}, fileName={}", 
            fileNotChange.getMessageTaskId(),
            fileNotChange.getFileId(),
            fileNotChange.getFileMetadata() != null ? fileNotChange.getFileMetadata().getFileName() : "unknown"
        );
        
        // 参数验证
        if (fileNotChange.getMessageTaskId() == null || fileNotChange.getMessageTaskId().trim().isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().body(Map.of(
                "status", "error",
                "message", "messageTaskId不能为空"
            )));
        }
        
        if (fileNotChange.getFileId() == null || fileNotChange.getFileId().trim().isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().body(Map.of(
                "status", "error", 
                "message", "fileId不能为空"
            )));
        }
        
        if (fileNotChange.getFileMetadata() == null) {
            return Mono.just(ResponseEntity.badRequest().body(Map.of(
                "status", "error",
                "message", "fileMetadata不能为空"
            )));
        }
        
        if (fileNotChange.getFileMetadata().getFileNumber() == null || 
            fileNotChange.getFileMetadata().getSystemName() == null) {
            return Mono.just(ResponseEntity.badRequest().body(Map.of(
                "status", "error",
                "message", "fileNumber和systemName不能为空"
            )));
        }
        
        // 直接使用响应式处理，因为service方法已经返回Mono
        return knowledgeRoleService.processFileNotChangeMessage(fileNotChange)
            .map(response -> {
                if ("success".equals(response.get("status"))) {
                    log.info("FILE_NOT_CHANGE消息处理成功: messageTaskId={}", fileNotChange.getMessageTaskId());
                    return ResponseEntity.ok(response);
                } else {
                    log.error("FILE_NOT_CHANGE消息处理失败: messageTaskId={}, error={}",
                        fileNotChange.getMessageTaskId(), response.get("message"));
                    return ResponseEntity.badRequest().body(response);
                }
            })
            .onErrorResume(e -> {
                log.error("处理FILE_NOT_CHANGE消息时发生异常: messageTaskId={}", 
                    fileNotChange.getMessageTaskId(), e);
                
                return Mono.just(ResponseEntity.internalServerError().body(Map.of(
                    "status", "error",
                    "message", "服务器内部错误: " + e.getMessage(),
                    "messageTaskId", fileNotChange.getMessageTaskId()
                )));
            });
    }
    
    /**
     * 健康检查接口
     * 
     * @return 服务状态
     */
    @GetMapping("/health")
    public Mono<ResponseEntity<Map<String, Object>>> health() {
        return Mono.just(ResponseEntity.ok(Map.of(
            "status", "success",
            "message", "Knowledge Role Service is running",
            "timestamp", System.currentTimeMillis()
        )));
    }
    
    /**
     * 获取服务信息
     * 
     * @return 服务信息
     */
    @GetMapping("/info")
    public Mono<ResponseEntity<Map<String, Object>>> info() {
        return Mono.just(ResponseEntity.ok(Map.of(
            "service", "Knowledge Role Service",
            "version", "1.0.0",
            "description", "处理文件权限变更的服务",
            "endpoints", Map.of(
                "fileNotChange", "/api/knowledge/role/file-not-change",
                "health", "/api/knowledge/role/health",
                "info", "/api/knowledge/role/info"
            )
        )));
    }
} 
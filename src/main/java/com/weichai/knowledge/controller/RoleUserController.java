package com.weichai.knowledge.controller;

import com.weichai.knowledge.entity.RoleUserMessage;
import com.weichai.knowledge.service.RoleUserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.HashMap;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * 角色用户消息处理控制器
 * 接收并处理角色用户相关的四类消息：
 * ADD_ROLE, DEL_ROLE, ROLE_ADD_USER, ROLE_DEL_USER
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/role-user")
public class RoleUserController {
    
    @Autowired
    private RoleUserService roleUserService;
    
    /**
     * 处理角色用户消息
     * 接收JSON格式的RoleUserMessage并异步处理
     * 
     * @param roleUserMessage 角色用户消息对象
     * @return 处理结果
     */
    @PostMapping("/process")
    public Mono<ResponseEntity<Map<String, Object>>> processRoleUserMessage(
            @Valid @RequestBody RoleUserMessage roleUserMessage) {
        
        log.info("接收到{}消息处理请求: roleId={}, messageTaskId={}", 
                roleUserMessage.getMessageType(), 
                roleUserMessage.getRoleId(), 
                roleUserMessage.getMessageTaskId());
        
        // 参数验证
        if (roleUserMessage.getMessageType() == null || roleUserMessage.getMessageType().trim().isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().body(createErrorResponse("messageType不能为空")));
        }
        
        if (roleUserMessage.getRoleId() == null || roleUserMessage.getRoleId().trim().isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().body(createErrorResponse("roleId不能为空")));
        }
        
        if (roleUserMessage.getMessageTaskId() == null || roleUserMessage.getMessageTaskId().trim().isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().body(createErrorResponse("messageTaskId不能为空")));
        }
        
        // 消息类型验证
        if (!isValidMessageType(roleUserMessage.getMessageType())) {
            return Mono.just(ResponseEntity.badRequest().body(createErrorResponse("不支持的消息类型: " + roleUserMessage.getMessageType())));
        }
        
        // 直接使用响应式处理，因为service方法已经返回Mono
        return roleUserService.processRoleUserMessage(roleUserMessage)
            .map(success -> {
                if (success) {
                    log.info("{}消息处理成功: roleId={}, messageTaskId={}", 
                            roleUserMessage.getMessageType(), 
                            roleUserMessage.getRoleId(), 
                            roleUserMessage.getMessageTaskId());
                    return ResponseEntity.ok(createSuccessResponse("消息处理成功"));
                } else {
                    log.error("{}消息处理失败: roleId={}, messageTaskId={}", 
                            roleUserMessage.getMessageType(), 
                            roleUserMessage.getRoleId(), 
                            roleUserMessage.getMessageTaskId());
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(createErrorResponse("消息处理失败"));
                }
            })
            .onErrorResume(throwable -> {
                log.error("{}消息处理异常: roleId={}, messageTaskId={}, error={}", 
                        roleUserMessage.getMessageType(), 
                        roleUserMessage.getRoleId(), 
                        roleUserMessage.getMessageTaskId(), 
                        throwable.getMessage(), throwable);
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(createErrorResponse("消息处理异常: " + throwable.getMessage())));
            });
    }
    
    /**
     * 批量处理角色用户消息
     * 
     * @param messages 角色用户消息列表
     * @return 批量处理结果
     */
    @PostMapping("/batch-process")
    public Mono<ResponseEntity<Map<String, Object>>> batchProcessRoleUserMessages(
            @Valid @RequestBody java.util.List<RoleUserMessage> messages) {
        
        log.info("接收到批量处理请求，消息数量: {}", messages.size());
        
        if (messages.isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().body(createErrorResponse("消息列表不能为空")));
        }
        
        // 批量reactive处理
        return reactor.core.publisher.Flux.fromIterable(messages)
            .flatMap(message -> roleUserService.processRoleUserMessage(message))
            .collectList()
            .map(results -> {
                long successCount = results.stream().mapToLong(success -> success ? 1 : 0).sum();
                
                Map<String, Object> result = new HashMap<>();
                result.put("success", true);
                result.put("message", "批量处理完成");
                result.put("totalCount", messages.size());
                result.put("successCount", successCount);
                result.put("failedCount", messages.size() - successCount);
                
                log.info("批量处理完成: 总数={}, 成功={}, 失败={}", 
                        messages.size(), successCount, messages.size() - successCount);
                
                return ResponseEntity.ok(result);
            })
            .onErrorResume(throwable -> {
                log.error("批量处理异常: {}", throwable.getMessage(), throwable);
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(createErrorResponse("批量处理异常: " + throwable.getMessage())));
            });
    }
    
    /**
     * 健康检查接口
     * 
     * @return 健康状态
     */
    @GetMapping("/health")
    public Mono<ResponseEntity<Map<String, Object>>> healthCheck() {
        return Mono.fromCallable(() -> {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "UP");
            response.put("service", "RoleUserService");
            response.put("timestamp", System.currentTimeMillis());
            return ResponseEntity.ok(response);
        });
    }
    
    /**
     * 获取支持的消息类型
     * 
     * @return 支持的消息类型列表
     */
    @GetMapping("/message-types")
    public Mono<ResponseEntity<Map<String, Object>>> getSupportedMessageTypes() {
        return Mono.fromCallable(() -> {
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("messageTypes", java.util.Arrays.asList(
                "ADD_ROLE", "DEL_ROLE", "ROLE_ADD_USER", "ROLE_DEL_USER"
            ));
            response.put("description", Map.of(
                "ADD_ROLE", "新增角色",
                "DEL_ROLE", "删除角色", 
                "ROLE_ADD_USER", "角色未变更，新增用户",
                "ROLE_DEL_USER", "角色未变更，删除用户"
            ));
            return ResponseEntity.ok(response);
        });
    }
    
    /**
     * 验证消息类型是否有效
     */
    private boolean isValidMessageType(String messageType) {
        return messageType != null && 
               (messageType.equals("ADD_ROLE") || 
                messageType.equals("DEL_ROLE") || 
                messageType.equals("ROLE_ADD_USER") || 
                messageType.equals("ROLE_DEL_USER"));
    }
    
    /**
     * 创建成功响应
     */
    private Map<String, Object> createSuccessResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", message);
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }
    
    /**
     * 创建错误响应
     */
    private Map<String, Object> createErrorResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("message", message);
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }
}
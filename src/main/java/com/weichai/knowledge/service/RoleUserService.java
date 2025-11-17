package com.weichai.knowledge.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weichai.knowledge.entity.RoleUserMessage;
import com.weichai.knowledge.entity.VirtualGroupSyncLog;
import com.weichai.knowledge.utils.ErrorHandler;
import com.weichai.knowledge.repository.ReactiveVirtualGroupSyncLogRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.UUID;
import reactor.core.publisher.Mono;

/**
 * 角色用户消息处理服务
 * 对应Python中的process_role_user_message方法
 * 处理四类报文:
 * 1. 新增角色 (ADD_ROLE)
 * 2. 删除角色 (DEL_ROLE)
 * 3. 角色未变更，新增用户 (ROLE_ADD_USER)
 * 4. 角色未变更，删除用户 (ROLE_DEL_USER)
 */
@Slf4j
@Service
public class RoleUserService {
    
    @Autowired
    private RoleUserHandler roleUserHandler;
    
    @Autowired
    private ErrorHandler errorHandler;
    
    @Autowired
    private ReactiveVirtualGroupSyncLogRepository virtualGroupSyncLogRepository;

    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private DatabaseClient databaseClient;
    
    /**
     * 处理角色用户相关消息
     * 严格按照Python代码逻辑实现
     */
    @Transactional
    public Mono<Boolean> processRoleUserMessage(RoleUserMessage message) {
        String messageType = message.getMessageType();
        log.info("开始处理{}消息: roleId={}, systemName={}", 
                messageType, message.getRoleId(), message.getSystemName());
        
        return roleUserHandler.handleMessage(message)
            .flatMap(success -> {
                if (success) {
                    // 根据消息类型决定记录哪些用户列表
                    ArrayList<String> addList = new ArrayList<>();
                    ArrayList<String> delList = new ArrayList<>();
                    
                    if ("ADD_ROLE".equals(messageType) || "ROLE_ADD_USER".equals(messageType)) {
                        if (message.getAddUserList() != null) {
                            addList.addAll(message.getAddUserList());
                        }
                    }
                    
                    if ("ROLE_DEL_USER".equals(messageType)) {
                        if (message.getDelUserList() != null) {
                            delList.addAll(message.getDelUserList());
                        }
                    }
                    
                    return saveSuccessLog(message, addList, delList)
                        .doOnSuccess(logEntry -> 
                            log.info("{}：Successfully processed and logged for role {}", messageType, message.getRoleId())
                        )
                        .thenReturn(success);
                }
                return Mono.just(success);
            });
    }
    
    /**
     * 保存成功日志
     * 对应Python中的VirtualGroupSyncLog记录逻辑
     */
    private Mono<VirtualGroupSyncLog> saveSuccessLog(RoleUserMessage message, java.util.List<String> addUserList, java.util.List<String> delUserList) {
        String id = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();
        
        String insertSql = "INSERT INTO virtual_group_sync_logs " +
                          "(id, message_task_id, message_type, role_id, role_name, system_name, " +
                          "add_user_list, del_user_list, created_at, updated_at) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return databaseClient.sql(insertSql)
                .bind(0, id)
                .bind(1, message.getMessageTaskId())
                .bind(2, message.getMessageType())
                .bind(3, message.getRoleId())
                .bind(4, message.getRoleName())
                .bind(5, message.getSystemName())
                .bind(6, convertStringListToJsonString(addUserList))
                .bind(7, convertStringListToJsonString(delUserList))
                .bind(8, now)
                .bind(9, now)
                .fetch()
                .rowsUpdated()
                .map(rowsUpdated -> {
                    VirtualGroupSyncLog logEntry = new VirtualGroupSyncLog();
                    logEntry.setId(id);
                    logEntry.setMessageTaskId(message.getMessageTaskId());
                    logEntry.setMessageType(message.getMessageType());
                    logEntry.setRoleId(message.getRoleId());
                    logEntry.setRoleName(message.getRoleName());
                    logEntry.setSystemName(message.getSystemName());
                    logEntry.setCreatedAt(now);
                    logEntry.setUpdatedAt(now);
                    return logEntry;
                });
    }

    /**
     * 将字符串列表转换为JsonNode
     */
    private JsonNode convertStringListToJsonNode(java.util.List<String> stringList) {
        if (stringList == null || stringList.isEmpty()) {
            return objectMapper.createArrayNode();
        }
        return objectMapper.valueToTree(stringList);
    }
    
    /**
     * 将字符串列表转换为JSON字符串，用于DatabaseClient
     */
    private String convertStringListToJsonString(java.util.List<String> stringList) {
        try {
            if (stringList == null || stringList.isEmpty()) {
                return "[]";
            }
            return objectMapper.writeValueAsString(stringList);
        } catch (Exception e) {
            log.error("转换字符串列表为JSON失败", e);
            return "[]";
        }
    }
}
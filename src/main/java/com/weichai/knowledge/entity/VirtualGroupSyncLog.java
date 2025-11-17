package com.weichai.knowledge.entity;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/**
 * 虚拟组同步日志表
 */
@Table("virtual_group_sync_logs")
@Data
public class VirtualGroupSyncLog {
    
    @Id
    private String id;
    
    @Column("message_task_id")
    private String messageTaskId;
    
    @Column("message_type")
    private String messageType;
    
    @Column("role_id")
    private String roleId;
    
    @Column("role_name")
    private String roleName;
    
    @Column("system_name")
    private String systemName;
    
    @Column("add_user_list")
    private JsonNode addUserList;
    
    @Column("del_user_list")
    private JsonNode delUserList;
    
    @Column("created_at")
    private LocalDateTime createdAt;
    
    @Column("updated_at")
    private LocalDateTime updatedAt;
} 
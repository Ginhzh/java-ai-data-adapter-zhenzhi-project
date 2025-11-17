package com.weichai.knowledge.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/**
 * 文件权限同步表
 */
@Table("knowledge_role_sync_logs")
@Data
@EqualsAndHashCode(callSuper = false)
public class KnowledgeRoleSyncLog {
    
    @Id
    @Column("id")
    private String id;
    
    @Column("message_task_id")
    @JsonProperty("message_task_id")
    private String messageTaskId;
    
    @Column("message_type")
    @JsonProperty("message_type")
    private String messageType;
    
    @Column("message_datetime")
    @JsonProperty("message_datetime")
    private String messageDateTime;
    
    @Column("file_id")
    @JsonProperty("file_id")
    private String fileId;
    
    @Column("file_name")
    @JsonProperty("file_name")
    private String fileName;
    
    @Column("system_name")
    @JsonProperty("system_name")
    private String systemName;
    
    @Column("repo_id")
    @JsonProperty("repo_id")
    private String repoId;
    
    @Column("zhenzhi_file_id")
    @JsonProperty("zhenzhi_file_id")
    private String zhenzhiFileId;

    @Column("file_number")
    @JsonProperty("file_number")
    private String fileNumber;

    @Column("add_role_list")
    @JsonProperty("add_role_list")
    private JsonNode addRoleList;
    
    @Column("add_user_list")
    @JsonProperty("add_user_list")
    private JsonNode addUserList;
    
    @Column("del_role_list")
    @JsonProperty("del_role_list")
    private JsonNode delRoleList;
    
    @Column("del_user_list")
    @JsonProperty("del_user_list")
    private JsonNode delUserList;
    
    @Column("created_at")
    @JsonProperty("created_at")
    private LocalDateTime createdAt;
    
    @Column("updated_at")
    @JsonProperty("updated_at")
    private LocalDateTime updatedAt;
} 
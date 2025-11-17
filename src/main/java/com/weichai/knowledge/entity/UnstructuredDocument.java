package com.weichai.knowledge.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 非结构化文档记录表
 */
@Table("unstructured_documents")
@Data
@EqualsAndHashCode(callSuper = false)
public class UnstructuredDocument {
    
    @Id
    @Column("id")
    private String id;
    
    @Column("system_name")
    @JsonProperty("system_name")
    private String systemName;
    
    @Column("file_id")
    @JsonProperty("file_id")
    private String fileId;
    
    @Column("file_number")
    @JsonProperty("file_number")
    private String fileNumber;
    
    @Column("file_name")
    @JsonProperty("file_name")
    private String fileName;
    
    @Column("zhenzhi_file_id")
    @JsonProperty("zhenzhi_file_id")
    private String zhenzhiFileId;
    
    @Column("version")
    private String version;
    
    @Column("status")
    private Integer status = 0;
    
    @Column("role_list")
    @JsonProperty("role_list")
    private JsonNode roleList;
    
    @Column("user_list")
    @JsonProperty("user_list")
    private JsonNode userList;
    
    @Column("repo_id")
    @JsonProperty("repo_id")
    private String repoId;
    
    @Column("created_at")
    @JsonProperty("created_at")
    private LocalDateTime createdAt;
    
    @Column("updated_at")
    @JsonProperty("updated_at")
    private LocalDateTime updatedAt;
    
} 
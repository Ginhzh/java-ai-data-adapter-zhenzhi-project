package com.weichai.knowledge.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/**
 * 文档路径-知识库映射表
 */
@Table("repo_mappings")
@Data
public class RepoMapping {
    
    @Id
    private String id;
    
    @Column("system_name")
    private String systemName;
    
    @Column("file_path")
    private String filePath;
    
    @Column("repo_id")
    private String repoId;
    
    @Column("department_id")
    private String departmentId;
    
    @Column("created_at")
    private LocalDateTime createdAt;
    
    @Column("updated_at")
    private LocalDateTime updatedAt;
} 
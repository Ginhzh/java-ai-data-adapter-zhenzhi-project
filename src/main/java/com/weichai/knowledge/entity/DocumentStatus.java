package com.weichai.knowledge.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/**
 * 文档状态表
 */
@Table("document_status")
@Data
public class DocumentStatus {
    
    @Id
    private String id;
    
    @Column("system_name")
    private String systemName;
    
    @Column("file_id")
    private String fileId;
    
    @Column("file_number")
    private String fileNumber;
    
    @Column("status")
    private Integer status = 0;
    
    @Column("repo_id")
    private String repoId;
    
    @Column("zhenzhi_file_id")
    private String zhenzhiFileId;
    
    @Column("created_at")
    private LocalDateTime createdAt;
    
    @Column("updated_at")
    private LocalDateTime updatedAt;
} 
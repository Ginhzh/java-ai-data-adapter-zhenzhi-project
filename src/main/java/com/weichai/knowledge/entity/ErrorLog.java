package com.weichai.knowledge.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

/**
 * 失败记录表 以供后续补偿使用
 */
@Table("error_logs")
@Data
public class ErrorLog {
    
    @Id
    private Integer id;
    
    @Column("task_id")
    private String taskId;
    
    @Column("name")
    private String name;
    
    @Column("description")
    private String description;
    
    @Column("error_type")
    private Integer errorType;
    
    @Column("error_num")
    private Integer errorNum;
    
    @Column("message_type")
    private String messageType;
    
    @Column("file_source_id")
    private String fileSourceId;
    
    @Column("file_zhenzhi_id")
    private String fileZhenzhiId;
} 
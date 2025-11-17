package com.weichai.knowledge.repository;

import com.weichai.knowledge.dto.KnowledgeRoleSyncStatsDto;
import com.weichai.knowledge.dto.SystemMessageTypeCountDto;
import com.weichai.knowledge.entity.KnowledgeRoleSyncLog;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * 知识库角色同步日志响应式数据访问接口
 */
@Repository
public interface ReactiveKnowledgeRoleSyncLogRepository extends ReactiveCrudRepository<KnowledgeRoleSyncLog, String> {
    
    /**
     * 根据消息任务ID查询同步日志
     */
    Mono<KnowledgeRoleSyncLog> findByMessageTaskId(String messageTaskId);
    
    /**
     * 根据文件ID查询同步日志
     */
    Flux<KnowledgeRoleSyncLog> findByFileIdOrderByCreatedAtDesc(String fileId);
    
    /**
     * 根据系统名称查询同步日志
     */
    Flux<KnowledgeRoleSyncLog> findBySystemNameOrderByCreatedAtDesc(String systemName);
    
    /**
     * 根据甄知文件ID查询同步日志
     */
    Flux<KnowledgeRoleSyncLog> findByZhenzhiFileIdOrderByCreatedAtDesc(String zhenzhiFileId);
    
    /**
     * 根据消息类型查询同步日志
     */
    Flux<KnowledgeRoleSyncLog> findByMessageTypeOrderByCreatedAtDesc(String messageType);
    
    /**
     * 根据时间范围查询同步日志
     */
    @Query("SELECT * FROM knowledge_role_sync_logs WHERE created_at BETWEEN :startTime AND :endTime ORDER BY created_at DESC")
    Flux<KnowledgeRoleSyncLog> findByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 根据系统名称和时间范围查询同步日志
     */
    @Query("SELECT * FROM knowledge_role_sync_logs WHERE system_name = :systemName AND created_at BETWEEN :startTime AND :endTime ORDER BY created_at DESC")
    Flux<KnowledgeRoleSyncLog> findBySystemNameAndCreatedAtBetween(String systemName, LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 统计指定时间范围内的同步日志数量
     */
    @Query("SELECT COUNT(*) FROM knowledge_role_sync_logs WHERE created_at BETWEEN :startTime AND :endTime")
    Mono<Long> countByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 根据系统名称统计同步日志数量
     */
    Mono<Long> countBySystemName(String systemName);
    
    /**
     * 根据消息类型统计同步日志数量
     */
    Mono<Long> countByMessageType(String messageType);
    
    /**
     * 根据消息类型和时间范围查询同步日志
     */
    @Query("SELECT * FROM knowledge_role_sync_logs WHERE message_type = :messageType AND created_at BETWEEN :startTime AND :endTime ORDER BY created_at DESC")
    Flux<KnowledgeRoleSyncLog> findByMessageTypeAndCreatedAtBetween(String messageType, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 按系统名称和消息类型聚合统计同步日志数量 - 返回Object[]用于手动转换为DTO
     * 用于系统每日统计
     */
    @Query("SELECT system_name as systemName, message_type as messageType, COUNT(id) as count " +
           "FROM knowledge_role_sync_logs " +
           "WHERE created_at BETWEEN :startDateTime AND :endDateTime " +
           "GROUP BY system_name, message_type")
    Flux<SystemMessageTypeCountDto> countSyncLogsBySystemAndType(LocalDateTime startDateTime, LocalDateTime endDateTime);

    /**
     * 获取指定系统和消息类型的同步日志记录，用于解析JSON内容
     */
    @Query("SELECT * FROM knowledge_role_sync_logs " +
           "WHERE system_name = :systemName " +
           "AND message_type = :messageType " +
           "AND created_at BETWEEN :startDateTime AND :endDateTime")
    Flux<KnowledgeRoleSyncLog> findSyncLogsBySystemAndMessageType(
            String systemName,
            String messageType,
            LocalDateTime startDateTime,
            LocalDateTime endDateTime);

    /**
     * 统计指定系统和消息类型在指定时间范围内的同步日志数量
     */
    @Query("SELECT COUNT(id) FROM knowledge_role_sync_logs " +
           "WHERE system_name = :systemName " +
           "AND message_type = :messageType " +
           "AND created_at BETWEEN :startDateTime AND :endDateTime")
    Mono<Long> countBySystemAndMessageType(String systemName, String messageType, LocalDateTime startDateTime, LocalDateTime endDateTime);
}
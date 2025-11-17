package com.weichai.knowledge.repository;

import com.weichai.knowledge.dto.MessageTypeCountDto;
import com.weichai.knowledge.dto.SystemMessageTypeCountDto;
import com.weichai.knowledge.dto.VirtualGroupSyncStatsDto;
import com.weichai.knowledge.entity.VirtualGroupSyncLog;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * 虚拟组同步日志响应式数据访问层
 */
@Repository
public interface ReactiveVirtualGroupSyncLogRepository extends ReactiveCrudRepository<VirtualGroupSyncLog, String> {
    
    /**
     * 根据消息任务ID查询日志
     */
    Mono<VirtualGroupSyncLog> findByMessageTaskId(String messageTaskId);
    
    /**
     * 根据角色ID查询日志列表
     */
    Flux<VirtualGroupSyncLog> findByRoleIdOrderByCreatedAtDesc(String roleId);
    
    /**
     * 根据消息类型查询日志列表
     */
    Flux<VirtualGroupSyncLog> findByMessageTypeOrderByCreatedAtDesc(String messageType);
    
    /**
     * 根据系统名称查询日志列表
     */
    Flux<VirtualGroupSyncLog> findBySystemNameOrderByCreatedAtDesc(String systemName);
    
    /**
     * 根据角色ID和消息类型查询日志
     */
    Flux<VirtualGroupSyncLog> findByRoleIdAndMessageTypeOrderByCreatedAtDesc(String roleId, String messageType);
    
    /**
     * 查询指定时间范围内的日志
     */
    @Query("SELECT * FROM virtual_group_sync_logs WHERE created_at BETWEEN :startTime AND :endTime ORDER BY created_at DESC")
    Flux<VirtualGroupSyncLog> findByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 查询最近的日志记录
     */
    @Query("SELECT * FROM virtual_group_sync_logs ORDER BY created_at DESC LIMIT :limit")
    Flux<VirtualGroupSyncLog> findRecentLogs(Long limit);
    
    /**
     * 根据角色ID和时间范围查询日志
     */
    @Query("SELECT * FROM virtual_group_sync_logs WHERE role_id = :roleId AND created_at BETWEEN :startTime AND :endTime ORDER BY created_at DESC")
    Flux<VirtualGroupSyncLog> findByRoleIdAndCreatedAtBetween(String roleId, LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 统计指定时间范围内各消息类型的数量 - 返回Object[]用于手动转换为DTO
     */
    @Query("SELECT message_type, COUNT(*) FROM virtual_group_sync_logs WHERE created_at BETWEEN :startTime AND :endTime GROUP BY message_type")
    Flux<Object[]> countByMessageTypeAndCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 检查是否存在指定的消息任务ID
     */
    Mono<Boolean> existsByMessageTaskId(String messageTaskId);
    
    /**
     * 删除指定时间之前的日志（用于清理历史数据）
     */
    @Query("DELETE FROM virtual_group_sync_logs WHERE created_at < :beforeTime")
    Mono<Integer> deleteByCreatedAtBefore(LocalDateTime beforeTime);

    /**
     * 按系统名称和消息类型聚合统计虚拟组同步日志数量 - 返回Object[]用于手动转换为DTO
     * 用于系统每日统计
     */
    @Query("SELECT system_name as systemName, message_type as messageType, COUNT(id) as count " +
           "FROM virtual_group_sync_logs " +
           "WHERE created_at BETWEEN :startDateTime AND :endDateTime " +
           "GROUP BY system_name, message_type")
    Flux<SystemMessageTypeCountDto> countVirtualGroupSyncLogsBySystemAndType(LocalDateTime startDateTime, LocalDateTime endDateTime);

    /**
     * 获取指定系统和消息类型的虚拟组同步日志记录，用于解析JSON内容
     */
    @Query("SELECT * FROM virtual_group_sync_logs " +
           "WHERE system_name = :systemName " +
           "AND message_type = :messageType " +
           "AND created_at BETWEEN :startDateTime AND :endDateTime")
    Flux<VirtualGroupSyncLog> findVirtualGroupSyncLogsBySystemAndMessageType(
            String systemName,
            String messageType,
            LocalDateTime startDateTime,
            LocalDateTime endDateTime);

    /**
     * 统计指定系统和消息类型在指定时间范围内的虚拟组同步日志数量
     */
    @Query("SELECT COUNT(id) FROM virtual_group_sync_logs " +
           "WHERE system_name = :systemName " +
           "AND message_type = :messageType " +
           "AND created_at BETWEEN :startDateTime AND :endDateTime")
    Mono<Long> countBySystemAndMessageType(String systemName, String messageType, LocalDateTime startDateTime, LocalDateTime endDateTime);

    /**
     * 根据系统和消息类型查询虚拟组同步日志
     */
    @Query("SELECT * FROM virtual_group_sync_logs " +
           "WHERE system_name = :systemName " +
           "AND message_type = :messageType " +
           "AND created_at BETWEEN :startDateTime AND :endDateTime")
    Flux<VirtualGroupSyncLog> findBySystemAndMessageType(String systemName, String messageType, LocalDateTime startDateTime, LocalDateTime endDateTime);
}
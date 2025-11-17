package com.weichai.knowledge.repository;

import com.weichai.knowledge.dto.KafkaMessageStatsDto;
import com.weichai.knowledge.dto.SystemMessageTypeCountDto;
import com.weichai.knowledge.entity.KafkaMessageLog;
import org.springframework.data.r2dbc.repository.Query; 
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Kafka消息日志Repository (响应式)
 */
@Repository
public interface KafkaMessageLogRepository extends R2dbcRepository<KafkaMessageLog, String> {

    Flux<KafkaMessageLog> findBySystemName(String systemName);

    Flux<KafkaMessageLog> findByMessageType(String messageType);

    Flux<KafkaMessageLog> findBySystemNameAndMessageType(String systemName, String messageType);

    /**
     * 根据时间范围查询消息日志
     */
    @Query("SELECT * FROM kafka_message_logs k WHERE k.created_at BETWEEN :startTime AND :endTime ORDER BY k.created_at DESC")
    Flux<KafkaMessageLog> findByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);

    @Query("SELECT * FROM kafka_message_logs k ORDER BY k.created_at DESC")
    Flux<KafkaMessageLog> findRecentLogs();

    @Query("SELECT * FROM kafka_message_logs k WHERE k.message_type = :messageType AND k.created_at BETWEEN :startTime AND :endTime ORDER BY k.created_at DESC")
    Flux<KafkaMessageLog> findByMessageTypeAndCreatedAtBetween(String messageType, LocalDateTime startTime, LocalDateTime endTime);

    @Query("SELECT DISTINCT k.system_name FROM kafka_message_logs k WHERE k.message_type = :messageType AND k.created_at BETWEEN :startTime AND :endTime")
    Flux<String> findDistinctSystemNamesByMessageTypeAndCreatedAtBetween(String messageType, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 查询未成功接入的FILE_ADD消息日志
     */
    @Query("SELECT * FROM kafka_message_logs k WHERE k.message_type = 'FILE_ADD' " +
           "AND (:systemName IS NULL OR k.system_name = :systemName) " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime " +
           "AND k.file_number NOT IN (" +
           "  SELECT u.file_number FROM unstructured_documents u " +
           "  WHERE u.file_number IS NOT NULL AND (" +
           "    u.created_at BETWEEN :startDateTime AND :endDateTime OR " +
           "    u.updated_at BETWEEN :startDateTime AND :endDateTime" +
           "  )" +
           ")")
    Flux<KafkaMessageLog> findFailedIngestionLogs(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime, Pageable pageable);

    /**
     * 查询FILE_ADD类型的消息日志，支持系统名称过滤和分页
     */
    @Query("SELECT * FROM kafka_message_logs k WHERE k.message_type = 'FILE_ADD' " +
           "AND (:systemName IS NULL OR k.system_name = :systemName) " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime " +
           "ORDER BY k.created_at DESC")
    Flux<KafkaMessageLog> findFileAddLogs(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime, Pageable pageable);

    /**
     * 查询FILE_DEL类型的消息日志，支持系统名称过滤和分页
     */
    @Query("SELECT * FROM kafka_message_logs k WHERE k.message_type = 'FILE_DEL' " +
           "AND (:systemName IS NULL OR k.system_name = :systemName) " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime " +
           "ORDER BY k.created_at DESC")
    Flux<KafkaMessageLog> findFileDeletionLogs(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime, Pageable pageable);

    @Query("SELECT * FROM kafka_message_logs k WHERE k.message_type = 'FILE_ADD' " +
           "AND k.system_name = :systemName " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime " +
           "ORDER BY k.created_at DESC")
    Flux<KafkaMessageLog> findFileAddLogsBySystem(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime);

    @Query("SELECT COUNT(k.id) FROM kafka_message_logs k WHERE k.message_type = 'FILE_ADD' " +
           "AND (:systemName IS NULL OR k.system_name = :systemName) " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime")
    Mono<Long> countFileAddLogs(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime);

    @Query("SELECT COUNT(k.id) FROM kafka_message_logs k WHERE k.message_type = 'FILE_DEL' " +
           "AND (:systemName IS NULL OR k.system_name = :systemName) " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime")
    Mono<Long> countFileDeletionLogs(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime);

    /**
     * 按系统名称和消息类型聚合统计当天的消息数量 - 返回Object[]用于手动转换为DTO
     */
    @Query("SELECT k.system_name as systemName, k.message_type as messageType, COUNT(k.id) as count " +
           "FROM kafka_message_logs k " +
           "WHERE k.created_at BETWEEN :startDateTime AND :endDateTime " +
           "GROUP BY k.system_name, k.message_type")
    Flux<SystemMessageTypeCountDto> countMessagesBySystemAndType(LocalDateTime startDateTime, LocalDateTime endDateTime);

    @Query("SELECT DISTINCT k.system_name FROM kafka_message_logs k " +
           "WHERE k.created_at BETWEEN :startDateTime AND :endDateTime")
    Flux<String> findDistinctSystemNames(LocalDateTime startDateTime, LocalDateTime endDateTime);

    @Query("SELECT * FROM kafka_message_logs k " +
           "WHERE k.system_name = :systemName " +
           "AND k.message_type = :messageType " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime")
    Flux<KafkaMessageLog> findBySystemAndMessageType(String systemName, String messageType, LocalDateTime startDateTime, LocalDateTime endDateTime);

    /**
     * 统计指定系统和消息类型在指定时间范围内的消息数量
     */
    @Query("SELECT COUNT(k.id) FROM kafka_message_logs k " +
           "WHERE k.system_name = :systemName " +
           "AND k.message_type = :messageType " +
           "AND k.created_at BETWEEN :startDateTime AND :endDateTime")
    Mono<Long> countBySystemAndMessageType(String systemName, String messageType, LocalDateTime startDateTime, LocalDateTime endDateTime);
}
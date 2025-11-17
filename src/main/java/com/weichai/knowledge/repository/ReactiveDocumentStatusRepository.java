package com.weichai.knowledge.repository;

import com.weichai.knowledge.entity.DocumentStatus;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * 文档状态响应式数据访问接口
 * 使用 R2DBC 实现非阻塞数据库访问
 */
@Repository
public interface ReactiveDocumentStatusRepository extends ReactiveCrudRepository<DocumentStatus, String> {
    
    /**
     * 根据文件编号查询文档状态
     */
    Mono<DocumentStatus> findByFileNumber(String fileNumber);
    
    /**
     * 根据系统名称和文件ID查询文档状态
     */
    Mono<DocumentStatus> findBySystemNameAndFileId(String systemName, String fileId);
    
    /**
     * 根据系统名称查询所有文档状态
     */
    Flux<DocumentStatus> findBySystemName(String systemName);
    
    /**
     * 根据状态查询文档
     */
    Flux<DocumentStatus> findByStatus(Integer status);
    
    /**
     * 根据知识库ID查询文档状态
     */
    Flux<DocumentStatus> findByRepoId(String repoId);
    
    /**
     * 根据甄知文件ID查询文档状态
     */
    Mono<DocumentStatus> findByZhenzhiFileId(String zhenzhiFileId);
    
    /**
     * 根据系统名称和状态查询文档状态
     */
    Flux<DocumentStatus> findBySystemNameAndStatus(String systemName, Integer status);
    
    /**
     * 根据时间范围查询文档状态
     */
    @Query("SELECT * FROM document_status WHERE created_at BETWEEN :startTime AND :endTime ORDER BY created_at DESC")
    Flux<DocumentStatus> findByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 根据更新时间范围查询文档状态
     */
    @Query("SELECT * FROM document_status WHERE updated_at BETWEEN :startTime AND :endTime ORDER BY updated_at DESC")
    Flux<DocumentStatus> findByUpdatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 检查指定文件编号是否存在
     */
    Mono<Boolean> existsByFileNumber(String fileNumber);
    
    /**
     * 检查指定系统和文件ID的状态是否存在
     */
    Mono<Boolean> existsBySystemNameAndFileId(String systemName, String fileId);
    
    /**
     * 根据系统名称统计文档数量
     */
    Mono<Long> countBySystemName(String systemName);
    
    /**
     * 根据状态统计文档数量
     */
    Mono<Long> countByStatus(Integer status);
    
    /**
     * 根据系统名称和状态统计文档数量
     */
    Mono<Long> countBySystemNameAndStatus(String systemName, Integer status);
    
    /**
     * 批量更新状态
     */
    @Query("UPDATE document_status SET status = :status, updated_at = :updatedAt WHERE id IN (:ids)")
    Mono<Integer> batchUpdateStatus(Integer status, LocalDateTime updatedAt, Flux<String> ids);
    
    /**
     * 根据甄知文件ID更新状态
     */
    @Query("UPDATE document_status SET status = :status, updated_at = :updatedAt WHERE zhenzhi_file_id = :zhenzhiFileId")
    Mono<Integer> updateStatusByZhenzhiFileId(Integer status, LocalDateTime updatedAt, String zhenzhiFileId);
    
    /**
     * 删除指定时间之前的旧记录
     */
    @Query("DELETE FROM document_status WHERE created_at < :beforeTime")
    Mono<Integer> deleteOldRecords(LocalDateTime beforeTime);
}
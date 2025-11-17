package com.weichai.knowledge.repository;

import com.weichai.knowledge.entity.UnstructuredDocument;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * 非结构化文档响应式数据访问接口
 * 使用 R2DBC 实现非阻塞数据库访问
 */
@Repository
public interface ReactiveUnstructuredDocumentRepository extends ReactiveCrudRepository<UnstructuredDocument, String> {
    
    /**
     * 根据系统名称和文件ID查询文档
     */
    Mono<UnstructuredDocument> findBySystemNameAndFileId(String systemName, String fileId);
    
    /**
     * 根据系统名称和文件编号查询文档
     */
    Mono<UnstructuredDocument> findBySystemNameAndFileNumber(String systemName, String fileNumber);
    
    /**
     * 根据文件编号和系统名称查询文档
     */
    Mono<UnstructuredDocument> findByFileNumberAndSystemName(String fileNumber, String systemName);
    
    /**
     * 根据甄知文件ID查询文档
     */
    Mono<UnstructuredDocument> findByZhenzhiFileId(String zhenzhiFileId);
    
    /**
     * 查询指定状态的文档
     */
    Flux<UnstructuredDocument> findByStatus(Integer status);
    
    /**
     * 查询指定状态且甄知文件ID不为空的文档
     */
    @Query("SELECT * FROM unstructured_documents WHERE status = :status AND zhenzhi_file_id IS NOT NULL")
    Flux<UnstructuredDocument> findByStatusAndZhenzhiFileIdNotNull(Integer status);

    /**
     * 分页查询指定状态且甄知文件ID不为空的文档
     */
    @Query("SELECT * FROM unstructured_documents WHERE status = :status AND zhenzhi_file_id IS NOT NULL ORDER BY created_at ASC LIMIT :limit OFFSET :offset")
    Flux<UnstructuredDocument> findByStatusAndZhenzhiFileIdNotNullOrderByCreatedAt(Integer status, Long limit, Long offset);
    
    /**
     * 统计指定状态且甄知文件ID不为空的文档数量
     */
    @Query("SELECT COUNT(*) FROM unstructured_documents WHERE status = :status AND zhenzhi_file_id IS NOT NULL")
    Mono<Long> countByStatusAndZhenzhiFileIdNotNull(Integer status);
    
    /**
     * 根据系统名称查询文档
     */
    Flux<UnstructuredDocument> findBySystemNameOrderByCreatedAtDesc(String systemName);
    
    /**
     * 根据时间范围查询文档
     */
    @Query("SELECT * FROM unstructured_documents WHERE created_at BETWEEN :startTime AND :endTime ORDER BY created_at DESC")
    Flux<UnstructuredDocument> findByCreatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 批量更新文档状态
     */
    @Query("UPDATE unstructured_documents SET status = :status, updated_at = :updatedAt WHERE id IN (:ids)")
    Mono<Integer> batchUpdateStatus(Integer status, LocalDateTime updatedAt, Flux<String> ids);

    /**
     * 根据甄知文件ID更新状态
     */
    @Query("UPDATE unstructured_documents SET status = :status, updated_at = :updatedAt WHERE zhenzhi_file_id = :zhenzhiFileId")
    Mono<Integer> updateStatusByZhenzhiFileId(Integer status, LocalDateTime updatedAt, String zhenzhiFileId);
    
    /**
     * 根据系统名称和状态查询文档数量
     */
    Mono<Long> countBySystemNameAndStatus(String systemName, Integer status);
    
    /**
     * 根据知识库ID查询文档
     */
    Flux<UnstructuredDocument> findByRepoIdOrderByCreatedAtDesc(String repoId);
    
    /**
     * 查询最近更新的文档
     */
    @Query("SELECT * FROM unstructured_documents ORDER BY updated_at DESC LIMIT :limit")
    Flux<UnstructuredDocument> findRecentlyUpdated(Long limit);

    /**
     * 检查指定甄知文件ID是否存在
     */
    Mono<Boolean> existsByZhenzhiFileId(String zhenzhiFileId);

    /**
     * 根据甄知文件ID列表查询文档
     */
    @Query("SELECT * FROM unstructured_documents WHERE zhenzhi_file_id IN (:zhenzhiFileIds)")
    Flux<UnstructuredDocument> findByZhenzhiFileIdIn(Flux<String> zhenzhiFileIds);

    /**
     * 根据文件编号列表查询文档
     */
    @Query("SELECT * FROM unstructured_documents WHERE file_number IN (:fileNumbers)")
    Flux<UnstructuredDocument> findByFileNumberIn(Flux<String> fileNumbers);
    
    /**
     * 查询成功的文档（用于统计成功接入量）
     */
    @Query("SELECT * FROM unstructured_documents WHERE file_number IN (:fileNumbers) AND " +
           "((created_at BETWEEN :startTime AND :endTime) " +
           "OR (created_at < :startTime AND updated_at BETWEEN :startTime AND :endTime))")
    Flux<UnstructuredDocument> findSuccessDocumentsByFileNumbers(Flux<String> fileNumbers,
                                                                LocalDateTime startTime,
                                                                LocalDateTime endTime);
    
    /**
     * 根据更新时间范围查询文档
     */
    @Query("SELECT * FROM unstructured_documents WHERE updated_at BETWEEN :startTime AND :endTime ORDER BY updated_at DESC")
    Flux<UnstructuredDocument> findByUpdatedAtBetween(LocalDateTime startTime, LocalDateTime endTime);
}
package com.weichai.knowledge.repository;

import com.weichai.knowledge.entity.RepoMapping;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 知识库映射响应式数据访问接口
 * 使用 R2DBC 实现非阻塞数据库访问
 */
@Repository
public interface ReactiveRepoMappingRepository extends ReactiveCrudRepository<RepoMapping, String> {
    
    /**
     * 根据系统名称和文件路径查询映射
     */
    Mono<RepoMapping> findBySystemNameAndFilePath(String systemName, String filePath);
    
    /**
     * 根据系统名称查询所有映射
     */
    Flux<RepoMapping> findBySystemName(String systemName);
    
    /**
     * 根据知识库ID查询映射
     */
    Flux<RepoMapping> findByRepoId(String repoId);
    
    /**
     * 根据部门ID查询映射
     */
    Flux<RepoMapping> findByDepartmentId(String departmentId);
    
    /**
     * 检查指定系统和文件路径的映射是否存在
     */
    Mono<Boolean> existsBySystemNameAndFilePath(String systemName, String filePath);
    
    /**
     * 根据知识库ID删除映射
     */
    Mono<Void> deleteByRepoId(String repoId);
    
    /**
     * 根据系统名称删除映射
     */
    Mono<Void> deleteBySystemName(String systemName);
}
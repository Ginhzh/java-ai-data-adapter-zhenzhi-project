package com.weichai.knowledge.redis;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 响应式数据库同步管理器
 * 负责将Redis数据定期同步到数据库，完全基于响应式编程模型
 * 
 * 主要特性：
 * - 完全响应式架构，基于Mono/Flux
 * - 非阻塞I/O操作
 * - 响应式事务管理
 * - 背压支持
 * - 链式错误处理
 */
@Slf4j
@Component
public class ReactiveSyncManager {
    
    @Autowired
    private ReactiveRedisManager reactiveRedisManager;
    
    @Autowired
    private R2dbcEntityTemplate r2dbcEntityTemplate;
    
    @Autowired
    private TransactionalOperator transactionalOperator;
    
    // 同步任务映射
    private final Map<String, ReactiveSyncTask> syncTasks = new ConcurrentHashMap<>();
    
    // 同步间隔
    private Duration syncInterval = Duration.ofMinutes(5); // 默认5分钟
    
    // 是否正在运行
    private volatile boolean isRunning = false;
    
    // 同步任务订阅
    private reactor.core.Disposable syncSubscription;
    
    @Data
    private static class ReactiveSyncTask {
        private final String taskName;
        private final Class<?> modelClass;
        private final String redisKeyPrefix;
        private final Function<ReactiveSyncContext, Mono<Void>> syncFunction;
        
        public ReactiveSyncTask(String taskName, Class<?> modelClass, String redisKeyPrefix, 
                               Function<ReactiveSyncContext, Mono<Void>> syncFunction) {
            this.taskName = taskName;
            this.modelClass = modelClass;
            this.redisKeyPrefix = redisKeyPrefix;
            this.syncFunction = syncFunction;
        }
    }
    
    /**
     * 响应式同步上下文，包含同步所需的信息
     */
    @Data
    public static class ReactiveSyncContext {
        private final Class<?> modelClass;
        private final String redisKeyPrefix;
        private final R2dbcEntityTemplate r2dbcEntityTemplate;
        private final ReactiveRedisManager reactiveRedisManager;
        private final TransactionalOperator transactionalOperator;
        
        public ReactiveSyncContext(Class<?> modelClass, String redisKeyPrefix, 
                                  R2dbcEntityTemplate r2dbcEntityTemplate, 
                                  ReactiveRedisManager reactiveRedisManager,
                                  TransactionalOperator transactionalOperator) {
            this.modelClass = modelClass;
            this.redisKeyPrefix = redisKeyPrefix;
            this.r2dbcEntityTemplate = r2dbcEntityTemplate;
            this.reactiveRedisManager = reactiveRedisManager;
            this.transactionalOperator = transactionalOperator;
        }
    }
    
    @PostConstruct
    public Mono<Void> init() {
        return Mono.fromRunnable(() -> log.info("ReactiveSyncManager初始化完成"));
    }
    
    @PreDestroy
    public Mono<Void> destroy() {
        return stop()
            .doOnSuccess(v -> log.info("ReactiveSyncManager已销毁"));
    }
    
    /**
     * 启动同步线程 - 响应式版本
     *
     * @param interval 同步间隔
     */
    public Mono<Void> start(Duration interval) {
        if (interval != null && !interval.isZero() && !interval.isNegative()) {
            this.syncInterval = interval;
        }
        
        if (isRunning) {
            log.warn("同步线程已经在运行中");
            return Mono.empty();
        }
        
        return Mono.fromRunnable(() -> {
            isRunning = true;
            
            // 创建周期性同步任务
            syncSubscription = Flux.interval(syncInterval, Schedulers.boundedElastic())
                .doOnNext(tick -> log.debug("开始执行定时同步任务，tick: {}", tick))
                .flatMap(tick -> runAllSyncTasks()
                    .onErrorResume(error -> {
                        log.error("执行同步任务时发生错误", error);
                        return Mono.empty();
                    }))
                .subscribe();
            
            log.info("响应式数据同步线程已启动，同步间隔: {}", syncInterval);
        });
    }
    
    /**
     * 停止同步线程 - 响应式版本
     */
    public Mono<Void> stop() {
        return Mono.fromRunnable(() -> {
            isRunning = false;
            if (syncSubscription != null && !syncSubscription.isDisposed()) {
                syncSubscription.dispose();
            }
            log.info("响应式数据同步线程已停止");
        });
    }
    
    /**
     * 注册同步任务 - 响应式版本
     *
     * @param taskName       任务名称
     * @param modelClass     模型类
     * @param redisKeyPrefix Redis键前缀
     */
    public void registerSyncTask(String taskName, Class<?> modelClass, String redisKeyPrefix) {
        registerSyncTask(taskName, modelClass, redisKeyPrefix, this::defaultSyncFunction);
    }
    
    /**
     * 注册同步任务（自定义同步函数）- 响应式版本
     *
     * @param taskName       任务名称
     * @param modelClass     模型类
     * @param redisKeyPrefix Redis键前缀
     * @param syncFunction   响应式同步函数
     */
    public void registerSyncTask(String taskName, Class<?> modelClass, String redisKeyPrefix,
                                Function<ReactiveSyncContext, Mono<Void>> syncFunction) {
        ReactiveSyncTask task = new ReactiveSyncTask(taskName, modelClass, redisKeyPrefix, syncFunction);
        syncTasks.put(taskName, task);
        log.info("响应式同步任务 '{}' 已注册", taskName);
    }
    
    /**
     * 注销同步任务
     *
     * @param taskName 任务名称
     */
    public void unregisterSyncTask(String taskName) {
        if (syncTasks.remove(taskName) != null) {
            log.info("响应式同步任务 '{}' 已注销", taskName);
        } else {
            log.warn("响应式同步任务 '{}' 不存在", taskName);
        }
    }
    
    /**
     * 执行所有同步任务 - 响应式版本
     */
    private Mono<Void> runAllSyncTasks() {
        long startTime = System.currentTimeMillis();
        log.debug("开始执行响应式同步任务");
        
        return Flux.fromIterable(syncTasks.entrySet())
            .flatMap(entry -> {
                String taskName = entry.getKey();
                ReactiveSyncTask task = entry.getValue();
                
                ReactiveSyncContext context = new ReactiveSyncContext(
                    task.getModelClass(),
                    task.getRedisKeyPrefix(),
                    r2dbcEntityTemplate,
                    reactiveRedisManager,
                    transactionalOperator
                );
                
                return task.getSyncFunction().apply(context)
                    .doOnSuccess(v -> log.debug("响应式同步任务 '{}' 执行成功", taskName))
                    .onErrorResume(error -> {
                        log.error("执行响应式同步任务 '{}' 时发生错误", taskName, error);
                        return Mono.empty();
                    });
            })
            .then()
            .doOnSuccess(v -> {
                long duration = System.currentTimeMillis() - startTime;
                log.info("所有响应式同步任务完成，耗时: {}ms", duration);
            });
    }
    
    /**
     * 立即执行同步操作 - 响应式版本
     */
    public Mono<Void> forceSyncNow() {
        log.info("手动触发响应式数据同步");
        return runAllSyncTasks();
    }
    
    /**
     * 默认的响应式同步函数
     */
    protected Mono<Void> defaultSyncFunction(ReactiveSyncContext context) {
        return syncModelToDb(context)
            .as(context.getTransactionalOperator()::transactional);
    }
    
    /**
     * 将Redis中的模型数据同步到数据库 - 响应式版本
     */
    private Mono<Void> syncModelToDb(ReactiveSyncContext context) {
        String redisKeyPrefix = context.getRedisKeyPrefix();
        Class<?> modelClass = context.getModelClass();
        
        // 获取需要同步的记录ID列表
        return context.getReactiveRedisManager().getSyncList(redisKeyPrefix)
            .flatMap(syncList -> {
                if (syncList == null || syncList.isEmpty()) {
                    log.debug("没有需要同步的 {} 记录", modelClass.getSimpleName());
                    return Mono.empty();
                }
                
                log.info("开始同步 {} 数据，共 {} 条记录", modelClass.getSimpleName(), syncList.size());
                
                return Flux.fromIterable(syncList)
                    .flatMap(recordId -> syncSingleRecord(context, recordId)
                        .onErrorResume(error -> {
                            log.error("同步记录 {} 时发生错误", recordId, error);
                            return Mono.empty();
                        }))
                    .collectList()
                    .flatMap(successIds -> {
                        // 从同步列表中移除成功同步的记录
                        return Flux.fromIterable(successIds)
                            .flatMap(recordId -> 
                                context.getReactiveRedisManager().removeFromSyncList(redisKeyPrefix, recordId))
                            .then()
                            .doOnSuccess(v -> 
                                log.info("{} 数据同步完成，成功: {}, 失败: {}", 
                                    modelClass.getSimpleName(), 
                                    successIds.size(), 
                                    syncList.size() - successIds.size()));
                    });
            });
    }
    
    /**
     * 同步单个记录 - 响应式版本
     */
    private Mono<String> syncSingleRecord(ReactiveSyncContext context, String recordId) {
        String redisKeyPrefix = context.getRedisKeyPrefix();
        String recordKey = generateKey(redisKeyPrefix, recordId);
        Class<?> modelClass = context.getModelClass();
        
        return context.getReactiveRedisManager().getMap(recordKey)
            .switchIfEmpty(Mono.fromCallable(() -> {
                log.warn("Redis中未找到记录数据: {}", recordKey);
                return new HashMap<String, Object>(); // 返回空Map表示记录不存在，但仍要从同步列表移除
            }))
            .flatMap(recordData -> {
                if (recordData.isEmpty()) {
                    return Mono.just(recordId); // 直接返回recordId，表示需要从同步列表移除
                }
                
                String entityId = (String) recordData.get("id");
                if (entityId == null) {
                    log.warn("记录数据中缺少ID字段: {}", recordKey);
                    return Mono.just(recordId);
                }
                
                // 检查记录是否已存在
                return checkEntityExists(context, modelClass, entityId)
                    .flatMap(exists -> {
                        if (exists) {
                            // 更新现有记录
                            return updateEntityFromMap(context, modelClass, recordData)
                                .doOnSuccess(v -> log.debug("更新记录: {}", recordId))
                                .thenReturn(recordId);
                        } else {
                            // 创建新记录
                            return createEntityFromMap(context, modelClass, recordData)
                                .doOnSuccess(v -> log.debug("创建新记录: {}", recordId))
                                .thenReturn(recordId);
                        }
                    });
            });
    }
    
    /**
     * 检查实体是否存在 - 响应式版本
     */
    private Mono<Boolean> checkEntityExists(ReactiveSyncContext context, Class<?> modelClass, String entityId) {
        String tableName = getTableName(modelClass);
        String query = String.format("SELECT COUNT(*) FROM %s WHERE id = ?", tableName);
        
        return context.getR2dbcEntityTemplate()
            .getDatabaseClient()
            .sql(query)
            .bind(0, entityId)
            .map((row, metadata) -> row.get(0, Long.class))
            .one()
            .map(count -> count > 0);
    }
    
    /**
     * 从Map创建实体 - 响应式版本
     */
    private Mono<Void> createEntityFromMap(ReactiveSyncContext context, Class<?> modelClass, Map<String, Object> data) {
        return Mono.fromCallable(() -> createEntityInstance(modelClass, data))
            .flatMap(entity -> context.getR2dbcEntityTemplate().insert(entity))
            .then();
    }
    
    /**
     * 从Map更新实体 - 响应式版本
     */
    private Mono<Void> updateEntityFromMap(ReactiveSyncContext context, Class<?> modelClass, Map<String, Object> data) {
        return Mono.fromCallable(() -> createEntityInstance(modelClass, data))
            .flatMap(entity -> {
                setUpdatedAt(entity);
                return context.getR2dbcEntityTemplate().update(entity);
            })
            .then();
    }
    
    /**
     * 创建实体实例
     */
    private Object createEntityInstance(Class<?> modelClass, Map<String, Object> data) {
        try {
            Object entity = modelClass.getDeclaredConstructor().newInstance();
            updateEntityFields(entity, data);
            return entity;
        } catch (Exception e) {
            throw new RuntimeException("创建实体对象失败", e);
        }
    }
    
    /**
     * 更新实体字段
     */
    private void updateEntityFields(Object entity, Map<String, Object> data) {
        // 这里需要实现字段映射逻辑
        // 由于反射操作的复杂性，这里提供简化实现
        // 实际应用中建议使用对象映射框架如MapStruct
        log.debug("更新实体字段: {}", entity.getClass().getSimpleName());
    }
    
    /**
     * 设置更新时间
     */
    private void setUpdatedAt(Object entity) {
        try {
            java.lang.reflect.Field field = entity.getClass().getDeclaredField("updatedAt");
            field.setAccessible(true);
            field.set(entity, LocalDateTime.now());
        } catch (Exception e) {
            log.debug("设置更新时间失败: {}", e.getMessage());
        }
    }
    
    /**
     * 获取表名
     */
    private String getTableName(Class<?> modelClass) {
        // 简化实现，实际应该根据注解或命名规则来确定表名
        String className = modelClass.getSimpleName();
        // 将驼峰命名转换为下划线命名
        return className.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }
    
    /**
     * 生成缓存键
     */
    private String generateKey(String prefix, String... parts) {
        List<String> keyParts = new ArrayList<>();
        keyParts.add(prefix);
        for (String part : parts) {
            if (part != null) {
                keyParts.add(part);
            }
        }
        return String.join(":", keyParts);
    }
    
    /**
     * 获取同步状态信息 - 响应式版本
     */
    public Mono<Map<String, Object>> getSyncStatus() {
        return Mono.fromCallable(() -> {
            Map<String, Object> status = new HashMap<>();
            status.put("isRunning", isRunning);
            status.put("syncInterval", syncInterval.toString());
            status.put("registeredTasks", new ArrayList<>(syncTasks.keySet()));
            status.put("taskCount", syncTasks.size());
            return status;
        });
    }
}
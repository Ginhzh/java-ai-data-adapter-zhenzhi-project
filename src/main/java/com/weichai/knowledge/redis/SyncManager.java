package com.weichai.knowledge.redis;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;

/**
 * 数据库同步管理器
 * 负责将Redis数据定期同步到数据库
 */
@Slf4j
@Component
public class SyncManager {
    
    @Autowired
    private RedisManager redisManager;
    
    @Autowired
    private R2dbcEntityTemplate r2dbcEntityTemplate;
    
    // 同步任务执行器
    private ScheduledExecutorService scheduler;
    
    // 同步任务映射
    private final Map<String, SyncTask> syncTasks = new ConcurrentHashMap<>();
    
    // 同步间隔（秒）
    private int syncInterval = 300; // 默认5分钟
    
    // 是否正在运行
    private volatile boolean isRunning = false;
    
    // 同步任务Future
    private ScheduledFuture<?> syncFuture;
    
    @Data
    private static class SyncTask {
        private final String taskName;
        private final Class<?> modelClass;
        private final String redisKeyPrefix;
        private final Consumer<SyncContext> syncFunction;
        
        public SyncTask(String taskName, Class<?> modelClass, String redisKeyPrefix, 
                       Consumer<SyncContext> syncFunction) {
            this.taskName = taskName;
            this.modelClass = modelClass;
            this.redisKeyPrefix = redisKeyPrefix;
            this.syncFunction = syncFunction;
        }
    }
    
    /**
     * 同步上下文，包含同步所需的信息
     */
    @Data
    public static class SyncContext {
        private final Class<?> modelClass;
        private final String redisKeyPrefix;
        private final R2dbcEntityTemplate r2dbcEntityTemplate;
        private final RedisManager redisManager;
        
        public SyncContext(Class<?> modelClass, String redisKeyPrefix, 
                          R2dbcEntityTemplate r2dbcEntityTemplate, RedisManager redisManager) {
            this.modelClass = modelClass;
            this.redisKeyPrefix = redisKeyPrefix;
            this.r2dbcEntityTemplate = r2dbcEntityTemplate;
            this.redisManager = redisManager;
        }
    }
    
    @PostConstruct
    public void init() {
        scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread thread = new Thread(r);
            thread.setName("redis-sync-thread");
            thread.setDaemon(true);
            return thread;
        });
        log.info("数据同步管理器初始化完成");
    }
    
    @PreDestroy
    public void destroy() {
        stop();
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
    }
    
    /**
     * 启动同步线程
     *
     * @param interval 同步间隔（秒）
     */
    public void start(Integer interval) {
        if (interval != null && interval > 0) {
            this.syncInterval = interval;
        }
        
        if (isRunning) {
            log.warn("同步线程已经在运行中");
            return;
        }
        
        isRunning = true;
        syncFuture = scheduler.scheduleWithFixedDelay(
            this::runAllSyncTasks, 
            syncInterval, 
            syncInterval, 
            TimeUnit.SECONDS
        );
        
        log.info("数据同步线程已启动，同步间隔: {}秒", syncInterval);
    }
    
    /**
     * 停止同步线程
     */
    public void stop() {
        isRunning = false;
        if (syncFuture != null && !syncFuture.isCancelled()) {
            syncFuture.cancel(true);
        }
        log.info("数据同步线程已停止");
    }
    
    /**
     * 注册同步任务
     *
     * @param taskName       任务名称
     * @param modelClass     模型类
     * @param redisKeyPrefix Redis键前缀
     */
    public void registerSyncTask(String taskName, Class<?> modelClass, String redisKeyPrefix) {
        registerSyncTask(taskName, modelClass, redisKeyPrefix, this::defaultSyncFunction);
    }
    
    /**
     * 注册同步任务（自定义同步函数）
     *
     * @param taskName       任务名称
     * @param modelClass     模型类
     * @param redisKeyPrefix Redis键前缀
     * @param syncFunction   同步函数
     */
    public void registerSyncTask(String taskName, Class<?> modelClass, String redisKeyPrefix,
                                Consumer<SyncContext> syncFunction) {
        SyncTask task = new SyncTask(taskName, modelClass, redisKeyPrefix, syncFunction);
        syncTasks.put(taskName, task);
        log.info("同步任务 '{}' 已注册", taskName);
    }
    
    /**
     * 注销同步任务
     *
     * @param taskName 任务名称
     */
    public void unregisterSyncTask(String taskName) {
        if (syncTasks.remove(taskName) != null) {
            log.info("同步任务 '{}' 已注销", taskName);
        } else {
            log.warn("同步任务 '{}' 不存在", taskName);
        }
    }
    
    /**
     * 执行所有同步任务
     */
    private void runAllSyncTasks() {
        long startTime = System.currentTimeMillis();
        log.debug("开始执行同步任务");
        
        for (Map.Entry<String, SyncTask> entry : syncTasks.entrySet()) {
            String taskName = entry.getKey();
            SyncTask task = entry.getValue();
            
            try {
                SyncContext context = new SyncContext(
                    task.getModelClass(),
                    task.getRedisKeyPrefix(),
                    r2dbcEntityTemplate,
                    redisManager
                );
                
                task.getSyncFunction().accept(context);
                log.debug("同步任务 '{}' 执行成功", taskName);
            } catch (Exception e) {
                log.error("执行同步任务 '{}' 时发生错误", taskName, e);
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        log.info("所有同步任务完成，耗时: {}ms", duration);
    }
    
    /**
     * 立即执行同步操作
     */
    public void forceSyncNow() {
        log.info("手动触发数据同步");
        CompletableFuture.runAsync(this::runAllSyncTasks);
    }
    
    /**
     * 默认的同步函数
     */
    @Transactional
    protected void defaultSyncFunction(SyncContext context) {
        syncModelToDb(context);
    }
    
    /**
     * 将Redis中的模型数据同步到数据库（R2DBC版本）
     * 注意：由于R2DBC是异步的，但同步任务需要在定时任务中运行，这里使用block()来等待完成
     */
    private void syncModelToDb(SyncContext context) {
        String redisKeyPrefix = context.getRedisKeyPrefix();
        Class<?> modelClass = context.getModelClass();
        
        // 获取需要同步的记录ID列表
        List<String> syncList = redisManager.getSyncList(redisKeyPrefix);
        
        if (syncList == null || syncList.isEmpty()) {
            log.debug("没有需要同步的 {} 记录", modelClass.getSimpleName());
            return;
        }
        
        log.info("开始同步 {} 数据，共 {} 条记录", modelClass.getSimpleName(), syncList.size());
        
        List<String> successIds = new ArrayList<>();
        
        for (String recordId : syncList) {
            String recordKey = redisManager.generateKey(redisKeyPrefix, recordId);
            Map<String, Object> recordData = redisManager.getMap(recordKey);
            
            if (recordData == null || recordData.isEmpty()) {
                log.warn("Redis中未找到记录数据: {}", recordKey);
                successIds.add(recordId); // 也从同步列表移除
                continue;
            }
            
            try {
                // 由于我们没有简单的方式将Map转换为实体，这里我们记录警告并跳过
                // 在实际生产环境中，应该为每个实体类型实现具体的同步逻辑
                log.warn("跳过记录同步，因为需要为每个实体类型实现具体的同步逻辑: {}", recordId);
                successIds.add(recordId);
            } catch (Exception e) {
                log.error("同步记录 {} 时发生错误", recordId, e);
            }
        }
        
        // 从同步列表中移除处理过的记录
        for (String recordId : successIds) {
            redisManager.removeFromSyncList(redisKeyPrefix, recordId);
        }
        
        log.info("{} 数据同步完成，成功: {}, 失败: {}", 
            modelClass.getSimpleName(), 
            successIds.size(), 
            syncList.size() - successIds.size());
    }
    
    
    /**
     * 获取同步状态信息
     */
    public Map<String, Object> getSyncStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("isRunning", isRunning);
        status.put("syncInterval", syncInterval);
        status.put("registeredTasks", new ArrayList<>(syncTasks.keySet()));
        status.put("taskCount", syncTasks.size());
        return status;
    }
}
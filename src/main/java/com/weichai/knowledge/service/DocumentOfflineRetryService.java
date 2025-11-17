package com.weichai.knowledge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weichai.knowledge.entity.DocumentStatus;
import com.weichai.knowledge.repository.ReactiveDocumentStatusRepository;
import com.weichai.knowledge.redis.RedisManager;
import com.weichai.knowledge.utils.ErrorHandler;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 文档下线重试服务
 * 专门负责定期检查下线未成功的文档并重新尝试下线操作
 * 对应Python中的document_offline_retry_service.py
 */
@Slf4j
@Service
public class DocumentOfflineRetryService {
    
    @Autowired
    private ReactiveKnowledgeHandler reactiveKnowledgeHandler;
    
    @Autowired
    private RedisManager redisManager;
    
    @Autowired
    private ErrorHandler errorHandler;
    
    @Autowired
    private ReactiveDocumentStatusRepository documentStatusRepository;
    
    @Value("${knowledge.retry.interval:3600000}")  // 默认1小时，单位毫秒
    private long retryInterval;
    
    @Value("${knowledge.retry.batch-size:10}")
    private int batchSize;
    
    @Value("${knowledge.retry.max-attempts:5}")
    private int maxAttempts;
    
    @Value("${knowledge.retry.cache-expiry:604800}")  // 7天，单位秒
    private long cacheExpiry;
    
    private final ExecutorService threadPool = Executors.newFixedThreadPool(5);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean running = false;
    
    /**
     * 重试记录内部类
     */
    @Data
    private static class RetryRecord {
        private int attempts;
        private String lastRetry;
    }
    
    @PostConstruct
    public void init() {
        log.info("初始化文档下线重试服务");
        running = true;
    }
    
    @PreDestroy
    public void shutdown() {
        log.info("关闭文档下线重试服务");
        running = false;
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 定期执行文档下线重试任务
     * 使用固定延迟，初始延迟1分钟
     */
    @Scheduled(fixedDelayString = "${knowledge.retry.interval:3600000}", 
               initialDelay = 60000)
    public void retryOfflineDocuments() {
        if (!running) {
            log.info("服务已停止，跳过本次执行");
            return;
        }
        
        log.info("开始执行文档下线重试检查...");
        
        try {
            Map<String, Object> stats = processOfflineFailedDocuments();
            log.info("文档下线重试检查完成，统计信息: {}", stats);
        } catch (Exception e) {
            log.error("执行文档下线重试检查时发生错误", e);
        }
    }
    
    /**
     * 处理下线失败的文档
     * 
     * @return 处理统计信息
     */
    @Transactional
    public Map<String, Object> processOfflineFailedDocuments() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("total_to_process", 0);
        stats.put("processed_successfully", 0);
        stats.put("failed", 0);
        stats.put("skipped", 0);
        stats.put("start_time", LocalDateTime.now().toString());
        
        try {
            // 查询所有状态为0（下线未成功）的文档
            List<DocumentStatus> failedDocs = documentStatusRepository.findByStatus(0)
                .collectList()
                .block();
            
            int totalDocs = failedDocs != null ? failedDocs.size() : 0;
            stats.put("total_to_process", totalDocs);
            
            if (totalDocs == 0) {
                log.info("没有找到需要重试下线的文档");
                stats.put("end_time", LocalDateTime.now().toString());
                return stats;
            }
            
            log.info("找到 {} 个需要重试下线的文档", totalDocs);
            
            // 记录已处理和跳过的文档ID
            Set<String> processedFileIds = new HashSet<>();
            Set<String> skippedFileIds = new HashSet<>();
            
            // 批处理文档
            int batchCount = (totalDocs + batchSize - 1) / batchSize;
            
            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
                int startIdx = batchIndex * batchSize;
                int endIdx = Math.min(startIdx + batchSize, totalDocs);
                List<DocumentStatus> batchDocs = failedDocs.subList(startIdx, endIdx);
                
                log.info("正在处理第 {}/{} 批，共 {} 个文档", 
                    batchIndex + 1, batchCount, batchDocs.size());
                
                // 处理每个批次的文档
                int batchProcessed = 0;
                int batchSuccess = 0;
                int batchFailed = 0;
                int batchSkipped = 0;
                
                List<DocumentStatus> docsToProcess = new ArrayList<>();
                
                for (DocumentStatus doc : batchDocs) {
                    String fileId = doc.getFileId();
                    
                    // 检查是否已处理
                    if (processedFileIds.contains(fileId) || skippedFileIds.contains(fileId)) {
                        continue;
                    }
                    
                    // 检查是否应该处理该文件
                    if (!shouldProcessFile(fileId)) {
                        skippedFileIds.add(fileId);
                        batchSkipped++;
                        stats.put("skipped", (int)stats.get("skipped") + 1);
                        log.info("文档 [ID={}] 因多次处理失败被暂时跳过", fileId);
                        continue;
                    }
                    
                    docsToProcess.add(doc);
                    batchProcessed++;
                }
                
                // 如果没有需要处理的文档，跳过这个批次
                if (docsToProcess.isEmpty()) {
                    log.info("本批次所有文档都被跳过，继续下一批次");
                    continue;
                }
                
                // 处理每个文档
                for (DocumentStatus doc : docsToProcess) {
                    if (!running) {
                        log.info("服务停止中，中断单个文档处理");
                        break;
                    }
                    
                    try {
                        boolean success = processDocumentOffline(doc);
                        
                        if (success) {
                            updateFileOfflineStatus(doc.getFileId(), true);
                            batchSuccess++;
                            stats.put("processed_successfully", 
                                (int)stats.get("processed_successfully") + 1);
                        } else {
                            updateFileOfflineStatus(doc.getFileId(), false);
                            batchFailed++;
                            stats.put("failed", (int)stats.get("failed") + 1);
                        }
                        
                        processedFileIds.add(doc.getFileId());
                        
                    } catch (Exception e) {
                        log.error("处理文档时发生未处理的异常", e);
                        updateFileOfflineStatus(doc.getFileId(), false);
                        batchFailed++;
                        stats.put("failed", (int)stats.get("failed") + 1);
                    }
                }
                
                log.info("本批次处理完成: {}个成功, {}个失败, {}个跳过, 处理了{}个文档",
                    batchSuccess, batchFailed, batchSkipped, batchProcessed);
            }
            
            stats.put("end_time", LocalDateTime.now().toString());
            
            int totalProcessed = (int)stats.get("processed_successfully") + (int)stats.get("failed");
            log.info("文档下线重试检查完成: 成功{}个, 失败{}个, 跳过{}个, 共处理{}个文档",
                stats.get("processed_successfully"), stats.get("failed"), 
                stats.get("skipped"), totalProcessed);
            
            return stats;
            
        } catch (Exception e) {
            log.error("处理下线失败文档时发生错误", e);
            stats.put("error", e.getMessage());
            stats.put("end_time", LocalDateTime.now().toString());
            return stats;
        }
    }
    
    /**
     * 处理单个文档的下线操作
     * 
     * @param doc 文档状态对象
     * @return 是否成功
     */
    private boolean processDocumentOffline(DocumentStatus doc) {
        try {
            String systemName = doc.getSystemName();
            String fileId = doc.getFileId();
            String fileNumber = doc.getFileNumber();
            String repoId = doc.getRepoId();
            String zhenzhiFileId = doc.getZhenzhiFileId();
            
            if (zhenzhiFileId == null || zhenzhiFileId.isEmpty()) {
                log.warn("文档 [ID={}] 没有甄知文件ID，无法执行下线操作", fileId);
                return false;
            }
            
            log.info("开始尝试对文档 [ID={}, 甄知ID={}] 执行下线操作", fileId, zhenzhiFileId);
            
            // 1. 查询系统信息获取admin_open_id
            String systemId = "system_" + systemName;
            Map<String, Object> systemInfo = reactiveKnowledgeHandler.querySystemInfo(systemId).block();
            
            String adminOpenId = (String) systemInfo.get("admin_open_id");
            if (adminOpenId == null || adminOpenId.isEmpty()) {
                log.error("系统 {} 的admin_open_id为空", systemName);
                errorHandler.logError(8, fileId, "查询系统信息", 
                    "系统的admin_open_id为空", 
                    Map.of("system_name", systemName, "zhenzhi_id", zhenzhiFileId));
                return false;
            }
            
            // 生成本次任务专用的请求头
            HttpHeaders taskHeaders = reactiveKnowledgeHandler.generateHeadersWithSignature(adminOpenId).block();
            
            // 2. 使用file_number作为文件名
            String fileName = fileNumber;
            
            // 3. 执行文档下线操作
            Map<String, Object> fileLogoutState = reactiveKnowledgeHandler.manageDocumentOnlineStatus(
                repoId,
                zhenzhiFileId,
                0,  // 0表示下线
                fileName,
                adminOpenId,
                "file",
                taskHeaders
            ).block();
            
            // 判断下线是否成功
            Integer errno = (Integer) fileLogoutState.get("errno");
            String msg = (String) fileLogoutState.get("msg");
            
            if (errno != null && errno == 0 && "success".equals(msg)) {
                log.info("文档 [ID={}] 下线成功", fileId);
                
                // 4. 更新文档状态
                doc.setStatus(1);  // 下线成功
                doc.setUpdatedAt(LocalDateTime.now());
                documentStatusRepository.save(doc).block();
                
                return true;
            } else {
                log.error("文档 [ID={}] 下线失败: {}", fileId, 
                    fileLogoutState.getOrDefault("msg", "未知错误"));
                errorHandler.logError(6, fileId, "文档下线操作", 
                    "文档下线失败: " + fileLogoutState.getOrDefault("msg", "未知错误"),
                    Map.of("zhenzhi_id", zhenzhiFileId));
                return false;
            }
            
        } catch (Exception e) {
            log.error("文档 [ID={}] 下线操作出错", doc.getFileId(), e);
            errorHandler.logError(6, doc.getFileId(), "文档下线操作", 
                "文档下线操作出错: " + e.getMessage(),
                Map.of("zhenzhi_id", doc.getZhenzhiFileId()));
            return false;
        }
    }
    
    /**
     * 判断文件是否应该被处理
     * 
     * @param fileId 文件ID
     * @return 是否应该处理
     */
    private boolean shouldProcessFile(String fileId) {
        try {
            String key = redisManager.generateKey("doc:offline:retry", fileId);
            Map<String, Object> record = redisManager.getMap(key);
            
            // 如果记录不存在，应该处理
            if (record == null) {
                return true;
            }
            
            // 获取尝试次数
            Integer attempts = (Integer) record.get("attempts");
            if (attempts == null) {
                attempts = 0;
            }
            
            // 如果尝试次数超过最大次数且最后检查时间在24小时内，不处理
            if (attempts >= maxAttempts) {
                String lastRetryStr = (String) record.get("last_retry");
                if (lastRetryStr != null) {
                    LocalDateTime lastRetry = LocalDateTime.parse(lastRetryStr, 
                        DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    long hoursSinceLastRetry = java.time.Duration.between(
                        lastRetry, LocalDateTime.now()).toHours();
                    
                    if (hoursSinceLastRetry < 24) {
                        log.debug("文档 [ID={}] 尝试下线次数已达{}次，且最后重试时间在24小时内，暂时跳过", 
                            fileId, attempts);
                        return false;
                    }
                }
            }
            
            return true;
            
        } catch (Exception e) {
            log.warn("检查文件处理状态失败: {}, 默认允许处理文档", e.getMessage());
            return true;
        }
    }
    
    /**
     * 更新文件下线重试状态缓存
     * 
     * @param fileId 文件ID
     * @param success 是否成功
     */
    private void updateFileOfflineStatus(String fileId, boolean success) {
        try {
            String key = redisManager.generateKey("doc:offline:retry", fileId);
            String currentTime = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            
            if (success) {
                // 处理成功，删除记录
                boolean deleted = redisManager.delete(key);
                if (deleted) {
                    log.debug("文档 [ID={}] 下线处理成功，已从Redis缓存中移除", fileId);
                }
            } else {
                // 获取当前记录
                Map<String, Object> record = redisManager.getMap(key);
                if (record == null) {
                    record = new HashMap<>();
                    record.put("attempts", 1);
                } else {
                    Integer attempts = (Integer) record.get("attempts");
                    record.put("attempts", attempts != null ? attempts + 1 : 1);
                }
                record.put("last_retry", currentTime);
                
                // 存储到Redis，7天过期
                boolean saved = redisManager.set(key, record, cacheExpiry);
                if (saved) {
                    log.debug("文档 [ID={}] 下线处理失败，更新Redis缓存: 尝试次数={}", 
                        fileId, record.get("attempts"));
                }
            }
        } catch (Exception e) {
            log.warn("更新Redis缓存状态失败: {}", e.getMessage());
        }
    }
}
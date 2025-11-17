package com.weichai.knowledge.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.weichai.knowledge.entity.UnstructuredDocument;
import com.weichai.knowledge.repository.ReactiveUnstructuredDocumentRepository;
import com.weichai.knowledge.redis.RedisManager;
import com.weichai.knowledge.utils.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 知识库状态检查服务
 * 定期检查未上线文档并设置权限
 * 对应Python中的KnowledgeStatusCheckService
 */
@Slf4j
@Service
public class KnowledgeStatusCheckService {
    
    // 错误类型常量
    private static final int ERROR_TYPE_QUERY_STATUS = 5;  // 查询文档状态错误
    private static final int ERROR_TYPE_DATABASE_OPERATION = 6;  // 数据库操作错误
    private static final int ERROR_TYPE_SET_PERMISSION = 9;  // 设置文档权限错误
    
    // 批处理配置
    private static final int BATCH_SIZE = 20;
    private static final int MAX_DOCS_TO_PROCESS = 100;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    
    // 进度管理配置
    private static final String PROCESSING_OFFSET_KEY = "knowledge:check:processing_offset";
    private static final String PROCESSING_LOCK_KEY = "knowledge:check:processing_lock";
    private static final int LOCK_TIMEOUT_MINUTES = 30;
    
    @Autowired
    private ReactiveUnstructuredDocumentRepository unstructuredDocumentRepository;
    
    @Autowired
    private ReactiveKnowledgeHandler reactiveKnowledgeHandler;
    
    @Autowired
    private RedisManager redisManager;
    
    @Autowired
    private ErrorHandler errorHandler;
    
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    
    // 服务运行状态
    private volatile boolean isRunning = false;
    
    @PostConstruct
    public void init() {
        this.isRunning = true;
        log.info("知识库状态检查服务初始化完成（串行处理模式）");
    }
    
    @PreDestroy
    public void destroy() {
        this.isRunning = false;
        log.info("知识库状态检查服务已关闭");
    }
    
    /**
     * 定时检查未上线文档状态并设置权限
     * 可通过配置文件调整检查间隔，默认30分钟
     */
    @Scheduled(fixedDelayString = "${app.knowledge.check-interval:1800000}") // 默认30分钟
    @Async
    public void checkDocumentsStatus() {
        if (!isRunning) {
            log.debug("服务未运行，跳过状态检查");
            return;
        }
        
        log.info("开始定时检查未上线文档状态（串行处理模式）");
        
        try {
            // 获取待处理文档信息
            getPendingDocuments()
                .flatMap(pendingDocsInfo -> {
                    if (pendingDocsInfo.getDocuments().isEmpty()) {
                        log.info("没有找到需要检查状态的未上线文档");
                        return Mono.empty();
                    }
                    
                    // 串行处理文档
                    return processDocumentsInBatches(pendingDocsInfo)
                        .doOnNext(results -> {
                            // 记录处理结果
                            logProcessingResults(results, pendingDocsInfo.getTotal());
                        });
                })
                .block(); // 在定时任务中使用block()等待完成
            
        } catch (Exception e) {
            log.error("检查未上线文档过程中发生错误", e);
            errorHandler.logError(ERROR_TYPE_QUERY_STATUS, "unknown", "check_documents_status", 
                "检查文档状态任务失败: " + e.getMessage(), Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * 获取待处理的文档信息 - 使用动态偏移量避免重复处理
     */
    private Mono<DocumentBatchInfo> getPendingDocuments() {
        return acquireProcessingLock()
            .flatMap(lockAcquired -> {
                if (!lockAcquired) {
                    log.info("无法获取处理锁，可能有其他实例正在处理，跳过本次检查");
                    return Mono.just(new DocumentBatchInfo(0, Collections.emptyList()));
                }
                
                return getCurrentProcessingOffset()
                    .flatMap(currentOffset -> {
                        return unstructuredDocumentRepository.countByStatusAndZhenzhiFileIdNotNull(0)
                            .flatMap(totalPendingDocs -> {
                                if (totalPendingDocs == 0) {
                                    // 没有待处理文档，重置偏移量
                                    resetProcessingOffset();
                                    return Mono.just(new DocumentBatchInfo(0, Collections.emptyList()));
                                }
                                
                                log.info("发现总共有 {} 个未上线文档，当前处理偏移量: {}", totalPendingDocs, currentOffset);
                                
                                // 检查是否需要重置偏移量
                                final long finalOffset;
                                if (currentOffset >= totalPendingDocs) {
                                    log.info("当前偏移量 {} 已超过总文档数 {}，重置偏移量到0", currentOffset, totalPendingDocs);
                                    finalOffset = 0L;
                                    resetProcessingOffset();
                                } else {
                                    finalOffset = currentOffset;
                                }
                                
                                // 查询待处理文档（使用动态偏移量）
                                return unstructuredDocumentRepository
                                    .findByStatusAndZhenzhiFileIdNotNullOrderByCreatedAt(0, (long) MAX_DOCS_TO_PROCESS, finalOffset)
                                    .collectList()
                                    .map(documents -> {
                                        log.info("从偏移量 {} 开始获取到 {} 个文档", finalOffset, documents.size());
                                        return new DocumentBatchInfo(totalPendingDocs.intValue(), documents, finalOffset);
                                    });
                            });
                    });
            })
            .doFinally(signalType -> releaseProcessingLock());
    }
    
    /**
     * 文档批次信息
     */
    private static class DocumentBatchInfo {
        private final int total;
        private final List<UnstructuredDocument> documents;
        private final long currentOffset;
        
        public DocumentBatchInfo(int total, List<UnstructuredDocument> documents) {
            this.total = total;
            this.documents = documents;
            this.currentOffset = 0L;
        }
        
        public DocumentBatchInfo(int total, List<UnstructuredDocument> documents, long currentOffset) {
            this.total = total;
            this.documents = documents;
            this.currentOffset = currentOffset;
        }
        
        public int getTotal() { return total; }
        public List<UnstructuredDocument> getDocuments() { return documents; }
        public long getCurrentOffset() { return currentOffset; }
    }
    
    /**
     * 处理结果统计
     */
    private static class ProcessingResults {
        private int totalSuccess = 0;
        private int totalFailed = 0;
        private int totalSkipped = 0;
        private int totalProcessed = 0;
        
        public void addResults(BatchProcessResults batchResults) {
            this.totalSuccess += batchResults.getSuccess();
            this.totalFailed += batchResults.getFailed();
            this.totalSkipped += batchResults.getSkipped();
            this.totalProcessed += batchResults.getProcessed();
        }
        
        public void addFailed(int count) { this.totalFailed += count; }
        public void addSkipped(int count) { this.totalSkipped += count; }
        
        // Getters
        public int getTotalSuccess() { return totalSuccess; }
        public int getTotalFailed() { return totalFailed; }
        public int getTotalSkipped() { return totalSkipped; }
        public int getTotalProcessed() { return totalProcessed; }
    }
    
    /**
     * 批次处理结果
     */
    private static class BatchProcessResults {
        private int success = 0;
        private int failed = 0;
        private int skipped = 0;
        private int processed = 0;
        
        public void addSuccess(int count) { this.success += count; }
        public void addFailed(int count) { this.failed += count; }
        public void addSkipped(int count) { this.skipped += count; }
        public void addProcessed(int count) { this.processed += count; }
        
        // Getters
        public int getSuccess() { return success; }
        public int getFailed() { return failed; }
        public int getSkipped() { return skipped; }
        public int getProcessed() { return processed; }
    }
    
    /**
     * 单个文档处理结果
     */
    private static class DocumentProcessResult {
        private final boolean success;
        
        public DocumentProcessResult(boolean success) {
            this.success = success;
        }
        
        public boolean isSuccess() { return success; }
    }
    
    /**
     * 记录处理结果统计 - 增强版本，包含进度监控
     */
    private void logProcessingResults(ProcessingResults results, int totalPendingDocs) {
        log.info("文档状态检查完成: 成功{}个, 失败{}个, 跳过{}个, 共处理{}个文档", 
            results.getTotalSuccess(), results.getTotalFailed(), 
            results.getTotalSkipped(), results.getTotalProcessed());
        log.info("数据库中共有{}个待处理文档", totalPendingDocs);
        
        // 记录处理进度统计到Redis
        recordProcessingProgress(results, totalPendingDocs);
    }
    
    /**
     * 批量处理文档 - 按系统分组优化，处理完成后更新偏移量
     */
    private Mono<ProcessingResults> processDocumentsInBatches(DocumentBatchInfo pendingDocsInfo) {
        List<UnstructuredDocument> documents = pendingDocsInfo.getDocuments();
        
        // 处理结果统计
        ProcessingResults totalResults = new ProcessingResults();
        
        // 记录已处理和跳过的文档ID
        Set<String> processedIds = new HashSet<>();
        Set<String> skippedIds = new HashSet<>();
        
        return Mono.fromCallable(() -> {
            try {
                // 1. 按 system_name 分组文档
                Map<String, List<UnstructuredDocument>> documentsBySystem = groupDocumentsBySystem(documents, processedIds, skippedIds, totalResults);
                
                if (documentsBySystem.isEmpty()) {
                    log.info("没有需要处理的文档");
                    return totalResults;
                }
                
                return totalResults;
            } catch (Exception e) {
                log.error("分组文档时发生错误", e);
                throw new RuntimeException(e);
            }
        })
        .flatMap(results -> {
            // 2. 遍历每个系统，进行批量处理
            Map<String, List<UnstructuredDocument>> documentsBySystem = groupDocumentsBySystem(documents, processedIds, skippedIds, results);
            
            return Flux.fromIterable(documentsBySystem.entrySet())
                .flatMap(entry -> {
                    String systemName = entry.getKey();
                    List<UnstructuredDocument> systemDocs = entry.getValue();
                    
                    log.info("开始处理系统 '{}' 的 {} 个文档", systemName, systemDocs.size());
                    
                    return processSystemDocuments(systemName, systemDocs, processedIds)
                        .doOnNext(systemResults -> {
                            results.addResults(systemResults);
                            log.info("系统 '{}' 处理完成: {}个成功, {}个失败", 
                                systemName, systemResults.getSuccess(), systemResults.getFailed());
                        })
                        .onErrorResume(e -> {
                            log.error("处理系统 '{}' 的文档时发生错误", systemName, e);
                            // 将该系统所有文档标记为失败
                            for (UnstructuredDocument doc : systemDocs) {
                                updateFileCheckStatus(doc.getZhenzhiFileId(), false);
                                processedIds.add(doc.getZhenzhiFileId());
                                results.addFailed(1);
                            }
                            return Mono.empty();
                        });
                })
                .then(Mono.just(results));
        })
        .doOnNext(results -> {
            // 处理完成后更新偏移量
            if (!documents.isEmpty()) {
                long newOffset = pendingDocsInfo.getCurrentOffset() + documents.size();
                updateProcessingOffset(newOffset);
                log.info("处理完成，更新偏移量为: {}", newOffset);
            }
        })
        .onErrorReturn(totalResults);
    }
    
    /**
     * 按系统分组文档
     */
    private Map<String, List<UnstructuredDocument>> groupDocumentsBySystem(List<UnstructuredDocument> documents,
                                                                          Set<String> processedIds, Set<String> skippedIds,
                                                                          ProcessingResults totalResults) {
        Map<String, List<UnstructuredDocument>> documentsBySystem = new HashMap<>();
        
        for (UnstructuredDocument doc : documents) {
            String zhenzhiFileId = doc.getZhenzhiFileId();
            
            // 检查是否已处理或跳过
            if (processedIds.contains(zhenzhiFileId) || skippedIds.contains(zhenzhiFileId)) {
                continue;
            }
            
            // 检查是否应该处理该文件
            if (!shouldProcessFile(zhenzhiFileId)) {
                skippedIds.add(zhenzhiFileId);
                totalResults.addSkipped(1);
                log.info("文档 [GUID={}] 因多次处理失败被暂时跳过", zhenzhiFileId);
                continue;
            }
            
            String systemName = doc.getSystemName();
            documentsBySystem.computeIfAbsent(systemName, k -> new ArrayList<>()).add(doc);
        }
        
        return documentsBySystem;
    }
    
    /**
     * 处理一个系统的所有文档
     */
    private Mono<BatchProcessResults> processSystemDocuments(String systemName, List<UnstructuredDocument> systemDocs,
                                                      Set<String> processedIds) {
        BatchProcessResults results = new BatchProcessResults();
        
        // 1. 获取系统信息并生成专用 headers
        String systemId = systemName;
        return reactiveKnowledgeHandler.querySystemInfo(systemId)
            .flatMap(systemInfo -> {
                String adminOpenId = (String) systemInfo.get("admin_open_id");
                String departmentId = (String) systemInfo.get("department_guid");
                
                if (adminOpenId == null || adminOpenId.isEmpty() || departmentId == null || departmentId.isEmpty()) {
                    log.error("无法获取系统 '{}' 的完整信息，跳过该系统所有文档", systemName);
                    // 将该系统所有文档标记为处理失败
                    for (UnstructuredDocument doc : systemDocs) {
                        updateFileCheckStatus(doc.getZhenzhiFileId(), false);
                        processedIds.add(doc.getZhenzhiFileId());
                        results.addFailed(1);
                    }
                    return Mono.just(results);
                }
        
                // 2. 生成当前系统批次专用的 headers
                HttpHeaders systemHeaders = reactiveKnowledgeHandler.generateHeadersWithSignature(adminOpenId).block();
                log.info("为系统 '{}' 生成专用请求头完成", systemName);
                
                // 3. 批量查询当前系统所有文档的状态
                List<String> docGuids = systemDocs.stream()
                    .map(UnstructuredDocument::getZhenzhiFileId)
                    .collect(Collectors.toList());
                
                return reactiveKnowledgeHandler.queryFileStatus(docGuids, 1)
                    .flatMap(statusResponse -> {
                        if (!Boolean.TRUE.equals(statusResponse.get("success"))) {
                            log.error("系统 '{}' 批量查询文档状态失败: {}", systemName, statusResponse.get("message"));
                            // 将该系统所有文档标记为处理失败
                            for (UnstructuredDocument doc : systemDocs) {
                                updateFileCheckStatus(doc.getZhenzhiFileId(), false);
                                processedIds.add(doc.getZhenzhiFileId());
                                results.addFailed(1);
                            }
                            return Mono.just(results);
                        }
        
                        // 4. 创建状态映射表
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> docStatusList = (List<Map<String, Object>>) statusResponse.get("data");
                        Map<String, Map<String, Object>> statusMap = docStatusList.stream()
                            .filter(status -> status.get("guid") != null)
                            .collect(Collectors.toMap(
                                status -> (String) status.get("guid"),
                                status -> status,
                                (existing, replacement) -> existing
                            ));
                        
                        log.info("系统 '{}' 共获取到 {} 个文档状态信息", systemName, statusMap.size());
                        
                        // 5. 逐个处理已上线的文档（使用专用的 systemHeaders）
                        return Flux.fromIterable(systemDocs)
                            .delayElements(Duration.ofMillis(100)) // 添加延迟，避免对外部API造成过大压力
                            .flatMap(doc -> 
                                processSingleDocumentWithHeaders(doc, statusMap, docStatusList, systemInfo, systemHeaders)
                                    .doOnNext(result -> {
                                        processedIds.add(doc.getZhenzhiFileId());
                                        if (result.isSuccess()) {
                                            results.addSuccess(1);
                                        } else {
                                            results.addFailed(1);
                                        }
                                    })
                                    .onErrorResume(e -> {
                                        log.error("处理文档 {} 时发生异常", doc.getZhenzhiFileId(), e);
                                        processedIds.add(doc.getZhenzhiFileId());
                                        updateFileCheckStatus(doc.getZhenzhiFileId(), false);
                                        results.addFailed(1);
                                        return Mono.empty();
                                    })
                            )
                            .then(Mono.just(results));
                    });
            });
    }
    
    /**
     * 处理单个文档 - 带系统信息和 headers
     */
    private Mono<DocumentProcessResult> processSingleDocumentWithHeaders(UnstructuredDocument doc,
                                                                   Map<String, Map<String, Object>> statusMap,
                                                                   List<Map<String, Object>> docStatusList,
                                                                   Map<String, Object> systemInfo,
                                                                   HttpHeaders systemHeaders) {
        
        final String zhenzhiFileId = doc.getZhenzhiFileId();
        final String fileId = doc.getFileId();
        
        // 先检查是否已经在Redis中标记为处理成功，避免重复处理
        String successKey = "doc:success:" + zhenzhiFileId;
        if (redisManager.exists(successKey)) {
            log.info("文档 [ID={}, GUID={}] 已在Redis中标记为处理成功，跳过处理", fileId, zhenzhiFileId);
            return Mono.just(new DocumentProcessResult(true));
        }
        
        return unstructuredDocumentRepository.findByZhenzhiFileId(zhenzhiFileId)
            .flatMap(currentDoc -> {
                if (currentDoc.getStatus() == 1) {
                    log.info("文档 [ID={}, GUID={}] 已经是成功状态，跳过处理", fileId, zhenzhiFileId);
                    updateFileCheckStatus(zhenzhiFileId, true);
                    return Mono.just(new DocumentProcessResult(true));
                }
                
                // 查找文档状态信息
                Map<String, Object> docInfo = findDocumentStatus(zhenzhiFileId, statusMap, docStatusList, doc);
                
                if (docInfo == null) {
                    log.warn("文档 [ID={}, GUID={}] 在返回结果中未找到状态信息，跳过处理", fileId, zhenzhiFileId);
                    updateFileCheckStatus(zhenzhiFileId, false);
                    return Mono.just(new DocumentProcessResult(false));
                }
                
                // 检查文档是否已上线
                boolean isOnline = checkDocumentOnline(docInfo, fileId, zhenzhiFileId);
                
                if (isOnline) {
                    // 设置文档权限并更新状态（传入系统信息和 headers）
                    return setDocumentPermissions(doc, currentDoc, systemInfo, systemHeaders);
                } else {
                    log.debug("文档 [ID={}, GUID={}] 仍未上线，状态: {}, 原因: {}", 
                        fileId, zhenzhiFileId, docInfo.get("status"), docInfo.get("message"));
                    updateFileCheckStatus(zhenzhiFileId, false);
                    return Mono.just(new DocumentProcessResult(false));
                }
            })
            .switchIfEmpty(Mono.<DocumentProcessResult>defer(() -> {
                // 查询不到文档时，先检查Redis中是否已标记为处理成功
                String successKeyForEmpty = "doc:success:" + zhenzhiFileId;
                if (redisManager.exists(successKeyForEmpty)) {
                    log.info("文档 [ID={}, GUID={}] 在Redis中已标记为处理成功，虽然数据库查询为空", fileId, zhenzhiFileId);
                    return Mono.just(new DocumentProcessResult(true));
                }
                
                log.warn("文档 [ID={}, GUID={}] 在数据库中不存在", fileId, zhenzhiFileId);
                updateFileCheckStatus(zhenzhiFileId, false);
                return Mono.just(new DocumentProcessResult(false));
            }))
            .onErrorResume(e -> {
                log.error("处理文档 [ID={}, GUID={}] 时发生错误", fileId, zhenzhiFileId, e);
                errorHandler.logError(ERROR_TYPE_QUERY_STATUS, fileId, "process_single_document_with_headers",
                    "处理单个文档失败: " + e.getMessage(), 
                    Map.of("zhenzhi_file_id", zhenzhiFileId, "system_name", doc.getSystemName()));
                updateFileCheckStatus(zhenzhiFileId, false);
                return Mono.just(new DocumentProcessResult(false));
            });
    }
    

    
    /**
     * 查找文档状态信息
     */
    private Map<String, Object> findDocumentStatus(String zhenzhiFileId,
                                                  Map<String, Map<String, Object>> statusMap,
                                                  List<Map<String, Object>> docStatusList,
                                                  UnstructuredDocument doc) {
        
        // 首先尝试直接用GUID匹配
        Map<String, Object> docInfo = statusMap.get(zhenzhiFileId);
        
        // 如果找不到，尝试遍历查找
        if (docInfo == null) {
            for (Map<String, Object> item : docStatusList) {
                String itemGuid = (String) item.get("guid");
                if (itemGuid != null && itemGuid.equals(zhenzhiFileId)) {
                    docInfo = item;
                    log.info("通过遍历找到文档状态: {}", zhenzhiFileId);
                    break;
                }
            }
        }
        
        // 如果仍找不到，尝试基于文件名/标题匹配
        if (docInfo == null && doc.getFileName() != null) {
            String fileName = doc.getFileName();
            for (Map<String, Object> item : docStatusList) {
                String itemTitle = (String) item.get("title");
                if (itemTitle != null && 
                    (fileName.toLowerCase().contains(itemTitle.toLowerCase()) ||
                     itemTitle.toLowerCase().contains(fileName.toLowerCase()))) {
                    docInfo = item;
                    log.info("通过标题匹配找到文档状态: [GUID={}], 标题: {}", zhenzhiFileId, itemTitle);
                    break;
                }
            }
        }
        
        return docInfo;
    }
    
    /**
     * 检查文档是否已上线
     */
    private boolean checkDocumentOnline(Map<String, Object> docInfo, String fileId, String zhenzhiFileId) {
        Boolean docOnline = (Boolean) docInfo.get("online");
        Object docStatus = docInfo.get("status");
        String docMessage = (String) docInfo.getOrDefault("message", "");
        
        log.info("文档 [ID={}, GUID={}] 状态: online={}, status={}, message={}", 
            fileId, zhenzhiFileId, docOnline, docStatus, docMessage);
        
        // 如果online不是布尔值，基于status判断
        if (docOnline == null && docStatus instanceof Number) {
            docOnline = ((Number) docStatus).intValue() >= 20;
            log.info("文档 [ID={}, GUID={}] online字段为空，使用status判断: {}", fileId, zhenzhiFileId, docOnline);
        }
        
        return Boolean.TRUE.equals(docOnline);
    }
    
    /**
     * 设置文档权限并更新状态
     */
    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    private Mono<DocumentProcessResult> setDocumentPermissions(UnstructuredDocument doc, UnstructuredDocument currentDoc,
                                                       Map<String, Object> systemInfo, HttpHeaders systemHeaders) {
        final String zhenzhiFileId = doc.getZhenzhiFileId();
        final String fileId = doc.getFileId();
        final String systemName = doc.getSystemName();
        
        log.info("文档 [ID={}, GUID={}] 已上线成功，准备设置权限", fileId, zhenzhiFileId);
        
        String departmentId = (String) systemInfo.get("department_guid");
        String adminOpenId = (String) systemInfo.get("admin_open_id");
        
        if (departmentId == null || departmentId.isEmpty()) {
            log.warn("无法获取系统 [{}] 的部门ID，跳过设置文档权限", systemName);
            updateFileCheckStatus(zhenzhiFileId, false);
            return Mono.just(new DocumentProcessResult(false));
        }
        
        // 转换角色和用户列表类型
        List<String> roleList = convertToStringList(doc.getRoleList());
        List<String> userList = convertToStringList(doc.getUserList());
        
        // 设置文档权限（无论是否有role_list都要调用）
        return reactiveKnowledgeHandler.setDocAdmin(
                systemName, zhenzhiFileId, doc.getRepoId(), roleList,
                userList, "add", departmentId, adminOpenId, 1, systemHeaders)
            .flatMap(permissionResult -> {
                log.info("文档权限设置结果: {}", permissionResult);
                
                // 检查权限设置是否成功
                String status = (String) permissionResult.get("status");
                if (!"success".equals(status)) {
                    String message = (String) permissionResult.getOrDefault("message", "未知错误");
                    log.error("文档 [ID={}, GUID={}] 权限设置失败: {}", fileId, zhenzhiFileId, message);
                    updateFileCheckStatus(zhenzhiFileId, false);
                    return Mono.just(new DocumentProcessResult(false));
                }
                
                log.info("文档 [ID={}, GUID={}] 权限设置成功", fileId, zhenzhiFileId);
                
                // 立即在Redis中标记处理成功，防止并发重复处理
                setDocumentProcessedSuccessfully(zhenzhiFileId);
                
                // 更新数据库状态（只有在status为0时才更新）
                if (currentDoc.getStatus() == 0) {
                    return unstructuredDocumentRepository.updateStatusByZhenzhiFileId(
                            1, LocalDateTime.now(), zhenzhiFileId)
                        .map(updatedRows -> {
                            if (updatedRows > 0) {
                                log.info("文档 [ID={}, GUID={}] 状态已更新为成功", fileId, zhenzhiFileId);
                            } else {
                                log.info("文档 [ID={}, GUID={}] 状态更新失败，可能已被其他进程更新", fileId, zhenzhiFileId);
                            }
                            updateFileCheckStatus(zhenzhiFileId, true);
                            return new DocumentProcessResult(true);
                        });
                } else {
                    log.info("文档 [ID={}, GUID={}] 状态已经被其他进程更新，跳过更新", fileId, zhenzhiFileId);
                    updateFileCheckStatus(zhenzhiFileId, true);
                    return Mono.just(new DocumentProcessResult(true));
                }
            })
            .onErrorResume(e -> {
                log.error("设置文档 [ID={}, GUID={}] 权限时发生错误", fileId, zhenzhiFileId, e);
                errorHandler.logError(ERROR_TYPE_SET_PERMISSION, fileId, "set_document_permissions",
                    "设置文档权限失败: " + e.getMessage(),
                    Map.of("zhenzhi_file_id", zhenzhiFileId, "system_name", systemName));
                updateFileCheckStatus(zhenzhiFileId, false);
                return Mono.just(new DocumentProcessResult(false));
            });
    }
    
    /**
     * 判断文件是否应该被处理（优化版Redis缓存策略）
     */
    private boolean shouldProcessFile(String zhenzhiFileId) {
        try {
            String key = "doc:check:" + zhenzhiFileId;
            Map<String, Object> record = redisManager.getMap(key);
            
            if (record == null) {
                return true;
            }
            
            // 获取尝试次数和最后检查时间
            Object attemptsObj = record.get("attempts");
            int attempts = (attemptsObj instanceof Number) ? ((Number) attemptsObj).intValue() : 0;
            String lastCheckStr = (String) record.get("last_check");
            
            if (attempts >= MAX_RETRY_ATTEMPTS && lastCheckStr != null) {
                try {
                    LocalDateTime lastCheck = LocalDateTime.parse(lastCheckStr);
                    LocalDateTime now = LocalDateTime.now();
                    
                    // 根据失败次数动态调整重试间隔
                    long hoursToWait = calculateRetryInterval(attempts);
                    
                    if (now.isBefore(lastCheck.plusHours(hoursToWait))) {
                        log.debug("文档 [{}] 尝试次数已达{}次，需等待{}小时后重试，暂时跳过", 
                            zhenzhiFileId, attempts, hoursToWait);
                        return false;
                    } else {
                        log.info("文档 [{}] 等待时间已到，允许重新尝试处理", zhenzhiFileId);
                        return true;
                    }
                } catch (Exception e) {
                    log.warn("解析时间戳失败: {}, 默认允许处理文档", e.getMessage());
                    return true;
                }
            }
            
            return true;
        } catch (Exception e) {
            log.warn("检查文件处理状态失败: {}, 默认允许处理文档", e.getMessage());
            return true;
        }
    }
    
    /**
     * 根据失败次数计算重试间隔（小时）
     */
    private long calculateRetryInterval(int attempts) {
        if (attempts <= 3) {
            return 1; // 前3次失败：等待1小时
        } else if (attempts <= 5) {
            return 6; // 4-5次失败：等待6小时
        } else if (attempts <= 10) {
            return 24; // 6-10次失败：等待24小时
        } else {
            return 72; // 超过10次失败：等待72小时
        }
    }
    
    /**
     * 更新文件检查状态缓存
     */
    private void updateFileCheckStatus(String zhenzhiFileId, boolean success) {
        try {
            String key = "doc:check:" + zhenzhiFileId;
            String currentTime = LocalDateTime.now().toString();
            
            if (success) {
                // 处理成功，删除记录
                boolean deleted = redisManager.delete(key);
                if (deleted) {
                    log.debug("文档 [{}] 处理成功，已从Redis缓存中移除", zhenzhiFileId);
                }
            } else {
                // 获取当前记录
                Map<String, Object> record = redisManager.getMap(key);
                if (record == null) {
                    record = new HashMap<>();
                    record.put("attempts", 1);
                } else {
                    Object attemptsObj = record.get("attempts");
                    int attempts = (attemptsObj instanceof Number) ? ((Number) attemptsObj).intValue() : 0;
                    record.put("attempts", attempts + 1);
                }
                record.put("last_check", currentTime);
                
                // 存储到Redis，7天过期
                boolean saved = redisManager.set(key, record, 604800L);
                if (saved) {
                    log.debug("文档 [{}] 处理失败，更新Redis缓存: 尝试次数={}", 
                        zhenzhiFileId, record.get("attempts"));
                }
            }
                 } catch (Exception e) {
             log.warn("更新文件检查状态缓存失败: {}", e.getMessage());
         }
     }
     
     /**
      * 转换JsonNode为String列表
      */
     private List<String> convertToStringList(JsonNode jsonNode) {
         if (jsonNode == null || jsonNode.isNull() || jsonNode.isEmpty()) {
             return null;
         }

         List<String> result = new ArrayList<>();

         if (jsonNode.isArray()) {
             // 如果是数组类型，遍历每个元素
             for (JsonNode element : jsonNode) {
                 if (element != null && !element.isNull()) {
                     result.add(element.asText());
                 }
             }
         } else if (jsonNode.isTextual()) {
             // 如果是字符串类型，按逗号分割
             String text = jsonNode.asText();
             if (text != null && !text.trim().isEmpty()) {
                 String[] parts = text.split(",");
                 for (String part : parts) {
                     String trimmed = part.trim();
                     if (!trimmed.isEmpty()) {
                         result.add(trimmed);
                     }
                 }
             }
         }

         return result.isEmpty() ? null : result;
     }
     
     /**
      * 清理指定文档的检查缓存
      * @param zhenzhiFileId 文档ID
      * @return 是否清理成功
      */
     public boolean clearDocumentCheckCache(String zhenzhiFileId) {
         try {
             if (zhenzhiFileId == null || zhenzhiFileId.trim().isEmpty()) {
                 log.warn("文档ID不能为空");
                 return false;
             }
             
             String key = "doc:check:" + zhenzhiFileId;
             boolean deleted = redisManager.delete(key);
             log.info("清理文档 [{}] 的检查缓存: {}", zhenzhiFileId, deleted ? "成功" : "失败");
             return deleted;
             
         } catch (Exception e) {
             log.error("清理文档检查缓存失败，文档ID: {}", zhenzhiFileId, e);
             return false;
         }
     }
     
           /**
       * 清理所有文档检查缓存
       * @return 清理的缓存数量
       */
      public int clearAllDocumentCheckCache() {
          try {
              String pattern = "doc:check:*";
              log.info("开始清理所有文档检查缓存，pattern: {}", pattern);
              
              return (int) clearRedisByPattern(pattern);
              
          } catch (Exception e) {
              log.error("清理所有文档检查缓存失败", e);
              return 0;
          }
      }
     
     /**
      * 获取文档检查缓存信息
      * @param zhenzhiFileId 文档ID
      * @return 缓存信息
      */
     public Map<String, Object> getDocumentCheckCacheInfo(String zhenzhiFileId) {
         try {
             if (zhenzhiFileId == null || zhenzhiFileId.trim().isEmpty()) {
                 return Map.of("exists", false, "message", "文档ID不能为空");
             }
             
             String key = "doc:check:" + zhenzhiFileId;
             Map<String, Object> record = redisManager.getMap(key);
             
             if (record == null) {
                 return Map.of("exists", false, "message", "缓存不存在");
             }
             
             Map<String, Object> result = new HashMap<>();
             result.put("exists", true);
             result.put("key", key);
             result.put("attempts", record.get("attempts"));
             result.put("last_check", record.get("last_check"));
             
             // 判断是否会被跳过处理
             Object attemptsObj = record.get("attempts");
             boolean wouldSkip = false;
             if (attemptsObj instanceof Number && ((Number) attemptsObj).intValue() >= 5) {
                 String lastCheckStr = (String) record.get("last_check");
                 if (lastCheckStr != null) {
                     try {
                         LocalDateTime lastCheck = LocalDateTime.parse(lastCheckStr);
                         wouldSkip = LocalDateTime.now().minusHours(24).isBefore(lastCheck);
                     } catch (Exception e) {
                         log.warn("解析时间戳失败: {}", e.getMessage());
                     }
                 }
             }
             result.put("wouldSkip", wouldSkip);
             result.put("message", wouldSkip ? "会被跳过处理" : "会被正常处理");
             
             return result;
             
         } catch (Exception e) {
             log.error("获取文档检查缓存信息失败，文档ID: {}", zhenzhiFileId, e);
                           return Map.of("exists", false, "message", "查询失败: " + e.getMessage());
          }
      }
      
      /**
       * 清理Redis所有键 - 危险操作
       * @return 删除的键数量
       */
      public long clearAllRedisKeys() {
          try {
              log.warn("执行危险操作：清理Redis所有键");
              
              // 使用SCAN命令获取所有键，然后批量删除
              return clearRedisByPattern("*");
              
          } catch (Exception e) {
              log.error("清理Redis所有键失败", e);
              throw new RuntimeException("清理Redis所有键失败: " + e.getMessage(), e);
          }
      }
      
      /**
       * 按模式清理Redis键
       * @param pattern 键模式，如 "doc:check:*"
       * @return 删除的键数量
       */
      public long clearRedisByPattern(String pattern) {
          try {
              if (pattern == null || pattern.trim().isEmpty()) {
                  throw new IllegalArgumentException("模式不能为空");
              }
              
              log.info("按模式清理Redis键，模式: {}", pattern);
              
              // 使用SCAN命令获取匹配的键
              Set<String> keysToDelete = new HashSet<>();
              ScanOptions options = ScanOptions.scanOptions().match(pattern).count(1000).build();
              
              try (Cursor<String> cursor = stringRedisTemplate.scan(options)) {
                  while (cursor.hasNext()) {
                      keysToDelete.add(cursor.next());
                  }
              }
              
              if (keysToDelete.isEmpty()) {
                  log.info("没有找到匹配模式 [{}] 的键", pattern);
                  return 0;
              }
              
              log.info("找到 {} 个匹配模式 [{}] 的键，开始删除", keysToDelete.size(), pattern);
              
              // 批量删除键
              long deletedCount = redisManager.delete(keysToDelete);
              
              log.info("成功删除 {} 个匹配模式 [{}] 的键", deletedCount, pattern);
              return deletedCount;
              
          } catch (Exception e) {
              log.error("按模式清理Redis键失败，模式: {}", pattern, e);
              throw new RuntimeException("按模式清理Redis键失败: " + e.getMessage(), e);
          }
      }
      
      /**
       * 获取Redis键统计信息
       * @return 统计信息
       */
      public Map<String, Object> getRedisStats() {
          try {
              log.info("获取Redis键统计信息");
              
              Map<String, Object> stats = new HashMap<>();
              
              // 统计不同模式的键数量
              long totalKeys = countKeysByPattern("*");
              long docCheckKeys = countKeysByPattern("doc:check:*");
              long kafkaMessageKeys = countKeysByPattern("kafka:message:*");
              long kafkaStatusKeys = countKeysByPattern("kafka:status:*");
              
              stats.put("totalKeys", totalKeys);
              stats.put("docCheckKeys", docCheckKeys);
              stats.put("kafkaMessageKeys", kafkaMessageKeys);
              stats.put("kafkaStatusKeys", kafkaStatusKeys);
              stats.put("timestamp", LocalDateTime.now().toString());
              
              // 添加详细的统计信息
              Map<String, Long> patternStats = new HashMap<>();
              patternStats.put("doc:check:*", docCheckKeys);
              patternStats.put("kafka:message:*", kafkaMessageKeys);
              patternStats.put("kafka:status:*", kafkaStatusKeys);
              patternStats.put("kafka:processing_queue", stringRedisTemplate.hasKey("kafka:processing_queue") ? 1L : 0L);
              
              stats.put("patternStats", patternStats);
              
              log.info("Redis统计信息: 总键数={}, 文档检查键数={}", totalKeys, docCheckKeys);
              
              return stats;
              
          } catch (Exception e) {
              log.error("获取Redis键统计信息失败", e);
              Map<String, Object> errorStats = new HashMap<>();
              errorStats.put("error", true);
              errorStats.put("message", "获取统计信息失败: " + e.getMessage());
              errorStats.put("timestamp", LocalDateTime.now().toString());
              return errorStats;
          }
      }
      
      /**
       * 统计匹配模式的键数量
       * @param pattern 键模式
       * @return 键数量
       */
      private long countKeysByPattern(String pattern) {
          try {
              long count = 0;
              ScanOptions options = ScanOptions.scanOptions().match(pattern).count(1000).build();
              
              try (Cursor<String> cursor = stringRedisTemplate.scan(options)) {
                  while (cursor.hasNext()) {
                      cursor.next();
                      count++;
                  }
              }
              
              return count;
          } catch (Exception e) {
              log.warn("统计模式 [{}] 的键数量失败: {}", pattern, e.getMessage());
              return 0;
          }
      }
      
      /**
       * 记录处理进度统计到Redis
       */
      private void recordProcessingProgress(ProcessingResults results, int totalPendingDocs) {
          try {
              Map<String, Object> progressInfo = new HashMap<>();
              progressInfo.put("totalSuccess", results.getTotalSuccess());
              progressInfo.put("totalFailed", results.getTotalFailed());
              progressInfo.put("totalSkipped", results.getTotalSkipped());
              progressInfo.put("totalProcessed", results.getTotalProcessed());
              progressInfo.put("totalPendingDocs", totalPendingDocs);
              progressInfo.put("timestamp", LocalDateTime.now().toString());
              progressInfo.put("processingRate", totalPendingDocs > 0 ? 
                  String.format("%.2f", (double) results.getTotalProcessed() / totalPendingDocs * 100) + "%" : "0%");
              
              // 存储最新的处理进度，保存24小时
              String progressKey = "knowledge:check:progress:latest";
              redisManager.set(progressKey, progressInfo, 24 * 3600L);
              
              // 存储历史记录，用时间戳作为key，保存7天
              String historyKey = "knowledge:check:progress:history:" + System.currentTimeMillis();
              redisManager.set(historyKey, progressInfo, 7 * 24 * 3600L);
              
              log.debug("处理进度已记录到Redis: 处理率{}%", progressInfo.get("processingRate"));
              
          } catch (Exception e) {
              log.warn("记录处理进度失败: {}", e.getMessage());
          }
      }
      
      /**
       * 获取处理进度统计信息
       */
      public Map<String, Object> getProcessingProgressInfo() {
          try {
              Map<String, Object> result = new HashMap<>();
              
              // 获取最新进度
              String progressKey = "knowledge:check:progress:latest";
              Map<String, Object> latestProgress = redisManager.getMap(progressKey);
              result.put("latestProgress", latestProgress);
              
              // 获取偏移量信息
              result.put("offsetInfo", getProcessingOffsetInfo());
              
              // 获取当前系统状态
              result.put("isRunning", this.isRunning);
              result.put("currentTime", LocalDateTime.now().toString());
              
              return result;
              
          } catch (Exception e) {
              log.error("获取处理进度信息失败", e);
              return Map.of("error", true, "message", e.getMessage());
          }
      }
      
      /**
       * 获取处理锁
       */
      private Mono<Boolean> acquireProcessingLock() {
          try {
              String lockValue = String.valueOf(System.currentTimeMillis());
              Boolean acquired = stringRedisTemplate.opsForValue().setIfAbsent(PROCESSING_LOCK_KEY, lockValue, 
                  Duration.ofMinutes(LOCK_TIMEOUT_MINUTES));
              boolean lockAcquired = Boolean.TRUE.equals(acquired);
              if (lockAcquired) {
                  log.debug("成功获取处理锁");
              } else {
                  log.debug("无法获取处理锁，可能有其他实例正在处理");
              }
              return Mono.just(lockAcquired);
          } catch (Exception e) {
              log.warn("获取处理锁失败: {}", e.getMessage());
              return Mono.just(false);
          }
      }
      
      /**
       * 释放处理锁
       */
      private void releaseProcessingLock() {
          try {
              boolean released = redisManager.delete(PROCESSING_LOCK_KEY);
              if (released) {
                  log.debug("成功释放处理锁");
              }
          } catch (Exception e) {
              log.warn("释放处理锁失败: {}", e.getMessage());
          }
      }
      
      /**
       * 获取当前处理偏移量
       */
      private Mono<Long> getCurrentProcessingOffset() {
          try {
              String offsetStr = redisManager.get(PROCESSING_OFFSET_KEY);
              long offset = 0L;
              if (offsetStr != null && !offsetStr.isEmpty()) {
                  try {
                      offset = Long.parseLong(offsetStr);
                  } catch (NumberFormatException e) {
                      log.warn("偏移量格式错误，重置为0: {}", offsetStr);
                      offset = 0L;
                  }
              }
              log.debug("当前处理偏移量: {}", offset);
              return Mono.just(offset);
          } catch (Exception e) {
              log.warn("获取处理偏移量失败: {}, 使用默认值0", e.getMessage());
              return Mono.just(0L);
          }
      }
      
      /**
       * 更新处理偏移量
       */
      private void updateProcessingOffset(long newOffset) {
          try {
              boolean updated = redisManager.set(PROCESSING_OFFSET_KEY, String.valueOf(newOffset), 24 * 3600L); // 24小时过期
              if (updated) {
                  log.debug("更新处理偏移量为: {}", newOffset);
              } else {
                  log.warn("更新处理偏移量失败");
              }
          } catch (Exception e) {
              log.warn("更新处理偏移量失败: {}", e.getMessage());
          }
      }
      
      /**
       * 重置处理偏移量
       */
      private void resetProcessingOffset() {
          try {
              boolean deleted = redisManager.delete(PROCESSING_OFFSET_KEY);
              if (deleted) {
                  log.info("已重置处理偏移量");
              }
          } catch (Exception e) {
              log.warn("重置处理偏移量失败: {}", e.getMessage());
          }
      }
      
      /**
       * 手动重置处理偏移量 - 用于外部调用
       */
      public void manualResetProcessingOffset() {
          log.info("手动重置处理偏移量");
          resetProcessingOffset();
      }
      
      /**
       * 获取处理偏移量信息 - 用于监控
       */
      public Map<String, Object> getProcessingOffsetInfo() {
          try {
              Map<String, Object> info = new HashMap<>();
              String offsetStr = redisManager.get(PROCESSING_OFFSET_KEY);
              String lockValue = redisManager.get(PROCESSING_LOCK_KEY);
              
              info.put("currentOffset", offsetStr != null ? Long.parseLong(offsetStr) : 0L);
              info.put("hasLock", lockValue != null);
              info.put("lockValue", lockValue);
              info.put("timestamp", LocalDateTime.now().toString());
              
              return info;
          } catch (Exception e) {
              log.error("获取处理偏移量信息失败", e);
              return Map.of("error", true, "message", e.getMessage());
          }
      }
      
      /**
       * 在Redis中标记文档处理成功，避免重复处理
       */
      private void setDocumentProcessedSuccessfully(String zhenzhiFileId) {
          try {
              String successKey = "doc:success:" + zhenzhiFileId;
              // 设置24小时过期，给足够时间避免重复处理
              boolean saved = redisManager.set(successKey, "true", 86400L);
              if (saved) {
                  log.debug("文档 [{}] 已在Redis中标记为处理成功", zhenzhiFileId);
              }
          } catch (Exception e) {
              log.warn("设置文档成功标记失败: {}", e.getMessage());
          }
      }
} 

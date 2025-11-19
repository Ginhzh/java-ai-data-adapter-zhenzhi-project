package com.weichai.knowledge.service.impl;

import com.weichai.knowledge.service.DataStatisticsService;
import com.weichai.knowledge.dto.*;
import com.weichai.knowledge.entity.*;
import com.weichai.knowledge.repository.*;
import com.weichai.knowledge.utils.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReactiveDataStatisticsServiceImpl implements DataStatisticsService {
    
    private final KafkaMessageLogRepository kafkaMessageLogRepository;
    private final ReactiveUnstructuredDocumentRepository unstructuredDocumentRepository;
    private final ReactiveDocumentStatusRepository documentStatusRepository;
    private final ReactiveKnowledgeRoleSyncLogRepository knowledgeRoleSyncLogRepository;
    private final ReactiveVirtualGroupSyncLogRepository virtualGroupSyncLogRepository;
    private final JsonUtils jsonUtils;
    
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private static final int CACHE_EXPIRY_MINUTES = 5;
    
    @PostConstruct
    public void init() {
        log.info("ReactiveDataStatisticsService初始化完成");
    }
    
    @PreDestroy
    public void shutdown() {
        log.info("正在关闭ReactiveDataStatisticsService...");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public Mono<DailyStatisticsResponse> getDailyStatistics(
            LocalDate startDate, LocalDate endDate) {
        
        final LocalDate finalStartDate = startDate != null ? startDate : LocalDate.now().minusDays(7);
        final LocalDate finalEndDate = endDate != null ? endDate : LocalDate.now();
        
        return collectDailyStatistics(finalStartDate, finalEndDate)
                .map(dailyStats -> DailyStatisticsResponse.builder()
                        .startDate(finalStartDate)
                        .endDate(finalEndDate)
                        .dailyStatistics(dailyStats)
                        .build())
;
    }
    
    @Override
    public Mono<SummaryStatisticsResponse> getSummaryStatistics(
            Integer days, LocalDate startDate, LocalDate endDate) {
        
        // 处理日期参数
        LocalDateTime startDateTime, endDateTime;
        if (startDate != null && endDate != null) {
            startDateTime = startDate.atStartOfDay();
            endDateTime = endDate.atTime(23, 59, 59);
            log.info("使用明确的日期范围: {} 到 {}", startDate, endDate);
        } else {
            endDateTime = LocalDateTime.now();
            startDateTime = endDateTime.minusDays(days != null ? days : 30);
            log.info("使用最近{}天数据: {} 到 {}", days != null ? days : 30, 
                    startDateTime.toLocalDate(), endDateTime.toLocalDate());
        }
        
        log.info("计算从{}到{}的数据接入汇总统计", startDateTime, endDateTime);
        
        return getSystemStatistics(startDateTime, endDateTime)
                .collectList()
                .map(systemStats -> {
                    // 防护性检查，确保 systemStats 不为空
                    List<SystemStatistics> safeSystemStats = systemStats != null ? systemStats : new ArrayList<>();
                    
                    // 计算总量
                    int totalIncremental = safeSystemStats.stream()
                            .filter(Objects::nonNull)
                            .mapToInt(stat -> stat.getIncrementalCount() != null ? stat.getIncrementalCount() : 0)
                            .sum();
                    int totalSuccess = safeSystemStats.stream()
                            .filter(Objects::nonNull)
                            .mapToInt(stat -> stat.getSuccessCount() != null ? stat.getSuccessCount() : 0)
                            .sum();
                    double successRate = totalIncremental > 0 ? 
                            Math.round(totalSuccess * 100.0 / totalIncremental * 100.0) / 100.0 : 0.0;
                    
                    return SummaryStatisticsResponse.builder()
                            .startDate(startDateTime.toLocalDate())
                            .endDate(endDateTime.toLocalDate())
                            .totalIncremental(totalIncremental)
                            .totalSuccess(totalSuccess)
                            .successRate(successRate)
                            .systemStatistics(safeSystemStats)
                            .build();
                });
    }
    
    @Override
    public Mono<DataNamesResponse> getIncomingDataNames(
            Integer limit, Integer offset, String systemName, 
            LocalDate startDate, LocalDate endDate) {
        
        final Integer finalLimit = limit != null ? limit : 100;
        final Integer finalOffset = offset != null ? offset : 0;
        
        // 设置默认日期范围（如果未指定）
        LocalDateTime startDateTime = startDate != null ? 
                startDate.atStartOfDay() : LocalDateTime.of(1970, 1, 1, 0, 0);
        LocalDateTime endDateTime = endDate != null ? 
                endDate.atTime(23, 59, 59) : LocalDateTime.now();
        
        log.info("获取接入数据名称列表，系统: {}, 日期范围: {} 到 {}", 
                systemName != null ? systemName : "全部", startDate != null ? startDate : "全部", 
                endDate != null ? endDate : "当前");
        
        org.springframework.data.domain.PageRequest pageRequest = 
                org.springframework.data.domain.PageRequest.of(finalOffset / finalLimit, finalLimit);
        
        return Mono.zip(
                kafkaMessageLogRepository.findFileAddLogs(systemName, startDateTime, endDateTime, pageRequest).collectList(),
                kafkaMessageLogRepository.countFileAddLogs(systemName, startDateTime, endDateTime)
        ).map(tuple -> {
            List<KafkaMessageLog> paginatedLogs = tuple.getT1();
            long totalCount = tuple.getT2();
            
            // 提取数据
            List<DataNameInfo> dataList = new ArrayList<>();
            for (KafkaMessageLog kafkaLog : paginatedLogs) {
                try {
                    // 解析消息内容（JSON格式）
                    Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                    
                    // 提取文件元数据
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                    
                    String fileName = "未知文件名";
                    String filePath = "未知路径";
                    String fileNumber = "未知文件编号";
                    
                    if (fileMetadata != null) {
                        fileName = (String) fileMetadata.getOrDefault("fileName", "未知文件名");
                        filePath = (String) fileMetadata.getOrDefault("filePath", "未知路径");
                        fileNumber = (String) fileMetadata.getOrDefault("fileNumber", "未知文件编号");
                    }
                    
                    // 构造记录
                    dataList.add(DataNameInfo.builder()
                            .id(kafkaLog.getId())
                            .systemName(kafkaLog.getSystemName())
                            .messageDateTime(kafkaLog.getMessageDateTime() != null ? 
                                    kafkaLog.getMessageDateTime().toString() : null)
                            .createdAt(kafkaLog.getCreatedAt().toString())
                            .fileName(fileName)
                            .filePath(filePath)
                            .fileNumber(fileNumber)
                            .build());
                } catch (Exception e) {
                    log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                    // 添加错误记录
                    dataList.add(DataNameInfo.builder()
                            .id(kafkaLog.getId())
                            .systemName(kafkaLog.getSystemName())
                            .messageDateTime(kafkaLog.getMessageDateTime() != null ? 
                                    kafkaLog.getMessageDateTime().toString() : null)
                            .createdAt(kafkaLog.getCreatedAt().toString())
                            .fileName("解析错误")
                            .filePath("解析错误")
                            .fileNumber("解析错误")
                            .build());
                }
            }
            
            return DataNamesResponse.builder()
                    .total(totalCount)
                    .limit(finalLimit)
                    .offset(finalOffset)
                    .systemName(systemName)
                    .startDate(startDate)
                    .endDate(endDate)
                    .data(dataList)
                    .build();
        });
    }
    
    @Override
    public Mono<FailedIngestionResponse> getFailedIngestionData(
            Integer limit, Integer offset, String systemName,
            LocalDate startDate, LocalDate endDate) {
        
        final Integer finalLimit = limit != null ? limit : 100;
        final Integer finalOffset = offset != null ? offset : 0;
        
        // 设置默认日期范围
        LocalDate finalStartDate = startDate != null ? startDate : LocalDate.now().minusDays(30);
        LocalDate finalEndDate = endDate != null ? endDate : LocalDate.now();
        
        LocalDateTime startDateTime = finalStartDate.atStartOfDay();
        LocalDateTime endDateTime = finalEndDate.atTime(23, 59, 59);
        
        log.info("获取从{}到{}的未成功接入数据，分页: {}-{}", finalStartDate, finalEndDate, finalOffset, finalOffset + finalLimit);
        
        // 计算分页参数（Pageable 以页为单位）
        int pageIndex = finalOffset / finalLimit;
        org.springframework.data.domain.Pageable pageable = org.springframework.data.domain.PageRequest.of(
                pageIndex, finalLimit, org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.DESC, "createdAt"));
        
        return kafkaMessageLogRepository.findFailedIngestionLogs(systemName, startDateTime, endDateTime, pageable)
                .collectList()
                .map(pageContent -> {
                    long totalCount = pageContent.size(); // 简化处理，实际应该有单独的计数查询
                    
                    boolean hasMore = totalCount > (finalOffset + finalLimit);
                    Integer nextOffset = hasMore ? finalOffset + finalLimit : null;
                    
                    // 转换数据格式
                    List<Object> failedList = pageContent.stream().map(message -> {
                        Map<String, Object> content = parseKafkaJson(message.getMessageContent());
                        String fileNumber = "";
                        String fileName = "";
                        String filePath = "";
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileMetadata = (Map<String, Object>) content.get("fileMetadata");
                        if (fileMetadata != null) {
                            fileNumber = (String) fileMetadata.getOrDefault("fileNumber", "");
                            fileName = (String) fileMetadata.getOrDefault("fileName", "");
                            filePath = (String) fileMetadata.getOrDefault("filePath", "");
                        }
                        Map<String, Object> fileData = new HashMap<>();
                        fileData.put("file_number", fileNumber);
                        fileData.put("file_name", fileName);
                        fileData.put("system_name", message.getSystemName());
                        fileData.put("message_type", message.getMessageType());
                        fileData.put("created_at", message.getCreatedAt().toString());
                        fileData.put("file_path", filePath);
                        return fileData;
                    }).collect(Collectors.toList());
                    
                    Map<String, Object> pagination = new HashMap<>();
                    pagination.put("offset", finalOffset);
                    pagination.put("limit", finalLimit);
                    pagination.put("has_more", hasMore);
                    pagination.put("next_offset", nextOffset);
                    
                    return FailedIngestionResponse.builder()
                            .totalCount(totalCount)
                            .failedList(failedList)
                            .pagination(pagination)
                            .build();
                });
    }
    
    @Override
    public Mono<FileStatusResponse> getDetailedFileStatus(String fileNumber) {
        log.info("获取文件{}的详细状态", fileNumber);
        
        return Mono.zip(
                // 查询Kafka消息日志
                kafkaMessageLogRepository.findByMessageType("FILE_ADD")
                        .filter(kafkaLog -> {
                            try {
                                Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                                @SuppressWarnings("unchecked")
                                Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                                if (fileMetadata != null) {
                                    String logFileNumber = (String) fileMetadata.get("fileNumber");
                                    return fileNumber.equals(logFileNumber);
                                }
                            } catch (Exception e) {
                                log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                            }
                            return false;
                        })
                        .sort((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()))
                        .collectList(),
                // 查询非结构化文档表
                unstructuredDocumentRepository.findByFileNumberIn(Flux.just(fileNumber)).next().defaultIfEmpty(null)
        ).map(tuple -> {
            List<KafkaMessageLog> kafkaLogs = tuple.getT1();
            UnstructuredDocument doc = tuple.getT2();
            
            Map<String, Object> kafkaStatus = new HashMap<>();
            Map<String, Object> documentStatus = new HashMap<>();
            
            // 构建结果
            kafkaStatus.put("exists", !kafkaLogs.isEmpty());
            
            // 添加Kafka消息详情
            if (!kafkaLogs.isEmpty()) {
                KafkaMessageLog kafkaLog = kafkaLogs.get(0); // 获取最新的
                try {
                    Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                    
                    kafkaStatus.put("id", kafkaLog.getId());
                    kafkaStatus.put("system_name", kafkaLog.getSystemName());
                    kafkaStatus.put("message_datetime", kafkaLog.getMessageDateTime() != null ? 
                            kafkaLog.getMessageDateTime().toString() : null);
                    kafkaStatus.put("created_at", kafkaLog.getCreatedAt().toString());
                    
                    if (fileMetadata != null) {
                        kafkaStatus.put("file_name", fileMetadata.getOrDefault("fileName", "未知文件名"));
                        kafkaStatus.put("file_path", fileMetadata.getOrDefault("filePath", "未知路径"));
                    }
                } catch (Exception e) {
                    log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                    kafkaStatus.put("parse_error", e.getMessage());
                }
            }
            
            // 添加文档详情
            documentStatus.put("exists", doc != null);
            if (doc != null) {
                documentStatus.put("id", doc.getId());
                documentStatus.put("system_name", doc.getSystemName());
                documentStatus.put("file_name", doc.getFileName());
                documentStatus.put("status", doc.getStatus()); // 0-未成功，1-成功
                documentStatus.put("status_text", doc.getStatus() == 1 ? "成功" : "未成功");
                documentStatus.put("zhenzhi_file_id", doc.getZhenzhiFileId());
                documentStatus.put("version", doc.getVersion());
                documentStatus.put("created_at", doc.getCreatedAt().toString());
                documentStatus.put("updated_at", doc.getUpdatedAt().toString());
            }
            
            return FileStatusResponse.builder()
                    .fileNumber(fileNumber)
                    .kafkaStatus(kafkaStatus)
                    .documentStatus(documentStatus)
                    .build();
        });
    }
    
    @Override
    public Mono<FileStatusResponse> getFileStatusByName(String fileName) {
        log.info("通过文件名{}获取文件状态", fileName);
        
        return kafkaMessageLogRepository.findByMessageType("FILE_ADD")
                .filter(kafkaLog -> {
                    try {
                        Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                        if (fileMetadata != null) {
                            String logFileName = (String) fileMetadata.get("fileName");
                            return fileName.equals(logFileName);
                        }
                    } catch (Exception e) {
                        log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                    }
                    return false;
                })
                .sort((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()))
                .collectList()
                .flatMap(kafkaLogs -> {
                    Map<String, Object> kafkaStatus = new HashMap<>();
                    Map<String, Object> documentStatus = new HashMap<>();
                    
                    // 如果找不到Kafka记录，则无法继续查询
                    if (kafkaLogs.isEmpty()) {
                        kafkaStatus.put("exists", false);
                        documentStatus.put("exists", false);
                        
                        return Mono.just(FileStatusResponse.builder()
                                .fileNumber("")
                                .kafkaStatus(kafkaStatus)
                                .documentStatus(documentStatus)
                                .message("未找到相关Kafka消息记录")
                                .build());
                    }
                    
                    try {
                        // 解析消息内容，获取file_number和其他信息
                        KafkaMessageLog kafkaLog = kafkaLogs.get(0); // 获取最新的
                        Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                        
                        String fileNumber = "";
                        String filePath = "";
                        
                        if (fileMetadata != null) {
                            fileNumber = (String) fileMetadata.getOrDefault("fileNumber", "");
                            filePath = (String) fileMetadata.getOrDefault("filePath", "未知路径");
                        }
                        
                        final String finalFileNumber = fileNumber;
                        final String finalFilePath = filePath;
                        
                        // 使用file_number查询非结构化文档表
                        return unstructuredDocumentRepository.findByFileNumberIn(Flux.just(finalFileNumber))
                                .next()
                                .defaultIfEmpty(null)
                                .map(doc -> {
                                    // 添加Kafka消息详情
                                    kafkaStatus.put("exists", true);
                                    kafkaStatus.put("id", kafkaLog.getId());
                                    kafkaStatus.put("system_name", kafkaLog.getSystemName());
                                    kafkaStatus.put("message_datetime", kafkaLog.getMessageDateTime() != null ? 
                                            kafkaLog.getMessageDateTime().toString() : null);
                                    kafkaStatus.put("created_at", kafkaLog.getCreatedAt().toString());
                                    kafkaStatus.put("file_path", finalFilePath);
                                    
                                    // 添加文档详情
                                    documentStatus.put("exists", doc != null);
                                    if (doc != null) {
                                        documentStatus.put("id", doc.getId());
                                        documentStatus.put("system_name", doc.getSystemName());
                                        documentStatus.put("file_name", doc.getFileName());
                                        documentStatus.put("status", doc.getStatus()); // 0-未成功，1-成功
                                        documentStatus.put("status_text", doc.getStatus() == 1 ? "成功" : "未成功");
                                        documentStatus.put("zhenzhi_file_id", doc.getZhenzhiFileId());
                                        documentStatus.put("version", doc.getVersion());
                                        documentStatus.put("created_at", doc.getCreatedAt().toString());
                                        documentStatus.put("updated_at", doc.getUpdatedAt().toString());
                                    }
                                    
                                    return FileStatusResponse.builder()
                                            .fileNumber(finalFileNumber)
                                            .kafkaStatus(kafkaStatus)
                                            .documentStatus(documentStatus)
                                            .build();
                                });
                                
                    } catch (Exception e) {
                        log.error("解析消息内容失败: {}", e.getMessage());
                        return Mono.just(FileStatusResponse.builder()
                                .fileNumber("")
                                .kafkaStatus(kafkaStatus)
                                .documentStatus(documentStatus)
                                .message("解析错误: " + e.getMessage())
                                .build());
                    }
                });
    }
    
    @Override
    public Mono<FilePermissionStatisticsResponse> getFilePermissionStatistics(
            LocalDate startDate, LocalDate endDate) {
        
        // 设置默认日期范围
        LocalDate finalStartDate = startDate != null ? startDate : LocalDate.now().minusDays(7);
        LocalDate finalEndDate = endDate != null ? endDate : LocalDate.now();
        
        log.info("获取从{}到{}的每日文件权限变更统计", finalStartDate, finalEndDate);
        
        LocalDateTime startDateTime = finalStartDate.atStartOfDay();
        LocalDateTime endDateTime = finalEndDate.atTime(23, 59, 59);
        
        return collectFilePermissionStatistics(startDateTime, endDateTime)
                .collectList()
                .map(dailyStats -> {
                    // 简化每日统计
                    List<Object> simplifiedDailyStats = new ArrayList<>();
                    Map<String, Object> totals = new HashMap<>();
                    totals.put("总Kafka消息数", 0);
                    totals.put("总同步日志数", 0);
                    totals.put("总角色变更数", 0);
                    totals.put("总用户变更数", 0);
                    totals.put("平均成功率", 0.0);
                    
                    for (Map<String, Object> day : dailyStats) {
                        // 提取关键信息
                        Map<String, Object> simplifiedDay = new HashMap<>();
                        simplifiedDay.put("日期", day.get("date"));
                        simplifiedDay.put("Kafka消息数", day.get("kafka_message_count"));
                        simplifiedDay.put("同步日志数", day.get("sync_log_count"));
                        
                        Map<String, Object> roleChange = new HashMap<>();
                        roleChange.put("添加", day.get("sync_add_role_count"));
                        roleChange.put("删除", day.get("sync_del_role_count"));
                        roleChange.put("总计", day.get("sync_total_role_count"));
                        simplifiedDay.put("角色变更", roleChange);
                        
                        Map<String, Object> userChange = new HashMap<>();
                        userChange.put("添加", day.get("sync_add_user_count"));
                        userChange.put("删除", day.get("sync_del_user_count"));
                        userChange.put("总计", day.get("sync_total_user_count"));
                        simplifiedDay.put("用户变更", userChange);
                        
                        simplifiedDay.put("成功率", day.get("success_rate"));
                        simplifiedDailyStats.add(simplifiedDay);
                        
                        // 累计总数
                        totals.put("总Kafka消息数", (Integer) totals.get("总Kafka消息数") + (Integer) day.get("kafka_message_count"));
                        totals.put("总同步日志数", (Integer) totals.get("总同步日志数") + (Integer) day.get("sync_log_count"));
                        totals.put("总角色变更数", (Integer) totals.get("总角色变更数") + (Integer) day.get("sync_total_role_count"));
                        totals.put("总用户变更数", (Integer) totals.get("总用户变更数") + (Integer) day.get("sync_total_user_count"));
                    }
                    
                    // 计算平均成功率
                    if ((Integer) totals.get("总Kafka消息数") > 0) {
                        double avgRate = (Integer) totals.get("总同步日志数") * 100.0 / (Integer) totals.get("总Kafka消息数");
                        totals.put("平均成功率", Math.round(avgRate * 100.0) / 100.0);
                    }
                    
                    // 构建结果
                    Map<String, Object> dateRange = new HashMap<>();
                    dateRange.put("start_date", finalStartDate.toString());
                    dateRange.put("end_date", finalEndDate.toString());
                    
                    return FilePermissionStatisticsResponse.builder()
                            .dateRange(dateRange)
                            .dailyStatistics(simplifiedDailyStats)
                            .summaryStatistics(totals)
                            .build();
                });
    }
    
    @Override
    public Mono<RoleUserStatisticsResponse> getRoleUserStatistics(
            LocalDate startDate, LocalDate endDate) {
        
        // 设置默认日期范围
        LocalDate finalStartDate = startDate != null ? startDate : LocalDate.now().minusDays(7);
        LocalDate finalEndDate = endDate != null ? endDate : LocalDate.now();
        
        log.info("获取从{}到{}的角色用户统计", finalStartDate, finalEndDate);
        
        LocalDateTime startDateTime = finalStartDate.atStartOfDay();
        LocalDateTime endDateTime = finalEndDate.atTime(23, 59, 59);
        
        return collectRoleUserStatistics(startDateTime, endDateTime)
                .collectList()
                .map(dailyStats -> {
                    // 计算汇总统计
                    Map<String, Object> summaryStats = calculateRoleUserSummary(dailyStats);
                    
                    // 构建日期范围
                    Map<String, Object> dateRange = new HashMap<>();
                    dateRange.put("start_date", finalStartDate.toString());
                    dateRange.put("end_date", finalEndDate.toString());
                    
                    return RoleUserStatisticsResponse.builder()
                            .dateRange(dateRange)
                            .dailyStatistics(new ArrayList<>(dailyStats))
                            .summaryStatistics(summaryStats)
                            .build();
                });
    }
    
    @Override
    public Mono<ComprehensiveStatisticsResponse> getComprehensiveStatistics(
            LocalDate startDate, LocalDate endDate, Integer days, String systemName) {
        
        log.info("获取综合统计");
        // TODO: 实现具体逻辑
        return Mono.just(ComprehensiveStatisticsResponse.builder()
                .dateRange(new HashMap<>())
                .systemName(systemName)
                .documentIngestionStatistics(new HashMap<>())
                .systemStatistics(new HashMap<>())
                .filePermissionStatistics(new HashMap<>())
                .roleUserStatistics(new HashMap<>())
                .build())
;
    }
    
    @Override
    public Mono<FileDeletionStatisticsResponse> getFileDeletionStatistics(
            Integer limit, Integer offset, String systemName,
            LocalDate startDate, LocalDate endDate) {
        
        final Integer finalLimit = limit != null ? limit : 100;
        final Integer finalOffset = offset != null ? offset : 0;
        
        // 设置默认日期范围
        LocalDate finalStartDate = startDate != null ? startDate : LocalDate.now().minusDays(30);
        LocalDate finalEndDate = endDate != null ? endDate : LocalDate.now();
        
        LocalDateTime startDateTime = finalStartDate.atStartOfDay();
        LocalDateTime endDateTime = finalEndDate.atTime(23, 59, 59);
        
        log.info("获取从{}到{}的文件删除统计，系统: {}, 分页: {}-{}", 
                finalStartDate, finalEndDate, systemName != null ? systemName : "全部", 
                finalOffset, finalOffset + finalLimit);
        
        org.springframework.data.domain.PageRequest pageRequest = 
                org.springframework.data.domain.PageRequest.of(finalOffset / finalLimit, finalLimit);
        
        return Mono.zip(
                kafkaMessageLogRepository.findFileDeletionLogs(systemName, startDateTime, endDateTime, pageRequest).collectList(),
                kafkaMessageLogRepository.countFileDeletionLogs(systemName, startDateTime, endDateTime),
                calculateFileDeletionSystemStatsOptimized(systemName, startDateTime, endDateTime).collectList()
        ).map(tuple -> {
            List<KafkaMessageLog> paginatedLogs = tuple.getT1();
            long totalCount = tuple.getT2();
            List<Object> systemStatistics = tuple.getT3();
            
            // 转换为删除记录列表
            List<Object> deletionList = new ArrayList<>();
            Set<String> successFileNumbers = new HashSet<>();
            
            for (KafkaMessageLog kafkaLog : paginatedLogs) {
                try {
                    Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                    
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                    
                    String fileNumber = "";
                    String fileName = "";
                    String filePath = "";
                    
                    if (fileMetadata != null) {
                        fileNumber = (String) fileMetadata.getOrDefault("fileNumber", "");
                        fileName = (String) fileMetadata.getOrDefault("fileName", "");
                        filePath = (String) fileMetadata.getOrDefault("filePath", "");
                    }
                    
                    // 检查是否成功删除（在document_status表中该文件的status为1）
                    // 这里简化处理，实际应该异步查询
                    boolean isDeleted = false;
                    if (!fileNumber.isEmpty()) {
                        // 这里需要同步查询，在实际应用中应该改为异步批量处理
                        isDeleted = documentStatusRepository.findByFileNumber(fileNumber)
                                .map(status -> status.getStatus() == 1)
                                .defaultIfEmpty(false)
                                .block();
                    }
                    
                    if (isDeleted) {
                        successFileNumbers.add(fileNumber);
                    }
                    
                    Map<String, Object> deletionRecord = new HashMap<>();
                    deletionRecord.put("file_number", fileNumber);
                    deletionRecord.put("file_name", fileName);
                    deletionRecord.put("file_path", filePath);
                    deletionRecord.put("system_name", kafkaLog.getSystemName());
                    deletionRecord.put("message_type", kafkaLog.getMessageType());
                    deletionRecord.put("created_at", kafkaLog.getCreatedAt().toString());
                    deletionRecord.put("is_deleted", isDeleted);
                    deletionRecord.put("status", isDeleted ? "成功" : "失败");
                    
                    deletionList.add(deletionRecord);
                    
                } catch (Exception e) {
                    log.error("解析删除消息失败: {}", e.getMessage());
                }
            }
            
            // 计算统计数据
            long successCount = successFileNumbers.size();
            double successRate = totalCount > 0 ? Math.round(successCount * 100.0 / totalCount * 100.0) / 100.0 : 0.0;
            
            // 分页信息
            boolean hasMore = totalCount > (finalOffset + finalLimit);
            Integer nextOffset = hasMore ? finalOffset + finalLimit : null;
            
            Map<String, Object> pagination = new HashMap<>();
            pagination.put("offset", finalOffset);
            pagination.put("limit", finalLimit);
            pagination.put("total", totalCount);
            pagination.put("has_more", hasMore);
            pagination.put("next_offset", nextOffset);
            
            return FileDeletionStatisticsResponse.builder()
                    .total(totalCount)
                    .successCount(successCount)
                    .successRate(successRate)
                    .deletionList(deletionList)
                    .systemStatistics(systemStatistics)
                    .pagination(pagination)
                    .build();
        });
    }
    
    @Override
    public Mono<SystemDailyStatisticsResponse> getSystemDailyStatistics() {
        log.info("获取系统每日统计");
        
        // 计算当天的日期范围
        LocalDate today = LocalDate.now();
        LocalDateTime startDateTime = today.atStartOfDay();
        LocalDateTime endDateTime = today.atTime(23, 59, 59);
        
        log.info("获取{}的每个系统综合性数据统计", today);
        
        // 不使用缓存，确保每个系统的数据都是实时计算的
        return kafkaMessageLogRepository.findDistinctSystemNames(startDateTime, endDateTime)
                .collectList()
                .flatMap(systemNames -> {
                    log.info("处理 {} 个系统的统计数据", systemNames.size());
                    
                    // 对每个系统进行统计
                    return Flux.fromIterable(systemNames)
                            .flatMap(systemName -> {
                                log.info("处理系统: {}", systemName);
                                return getSystemComprehensiveStats(systemName, startDateTime, endDateTime);
                            })
                            .collectList()
                            .map(systemStatsList -> {
                                // 计算总计统计
                                SystemDailyStats totalStats = calculateTotalStats(systemStatsList);
                                
                                return SystemDailyStatisticsResponse.builder()
                                        .date(today.toString())
                                        .systemStatistics(systemStatsList)
                                        .totalStatistics(totalStats)
                                        .build();
                            });
                })
                .doOnSuccess(result -> log.info("系统每日统计数据处理完成，共处理 {} 个系统", 
                        result.getSystemStatistics().size()))
                .doOnError(error -> log.error("获取系统每日统计失败: {}", error.getMessage(), error));
    }

    /**
     * 基于Redis优先的数据源获取当天各系统综合统计，
     * 当前实现复用现有逻辑（实时查询），后续可在此处对接Redis计数优先、无缓存降级到数据库的混合逻辑。
     */
    @Override
    public Mono<SystemDailyStatisticsResponse> getSystemDailyStatisticsWithRedis() {
        log.info("获取系统每日统计（Redis优先，当前复用实时查询）");
        return getSystemDailyStatistics();
    }
    
    /**
     * 获取单个系统的综合统计数据 - 对应Python版本的_get_system_comprehensive_stats
     */
    private Mono<SystemDailyStats> getSystemComprehensiveStats(String systemName, 
                                                            LocalDateTime startDateTime, 
                                                            LocalDateTime endDateTime) {
        return Mono.zip(
                getFileAddStatistics(systemName, startDateTime, endDateTime),
                getFileDelStatistics(systemName, startDateTime, endDateTime),
                getFileNotChangeStatistics(systemName, startDateTime, endDateTime),
                getRoleOperationsStatistics(systemName, startDateTime, endDateTime)
        ).map(tuple -> {
            FileOperationStats fileAddStats = tuple.getT1();
            FileOperationStats fileDelStats = tuple.getT2();
            FileNotChangeStats fileNotChangeStats = tuple.getT3();
            RoleOperationStats roleOpsStats = tuple.getT4();
            
            // 构建系统统计数据，对应Python的返回结构
            return SystemDailyStats.builder()
                    .systemName(systemName)
                    .fileOps(FileOpsStats.builder()
                            .add(fileAddStats.getAddCount() != null ? fileAddStats.getAddCount() : 0)
                            .delete(fileDelStats.getDeleteOperations() != null ? fileDelStats.getDeleteOperations() : 0)
                            .build())
                    .fileUpdate(FileUpdateStats.builder()
                            .fileUpdateRecords(fileNotChangeStats.getSyncRecordCount())
                            .roleAdd(fileNotChangeStats.getSyncRoles().getAddCount())
                            .roleDelete(fileNotChangeStats.getSyncRoles().getDelCount())
                            .userAdd(fileNotChangeStats.getSyncUsers().getAddCount())
                            .userDelete(fileNotChangeStats.getSyncUsers().getDelCount())
                            .build())
                    .virtualGroupOps(VirtualGroupOpsStats.builder()
                            .roleGroupCreate(roleOpsStats.getSyncOperations().getOrDefault("ADD_ROLE", 0))
                            .roleGroupCreateUsers(roleOpsStats.getSyncUsers().getOrDefault("ADD_ROLE", 0))
                            .roleGroupDelete(roleOpsStats.getSyncOperations().getOrDefault("DEL_ROLE", 0))
                            .roleAddRecords(roleOpsStats.getSyncOperations().getOrDefault("ROLE_ADD_USER", 0))
                            .roleAddUsers(roleOpsStats.getSyncUsers().getOrDefault("ROLE_ADD_USER", 0))
                            .roleDeleteRecords(roleOpsStats.getSyncOperations().getOrDefault("ROLE_DEL_USER", 0))
                            .roleDeleteUsers(roleOpsStats.getSyncUsers().getOrDefault("ROLE_DEL_USER", 0))
                            .build())
                    .kafkaMessageStats(KafkaMessageStats.builder()
                            .fileAddMessages(fileAddStats.getKafkaMessageCount())
                            .fileDeleteMessages(fileDelStats.getKafkaMessageCount())
                            .fileUpdateMessages(fileNotChangeStats.getKafkaMessageCount())
                            .roleGroupCreateMessages(roleOpsStats.getKafkaMessages().getOrDefault("ADD_ROLE", 0))
                            .roleGroupDeleteMessages(roleOpsStats.getKafkaMessages().getOrDefault("DEL_ROLE", 0))
                            .roleAddUserMessages(roleOpsStats.getKafkaMessages().getOrDefault("ROLE_ADD_USER", 0))
                            .roleDeleteUserMessages(roleOpsStats.getKafkaMessages().getOrDefault("ROLE_DEL_USER", 0))
                            .fileUpdate(FilePermissionUserStats.builder()
                                    .addUsers(fileNotChangeStats.getKafkaUsers().getAddCount())
                                    .deleteUsers(fileNotChangeStats.getKafkaUsers().getDelCount())
                                    .build())
                            .roleUserChange(RoleUserChangeStats.builder()
                                    .addUsers(roleOpsStats.getKafkaUsers().getOrDefault("ROLE_ADD_USER", 0))
                                    .deleteUsers(roleOpsStats.getKafkaUsers().getOrDefault("ROLE_DEL_USER", 0))
                                    .build())
                            .build())
                    .build();
        });
    }
    
    /**
     * 获取FILE_ADD类型消息的统计信息 - 对应Python的_get_file_add_statistics_optimized
     */
    private Mono<FileOperationStats> getFileAddStatistics(String systemName, 
                                                        LocalDateTime startDateTime, 
                                                        LocalDateTime endDateTime) {
        log.info("获取系统 {} 的文件新增统计，时间范围: {} - {}", systemName, startDateTime, endDateTime);
        
        // 获取Kafka消息数量
        Mono<Long> kafkaMessageCount = kafkaMessageLogRepository.countBySystemAndMessageType(systemName, "FILE_ADD", startDateTime, endDateTime);
        
        // 获取实际新增的文档数量（在指定时间范围内创建且系统名称匹配）
        Mono<Long> actualAddCount = unstructuredDocumentRepository.findByCreatedAtBetween(startDateTime, endDateTime)
                .filter(doc -> systemName.equals(doc.getSystemName()))
                .count();
        
        return Mono.zip(kafkaMessageCount, actualAddCount)
                .map(tuple -> {
                    Long kafkaCount = tuple.getT1();
                    Long addCount = tuple.getT2();
                    
                    log.info("系统 {} FILE_ADD统计 - Kafka消息数: {}, 实际新增文档数: {}", systemName, kafkaCount, addCount);
                    
                    return FileOperationStats.builder()
                            .kafkaMessageCount(kafkaCount.intValue())
                            .addCount(addCount.intValue())
                            .updateCount(0) // 更新统计单独处理
                            .totalOperations(addCount.intValue())
                            .build();
                });
    }
    
    /**
     * 获取FILE_DEL类型消息的统计信息 - 支持Redis统计，对应Python的_get_file_del_statistics_optimized
     */
    private Mono<FileOperationStats> getFileDelStatistics(String systemName, 
                                                        LocalDateTime startDateTime, 
                                                        LocalDateTime endDateTime) {
        log.info("获取系统 {} 的文件删除统计，时间范围: {} - {}", systemName, startDateTime, endDateTime);
        
        // 获取Kafka消息数量
        Mono<Long> kafkaMessageCount = kafkaMessageLogRepository.countBySystemAndMessageType(systemName, "FILE_DEL", startDateTime, endDateTime);
        
        // 对于删除操作，我们无法直接从unstructured_documents表统计，因为删除的文档可能已经不存在
        // 所以删除数量主要依赖Kafka消息统计
        return kafkaMessageCount
                .map(kafkaCount -> {
                    log.info("系统 {} FILE_DEL统计 - Kafka消息数: {}", systemName, kafkaCount);
                    
                    return FileOperationStats.builder()
                            .kafkaMessageCount(kafkaCount.intValue())
                            .deleteOperations(kafkaCount.intValue())
                            .build();
                });
    }
    
    /**
     * 获取FILE_NOT_CHANGE类型消息的统计信息 - 对应Python的_get_file_not_change_statistics_optimized
     */
    private Mono<FileNotChangeStats> getFileNotChangeStatistics(String systemName, 
                                                              LocalDateTime startDateTime, 
                                                              LocalDateTime endDateTime) {
        return Mono.zip(
                // 统计Kafka消息数量
                kafkaMessageLogRepository.countBySystemAndMessageType(systemName, "FILE_NOT_CHANGE", startDateTime, endDateTime),
                // 解析Kafka消息中的用户数量
                parseFileNotChangeKafkaUsers(systemName, startDateTime, endDateTime),
                // 统计同步记录数量
                knowledgeRoleSyncLogRepository.countBySystemAndMessageType(systemName, "FILE_NOT_CHANGE", startDateTime, endDateTime),
                // 获取同步表中的统计
                getFileNotChangeSyncStats(systemName, startDateTime, endDateTime)
        ).map(tuple -> {
            Long kafkaMessageCount = tuple.getT1();
            UserStats kafkaUsers = tuple.getT2();
            Long syncRecordCount = tuple.getT3();
            SyncStats syncStats = tuple.getT4();
            
            return FileNotChangeStats.builder()
                    .kafkaMessageCount(kafkaMessageCount.intValue())
                    .syncRecordCount(syncRecordCount.intValue())
                    .kafkaUsers(UserStats.builder()
                            .addCount(kafkaUsers.getAddCount())
                            .delCount(kafkaUsers.getDelCount())
                            .build())
                    .syncRoles(UserStats.builder()
                            .addCount(syncStats.getRoles().getAddCount())
                            .delCount(syncStats.getRoles().getDelCount())
                            .build())
                    .syncUsers(UserStats.builder()
                            .addCount(syncStats.getUsers().getAddCount())
                            .delCount(syncStats.getUsers().getDelCount())
                            .build())
                    .build();
        });
    }
    
    /**
     * 获取角色相关操作统计信息 - 对应Python的_get_role_operations_statistics_optimized
     */
    private Mono<RoleOperationStats> getRoleOperationsStatistics(String systemName, 
                                                               LocalDateTime startDateTime, 
                                                               LocalDateTime endDateTime) {
        List<String> roleMessageTypes = Arrays.asList("ADD_ROLE", "DEL_ROLE", "ROLE_ADD_USER", "ROLE_DEL_USER");
        
        return Mono.zip(
                // 批量查询Kafka消息
                Flux.fromIterable(roleMessageTypes)
                        .flatMap(messageType -> 
                            kafkaMessageLogRepository.findBySystemAndMessageType(systemName, messageType, startDateTime, endDateTime)
                                    .collectList()
                                    .map(messages -> Map.entry(messageType, messages)))
                        .collectMap(Map.Entry::getKey, Map.Entry::getValue),
                // 批量查询同步日志
                Flux.fromIterable(roleMessageTypes)
                        .flatMap(messageType -> 
                            virtualGroupSyncLogRepository.findBySystemAndMessageType(systemName, messageType, startDateTime, endDateTime)
                                    .collectList()
                                    .map(logs -> Map.entry(messageType, logs)))
                        .collectMap(Map.Entry::getKey, Map.Entry::getValue)
        ).map(tuple -> {
            Map<String, List<KafkaMessageLog>> kafkaMsgsByType = tuple.getT1();
            Map<String, List<VirtualGroupSyncLog>> syncLogsByType = tuple.getT2();
            
            return processRoleOperationsData(kafkaMsgsByType, syncLogsByType);
        });
    }
    
    /**
     * 解析FILE_NOT_CHANGE消息中的用户数量
     */
    private Mono<UserStats> parseFileNotChangeKafkaUsers(String systemName, 
                                                        LocalDateTime startDateTime, 
                                                        LocalDateTime endDateTime) {
        return kafkaMessageLogRepository.findBySystemAndMessageType(systemName, "FILE_NOT_CHANGE", startDateTime, endDateTime)
                .map(msg -> {
                    try {
                        Map<String, Object> messageContent = parseKafkaJson(msg.getMessageContent());
                        
                        @SuppressWarnings("unchecked")
                        List<Object> addUsers = (List<Object>) messageContent.get("fileAddUserList");
                        @SuppressWarnings("unchecked")
                        List<Object> delUsers = (List<Object>) messageContent.get("fileDelUserList");
                        
                        int addUserCount = addUsers != null ? addUsers.size() : 0;
                        int delUserCount = delUsers != null ? delUsers.size() : 0;
                        
                        return new int[]{addUserCount, delUserCount};
                    } catch (Exception e) {
                        log.error("解析FILE_NOT_CHANGE消息内容失败: {}, 消息ID: {}", e.getMessage(), msg.getId());
                        return new int[]{0, 0};
                    }
                })
                .reduce(new int[]{0, 0}, (acc, curr) -> new int[]{acc[0] + curr[0], acc[1] + curr[1]})
                .map(result -> UserStats.builder()
                        .addCount(result[0])
                        .delCount(result[1])
                        .build());
    }
    
    /**
     * 获取FILE_NOT_CHANGE同步统计
     */
    private Mono<SyncStats> getFileNotChangeSyncStats(String systemName, 
                                                     LocalDateTime startDateTime, 
                                                     LocalDateTime endDateTime) {
        return knowledgeRoleSyncLogRepository.findSyncLogsBySystemAndMessageType(systemName, "FILE_NOT_CHANGE", startDateTime, endDateTime)
                .map(syncLog -> {
                    try {
                        int addRoleCount = syncLog.getAddRoleList() != null ? syncLog.getAddRoleList().size() : 0;
                        int delRoleCount = syncLog.getDelRoleList() != null ? syncLog.getDelRoleList().size() : 0;
                        int addUserCount = syncLog.getAddUserList() != null ? syncLog.getAddUserList().size() : 0;
                        int delUserCount = syncLog.getDelUserList() != null ? syncLog.getDelUserList().size() : 0;
                        
                        return new int[]{addRoleCount, delRoleCount, addUserCount, delUserCount};
                    } catch (Exception e) {
                        log.error("解析同步列表失败: {}", e.getMessage());
                        return new int[]{0, 0, 0, 0};
                    }
                })
                .reduce(new int[]{0, 0, 0, 0}, (acc, curr) -> new int[]{
                        acc[0] + curr[0], acc[1] + curr[1], acc[2] + curr[2], acc[3] + curr[3]
                })
                .map(result -> SyncStats.builder()
                        .roles(UserStats.builder()
                                .addCount(result[0])
                                .delCount(result[1])
                                .build())
                        .users(UserStats.builder()
                                .addCount(result[2])
                                .delCount(result[3])
                                .build())
                        .build());
    }
    
    /**
     * 处理角色操作数据
     */
    private RoleOperationStats processRoleOperationsData(Map<String, List<KafkaMessageLog>> kafkaMsgsByType, 
                                                        Map<String, List<VirtualGroupSyncLog>> syncLogsByType) {
        Map<String, Integer> kafkaMessages = new HashMap<>();
        Map<String, Integer> syncOperations = new HashMap<>();
        Map<String, Integer> kafkaUsers = new HashMap<>();
        Map<String, Integer> syncUsers = new HashMap<>();
        
        // 处理各种消息类型
        for (String msgType : Arrays.asList("ADD_ROLE", "DEL_ROLE", "ROLE_ADD_USER", "ROLE_DEL_USER")) {
            List<KafkaMessageLog> kafkaMsgs = kafkaMsgsByType.getOrDefault(msgType, new ArrayList<>());
            List<VirtualGroupSyncLog> syncLogs = syncLogsByType.getOrDefault(msgType, new ArrayList<>());
            
            kafkaMessages.put(msgType, kafkaMsgs.size());
            syncOperations.put(msgType, syncLogs.size());
            
            // 处理用户数量统计
            if (msgType.equals("ROLE_ADD_USER") || msgType.equals("ROLE_DEL_USER")) {
                int kafkaUserCount = countUsersInKafkaMessages(kafkaMsgs);
                int syncUserCount = countUsersInSyncLogs(syncLogs, msgType);
                
                kafkaUsers.put(msgType, kafkaUserCount);
                syncUsers.put(msgType, syncUserCount);
            } else if (msgType.equals("ADD_ROLE")) {
                // ADD_ROLE时统计创建角色的用户数
                int syncUserCount = countUsersInSyncLogs(syncLogs, msgType);
                syncUsers.put(msgType, syncUserCount);
            }
        }
        
        return RoleOperationStats.builder()
                .kafkaMessages(kafkaMessages)
                .syncOperations(syncOperations)
                .kafkaUsers(kafkaUsers)
                .syncUsers(syncUsers)
                .build();
    }
    
    /**
     * 统计Kafka消息中的用户数量
     */
    private int countUsersInKafkaMessages(List<KafkaMessageLog> kafkaMsgs) {
        int totalUsers = 0;
        
        for (KafkaMessageLog msg : kafkaMsgs) {
            try {
                Map<String, Object> messageContent = parseKafkaJson(msg.getMessageContent());
                List<Object> userList = findUserListInMessage(messageContent, 
                        new String[]{"userList", "user_list", "UserList", "users", "addUserList", "delUserList"});
                if (userList != null) {
                    totalUsers += userList.size();
                }
            } catch (Exception e) {
                log.error("统计Kafka消息用户数失败: {}", e.getMessage());
            }
        }
        
        return totalUsers;
    }
    
    /**
     * 统计同步日志中的用户数量
     */
    private int countUsersInSyncLogs(List<VirtualGroupSyncLog> syncLogs, String msgType) {
        int totalUsers = 0;
        
        for (VirtualGroupSyncLog syncLog : syncLogs) {
            try {
                if ((msgType.equals("ROLE_ADD_USER") || msgType.equals("ADD_ROLE")) && syncLog.getAddUserList() != null) {
                    totalUsers += syncLog.getAddUserList().size();
                }
                
                if (msgType.equals("ROLE_DEL_USER") && syncLog.getDelUserList() != null) {
                    totalUsers += syncLog.getDelUserList().size();
                }
            } catch (Exception e) {
                log.error("统计同步日志用户数失败: {}", e.getMessage());
            }
        }
        
        return totalUsers;
    }
    
    // ================== 数据结构类 ==================
    
    /**
     * 文件操作统计数据结构
     */
    @lombok.Builder
    @lombok.Data
    private static class FileOperationStats {
        private Integer kafkaMessageCount;
        private Integer addCount;
        private Integer updateCount;
        private Integer totalOperations;
        private Integer deleteOperations;
    }
    
    /**
     * FILE_NOT_CHANGE统计数据结构
     */
    @lombok.Builder
    @lombok.Data
    private static class FileNotChangeStats {
        private Integer kafkaMessageCount;
        private Integer syncRecordCount;
        private UserStats kafkaUsers;
        private UserStats syncRoles;
        private UserStats syncUsers;
    }
    
    /**
     * 角色操作统计数据结构
     */
    @lombok.Builder
    @lombok.Data
    private static class RoleOperationStats {
        private Map<String, Integer> kafkaMessages;
        private Map<String, Integer> syncOperations;
        private Map<String, Integer> kafkaUsers;
        private Map<String, Integer> syncUsers;
    }
    
    /**
     * 用户统计数据结构
     */
    @lombok.Builder
    @lombok.Data
    private static class UserStats {
        private Integer addCount;
        private Integer delCount;
    }
    
    /**
     * 同步统计数据结构
     */
    @lombok.Builder
    @lombok.Data
    private static class SyncStats {
        private UserStats roles;
        private UserStats users;
    }
    
    // ================== 辅助方法 ==================
    
    /**
     * 收集每日统计数据的响应式方法
     */
    private Mono<List<DailyStatistics>> collectDailyStatistics(LocalDate startDate, LocalDate endDate) {
        return Flux.range(0, (int) java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate) + 1)
                .map(i -> startDate.plusDays(i))
                .flatMap(currentDate -> {
                    LocalDateTime dayStart = currentDate.atStartOfDay();
                    LocalDateTime dayEnd = currentDate.plusDays(1).atStartOfDay().minusSeconds(1);
                    
                    return Mono.zip(
                            getIncrementalStats(dayStart, dayEnd),
                            getUniqueFileNumbers(dayStart, dayEnd)
                    ).flatMap(tuple -> {
                        Map<String, Integer> incrementalStats = tuple.getT1();
                        Set<String> uniqueFileNumbers = tuple.getT2();
                        
                        int addCount = incrementalStats.get("add_count");
                        int updateCount = incrementalStats.get("update_count");
                        int totalCount = uniqueFileNumbers.size();
                        
                        return getSuccessCount(dayStart, dayEnd, uniqueFileNumbers)
                                .map(successCount -> {
                                    double successRate = totalCount > 0 ? 
                                            Math.round(successCount * 100.0 / totalCount * 100.0) / 100.0 : 0.0;
                                    
                                    return DailyStatistics.builder()
                                            .date(currentDate)
                                            .addCount(addCount)
                                            .updateCount(updateCount)
                                            .totalCount(totalCount)
                                            .successCount(successCount)
                                            .successRate(successRate)
                                            .build();
                                });
                    });
                })
                .collectList();
    }
    
    /**
     * 解析Kafka JSON消息内容
     */
    private Map<String, Object> parseKafkaJson(Object raw) {
        if (raw == null) {
            return new HashMap<>();
        }
        
        if (raw instanceof String) {
            String jsonString = (String) raw;
            if (jsonString.trim().isEmpty()) {
                return new HashMap<>();
            }
            try {
                Map<String, Object> parsed = jsonUtils.parseJsonToMap(jsonString);
                return parsed != null ? parsed : new HashMap<>();
            } catch (Exception e) {
                log.error("JSON解析失败: {}, 原始数据: {}", e.getMessage(), raw);
                return new HashMap<>();
            }
        } else if (raw instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) raw;
            return map != null ? map : new HashMap<>();
        }
        return new HashMap<>();
    }
    
    /**
     * 获取指定日期范围内的文件新增和更新统计
     */
    private Mono<Map<String, Integer>> getIncrementalStats(LocalDateTime dayStart, LocalDateTime dayEnd) {
        return kafkaMessageLogRepository.findByMessageTypeAndCreatedAtBetween("FILE_ADD", dayStart, dayEnd)
                .collectList()
                .flatMap(fileAddRecords -> {
                    Map<String, KafkaMessageLog> fileNumberToLog = new HashMap<>();
                    Set<String> fileNumbers = new HashSet<>();
                    
                    for (KafkaMessageLog kafkaLog : fileAddRecords) {
                        if (kafkaLog == null || kafkaLog.getMessageContent() == null) {
                            continue;
                        }
                        try {
                            Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                            @SuppressWarnings("unchecked")
                            Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                            
                            if (fileMetadata != null) {
                                String fileNumber = (String) fileMetadata.get("fileNumber");
                                if (fileNumber != null && !fileNumber.trim().isEmpty()) {
                                    fileNumbers.add(fileNumber);
                                    if (!fileNumberToLog.containsKey(fileNumber) || 
                                        kafkaLog.getCreatedAt().isAfter(fileNumberToLog.get(fileNumber).getCreatedAt())) {
                                        fileNumberToLog.put(fileNumber, kafkaLog);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                        }
                    }
                    
                    if (fileNumbers.isEmpty()) {
                        Map<String, Integer> result = new HashMap<>();
                        result.put("add_count", 0);
                        result.put("update_count", 0);
                        return Mono.just(result);
                    }
                    
                    return unstructuredDocumentRepository.findByFileNumberIn(Flux.fromIterable(fileNumbers))
                            .collectList()
                            .map(existingDocs -> {
                                Map<String, UnstructuredDocument> docMap = existingDocs.stream()
                                        .collect(Collectors.toMap(UnstructuredDocument::getFileNumber, doc -> doc));
                                
                                int addCount = 0;
                                int updateCount = 0;
                                
                                for (String fileNumber : fileNumbers) {
                                    KafkaMessageLog kafkaLog = fileNumberToLog.get(fileNumber);
                                    if (kafkaLog == null) continue;
                                    
                                    UnstructuredDocument doc = docMap.get(fileNumber);
                                    if (doc != null) {
                                        if (doc.getCreatedAt().isBefore(kafkaLog.getCreatedAt())) {
                                            updateCount++;
                                        } else if (!doc.getCreatedAt().isBefore(dayStart) && !doc.getCreatedAt().isAfter(dayEnd)) {
                                            addCount++;
                                        }
                                    }
                                }
                                
                                Map<String, Integer> result = new HashMap<>();
                                result.put("add_count", addCount);
                                result.put("update_count", updateCount);
                                return result;
                            });
                });
    }
    
    /**
     * 获取唯一文件编号集合
     */
    private Mono<Set<String>> getUniqueFileNumbers(LocalDateTime dayStart, LocalDateTime dayEnd) {
        return kafkaMessageLogRepository.findByMessageTypeAndCreatedAtBetween("FILE_ADD", dayStart, dayEnd)
                .map(kafkaLog -> {
                    if (kafkaLog == null || kafkaLog.getMessageContent() == null) {
                        return null;
                    }
                    try {
                        Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                        
                        if (fileMetadata != null) {
                            String fileNumber = (String) fileMetadata.get("fileNumber");
                            return fileNumber != null && !fileNumber.trim().isEmpty() ? fileNumber : null;
                        }
                    } catch (Exception e) {
                        log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
    
    /**
     * 获取指定日期范围内的成功接入文件数量
     */
    private Mono<Integer> getSuccessCount(LocalDateTime dayStart, LocalDateTime dayEnd, Set<String> uniqueFileNumbers) {
        if (uniqueFileNumbers.isEmpty()) {
            return Mono.just(0);
        }
        
        return unstructuredDocumentRepository.findSuccessDocumentsByFileNumbers(
                Flux.fromIterable(uniqueFileNumbers), dayStart, dayEnd)
                .map(UnstructuredDocument::getFileNumber)
                .distinct()
                .count()
                .map(Long::intValue);
    }
    
    /**
     * 获取按来源系统分组的统计数据
     */
    private Flux<SystemStatistics> getSystemStatistics(LocalDateTime startDate, LocalDateTime endDate) {
        return kafkaMessageLogRepository.findDistinctSystemNamesByMessageTypeAndCreatedAtBetween("FILE_ADD", startDate, endDate)
                .filter(systemName -> systemName != null && !systemName.trim().isEmpty()) // 过滤null和空字符串
                .flatMap(systemName -> {
                    return Mono.zip(
                            getSystemIncrementalStats(systemName, startDate, endDate),
                            getSystemUniqueFileNumbers(systemName, startDate, endDate)
                    ).flatMap(tuple -> {
                        Map<String, Integer> rawIncrementalStats = tuple.getT1();
                        Set<String> rawUniqueFileNumbers = tuple.getT2();
                        
                        // 防护性检查 - 创建 final 变量
                        final Map<String, Integer> incrementalStats;
                        if (rawIncrementalStats == null) {
                            incrementalStats = new HashMap<>();
                            incrementalStats.put("add_count", 0);
                            incrementalStats.put("update_count", 0);
                        } else {
                            incrementalStats = rawIncrementalStats;
                        }
                        
                        final Set<String> uniqueFileNumbers = rawUniqueFileNumbers != null ? rawUniqueFileNumbers : new HashSet<>();
                        final int incrementalCount = uniqueFileNumbers.size();
                        
                        return getSystemSuccessCount(systemName, startDate, endDate, uniqueFileNumbers)
                                .map(successCount -> {
                                    // 确保所有值都不为null
                                    int safeSuccessCount = successCount != null ? successCount : 0;
                                    double successRate = incrementalCount > 0 ? 
                                            Math.round(safeSuccessCount * 100.0 / incrementalCount * 100.0) / 100.0 : 0.0;
                                    
                                    // 防护性检查，确保没有null值
                                    Integer addCount = incrementalStats.get("add_count");
                                    Integer updateCount = incrementalStats.get("update_count");
                                    
                                    SystemStatistics result = SystemStatistics.builder()
                                            .systemName(systemName != null ? systemName : "")
                                            .incrementalCount(incrementalCount)
                                            .addCount(addCount != null ? addCount : 0)
                                            .updateCount(updateCount != null ? updateCount : 0)
                                            .successCount(safeSuccessCount)
                                            .successRate(successRate)
                                            .build();
                                    
                                    // 确保构建的对象不为null
                                    if (result == null) {
                                        throw new RuntimeException("构建SystemStatistics对象失败");
                                    }
                                    
                                    return result;
                                })
                                .onErrorReturn(SystemStatistics.builder()
                                        .systemName(systemName != null ? systemName : "")
                                        .incrementalCount(0)
                                        .addCount(0)
                                        .updateCount(0)
                                        .successCount(0)
                                        .successRate(0.0)
                                        .build());
                    });
                })
                .onErrorContinue((error, obj) -> {
                    log.error("处理系统统计时发生错误: {}, 对象: {}", error.getMessage(), obj);
                });
    }
    
    /**
     * 获取指定系统在日期范围内的文件新增和更新统计
     */
    private Mono<Map<String, Integer>> getSystemIncrementalStats(String systemName, LocalDateTime startDate, LocalDateTime endDate) {
        return kafkaMessageLogRepository.findBySystemAndMessageType(systemName, "FILE_ADD", startDate, endDate)
                .collectList()
                .flatMap(fileAddRecords -> {
                    Map<String, KafkaMessageLog> fileNumberToLog = new HashMap<>();
                    Set<String> fileNumbers = new HashSet<>();
                    
                    for (KafkaMessageLog kafkaLog : fileAddRecords) {
                        if (kafkaLog == null || kafkaLog.getMessageContent() == null) {
                            continue;
                        }
                        try {
                            Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                            @SuppressWarnings("unchecked")
                            Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                            
                            if (fileMetadata != null) {
                                String fileNumber = (String) fileMetadata.get("fileNumber");
                                if (fileNumber != null && !fileNumber.trim().isEmpty()) {
                                    fileNumbers.add(fileNumber);
                                    if (!fileNumberToLog.containsKey(fileNumber) || 
                                        kafkaLog.getCreatedAt().isAfter(fileNumberToLog.get(fileNumber).getCreatedAt())) {
                                        fileNumberToLog.put(fileNumber, kafkaLog);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                        }
                    }
                    
                    if (fileNumbers.isEmpty()) {
                        Map<String, Integer> result = new HashMap<>();
                        result.put("add_count", 0);
                        result.put("update_count", 0);
                        return Mono.just(result);
                    }
                    
                    return unstructuredDocumentRepository.findByFileNumberIn(Flux.fromIterable(fileNumbers))
                            .filter(doc -> systemName.equals(doc.getSystemName()))
                            .collectList()
                            .map(existingDocs -> {
                                Map<String, UnstructuredDocument> docMap = existingDocs.stream()
                                        .collect(Collectors.toMap(UnstructuredDocument::getFileNumber, doc -> doc));
                                
                                int addCount = 0;
                                int updateCount = 0;
                                
                                for (String fileNumber : fileNumbers) {
                                    KafkaMessageLog kafkaLog = fileNumberToLog.get(fileNumber);
                                    if (kafkaLog == null) continue;
                                    
                                    UnstructuredDocument doc = docMap.get(fileNumber);
                                    if (doc != null) {
                                        if (doc.getCreatedAt().isBefore(kafkaLog.getCreatedAt())) {
                                            updateCount++;
                                        } else if (!doc.getCreatedAt().isBefore(startDate) && !doc.getCreatedAt().isAfter(endDate)) {
                                            addCount++;
                                        }
                                    }
                                }
                                
                                Map<String, Integer> result = new HashMap<>();
                                result.put("add_count", addCount);
                                result.put("update_count", updateCount);
                                return result;
                            });
                });
    }
    
    /**
     * 获取指定系统的唯一文件编号集合
     */
    private Mono<Set<String>> getSystemUniqueFileNumbers(String systemName, LocalDateTime startDate, LocalDateTime endDate) {
        return kafkaMessageLogRepository.findBySystemAndMessageType(systemName, "FILE_ADD", startDate, endDate)
                .map(kafkaLog -> {
                    if (kafkaLog == null || kafkaLog.getMessageContent() == null) {
                        return null;
                    }
                    try {
                        Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                        
                        if (fileMetadata != null) {
                            String fileNumber = (String) fileMetadata.get("fileNumber");
                            return fileNumber != null && !fileNumber.trim().isEmpty() ? fileNumber : null;
                        }
                    } catch (Exception e) {
                        log.error("解析消息内容失败: {}, 消息ID: {}", e.getMessage(), kafkaLog.getId());
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
    
    /**
     * 获取指定系统的成功数据统计
     */
    private Mono<Integer> getSystemSuccessCount(String systemName, LocalDateTime startDate, LocalDateTime endDate, Set<String> uniqueFileNumbers) {
        if (uniqueFileNumbers == null || uniqueFileNumbers.isEmpty()) {
            return Mono.just(0);
        }
        
        return unstructuredDocumentRepository.findByFileNumberIn(Flux.fromIterable(uniqueFileNumbers))
                .filter(doc -> {
                    // 系统名称检查
                    if (systemName == null || !systemName.equals(doc.getSystemName())) {
                        return false;
                    }
                    
                    // 时间检查 - 添加null安全检查
                    LocalDateTime docCreatedAt = doc.getCreatedAt();
                    LocalDateTime docUpdatedAt = doc.getUpdatedAt();
                    
                    // 如果创建时间为null，跳过此记录
                    if (docCreatedAt == null) {
                        return false;
                    }
                    
                    // 检查创建时间是否在范围内
                    boolean createdInRange = !docCreatedAt.isBefore(startDate) && !docCreatedAt.isAfter(endDate);
                    
                    // 检查更新时间是否在范围内（如果更新时间不为null）
                    boolean updatedInRange = false;
                    if (docUpdatedAt != null) {
                        updatedInRange = docCreatedAt.isBefore(startDate) &&
                                       !docUpdatedAt.isBefore(startDate) && 
                                       !docUpdatedAt.isAfter(endDate);
                    }
                    
                    return createdInRange || updatedInRange;
                })
                .map(UnstructuredDocument::getFileNumber)
                .distinct()
                .count()
                .map(Long::intValue)
                .onErrorReturn(0); // 如果发生错误，返回0
    }
    
    /**
     * 收集每日文件权限变更统计数据
     */
    private Flux<Map<String, Object>> collectFilePermissionStatistics(LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return Flux.range(0, (int) java.time.temporal.ChronoUnit.DAYS.between(startDateTime.toLocalDate(), endDateTime.toLocalDate()) + 1)
                .map(i -> startDateTime.plusDays(i))
                .flatMap(currentDate -> {
                    LocalDateTime nextDate = currentDate.plusDays(1);
                    LocalDateTime dayStart = currentDate;
                    LocalDateTime dayEnd = nextDate.minusSeconds(1);
                    
                    return Mono.zip(
                            kafkaMessageLogRepository.findByMessageTypeAndCreatedAtBetween("FILE_NOT_CHANGE", dayStart, dayEnd).count(),
                            knowledgeRoleSyncLogRepository.findByMessageTypeAndCreatedAtBetween("FILE_NOT_CHANGE", dayStart, dayEnd).collectList()
                    ).map(tuple -> {
                        long kafkaCount = tuple.getT1();
                        List<KnowledgeRoleSyncLog> syncLogs = tuple.getT2();
                        
                        int syncLogCount = syncLogs.size();
                        
                        // 从同步表中获取角色和用户变更数量统计
                        int syncAddRoleCount = 0;
                        int syncDelRoleCount = 0;
                        int syncAddUserCount = 0;
                        int syncDelUserCount = 0;
                        
                        for (KnowledgeRoleSyncLog syncLog : syncLogs) {
                            if (syncLog.getAddRoleList() != null) {
                                syncAddRoleCount += syncLog.getAddRoleList().size();
                            }
                            if (syncLog.getDelRoleList() != null) {
                                syncDelRoleCount += syncLog.getDelRoleList().size();
                            }
                            if (syncLog.getAddUserList() != null) {
                                syncAddUserCount += syncLog.getAddUserList().size();
                            }
                            if (syncLog.getDelUserList() != null) {
                                syncDelUserCount += syncLog.getDelUserList().size();
                            }
                        }
                        
                        int syncTotalRoleCount = syncAddRoleCount + syncDelRoleCount;
                        int syncTotalUserCount = syncAddUserCount + syncDelUserCount;
                        
                        // 计算成功率
                        double successRate = kafkaCount > 0 ? Math.round(syncLogCount * 100.0 / kafkaCount * 100.0) / 100.0 : 0.0;
                        
                        // 添加到结果列表
                        Map<String, Object> dayStats = new HashMap<>();
                        dayStats.put("date", currentDate.toLocalDate().toString());
                        dayStats.put("kafka_message_count", (int) kafkaCount);
                        dayStats.put("sync_log_count", syncLogCount);
                        dayStats.put("sync_add_role_count", syncAddRoleCount);
                        dayStats.put("sync_del_role_count", syncDelRoleCount);
                        dayStats.put("sync_total_role_count", syncTotalRoleCount);
                        dayStats.put("sync_add_user_count", syncAddUserCount);
                        dayStats.put("sync_del_user_count", syncDelUserCount);
                        dayStats.put("sync_total_user_count", syncTotalUserCount);
                        dayStats.put("success_rate", successRate);
                        
                        return dayStats;
                    });
                });
    }
    
    /**
     * 收集角色用户统计数据
     */
    private Flux<Map<String, Object>> collectRoleUserStatistics(LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return Flux.range(0, (int) java.time.temporal.ChronoUnit.DAYS.between(startDateTime.toLocalDate(), endDateTime.toLocalDate()) + 1)
                .map(i -> startDateTime.plusDays(i))
                .flatMap(currentDate -> {
                    LocalDateTime nextDate = currentDate.plusDays(1);
                    LocalDateTime dayStart = currentDate;
                    LocalDateTime dayEnd = nextDate.minusSeconds(1);
                    
                    return Mono.zip(
                            kafkaMessageLogRepository.findByCreatedAtBetween(dayStart, dayEnd)
                                    .filter(log -> "ADD_ROLE".equals(log.getMessageType()) || 
                                                  "DEL_ROLE".equals(log.getMessageType()) ||
                                                  "ROLE_ADD_USER".equals(log.getMessageType()) ||
                                                  "ROLE_DEL_USER".equals(log.getMessageType()))
                                    .count(),
                            virtualGroupSyncLogRepository.findByCreatedAtBetween(dayStart, dayEnd).collectList(),
                            kafkaMessageLogRepository.findByCreatedAtBetween(dayStart, dayEnd)
                                    .filter(log -> "ADD_ROLE".equals(log.getMessageType()) || "DEL_ROLE".equals(log.getMessageType()))
                                    .collectList()
                    ).map(tuple -> {
                        long messageCount = tuple.getT1();
                        List<VirtualGroupSyncLog> syncLogs = tuple.getT2();
                        List<KafkaMessageLog> roleMessages = tuple.getT3();
                        
                        int syncLogCount = syncLogs.size();
                        
                        // 统计角色和用户变更数量
                        int addRoleCount = 0;
                        int delRoleCount = 0;
                        int addUserCount = 0;
                        int delUserCount = 0;
                        
                        for (VirtualGroupSyncLog syncLog : syncLogs) {
                            if (syncLog.getAddUserList() != null) {
                                addUserCount += syncLog.getAddUserList().size();
                            }
                            if (syncLog.getDelUserList() != null) {
                                delUserCount += syncLog.getDelUserList().size();
                            }
                        }
                        
                        // 从消息中统计角色变更
                        for (KafkaMessageLog message : roleMessages) {
                            if ("ADD_ROLE".equals(message.getMessageType())) {
                                addRoleCount++;
                            } else if ("DEL_ROLE".equals(message.getMessageType())) {
                                delRoleCount++;
                            }
                        }
                        
                        // 计算成功率
                        double successRate = messageCount > 0 ? Math.round(syncLogCount * 100.0 / messageCount * 100.0) / 100.0 : 0.0;
                        
                        Map<String, Object> dayStats = new HashMap<>();
                        dayStats.put("date", currentDate.toLocalDate().toString());
                        dayStats.put("message_count", (int) messageCount);
                        dayStats.put("sync_log_count", syncLogCount);
                        dayStats.put("add_role_count", addRoleCount);
                        dayStats.put("del_role_count", delRoleCount);
                        dayStats.put("add_user_count", addUserCount);
                        dayStats.put("del_user_count", delUserCount);
                        dayStats.put("total_role_change", addRoleCount + delRoleCount);
                        dayStats.put("total_user_change", addUserCount + delUserCount);
                        dayStats.put("success_rate", successRate);
                        
                        return dayStats;
                    });
                });
    }
    
    /**
     * 计算角色用户汇总统计
     */
    private Map<String, Object> calculateRoleUserSummary(List<Map<String, Object>> dailyStats) {
        Map<String, Object> summary = new HashMap<>();
        
        int totalMessages = 0;
        int totalSyncLogs = 0;
        int totalAddRoles = 0;
        int totalDelRoles = 0;
        int totalAddUsers = 0;
        int totalDelUsers = 0;
        
        for (Map<String, Object> dayStats : dailyStats) {
            totalMessages += (Integer) dayStats.get("message_count");
            totalSyncLogs += (Integer) dayStats.get("sync_log_count");
            totalAddRoles += (Integer) dayStats.get("add_role_count");
            totalDelRoles += (Integer) dayStats.get("del_role_count");
            totalAddUsers += (Integer) dayStats.get("add_user_count");
            totalDelUsers += (Integer) dayStats.get("del_user_count");
        }
        
        double avgSuccessRate = totalMessages > 0 ? 
                Math.round(totalSyncLogs * 100.0 / totalMessages * 100.0) / 100.0 : 0.0;
        
        summary.put("total_messages", totalMessages);
        summary.put("total_sync_logs", totalSyncLogs);
        summary.put("total_add_roles", totalAddRoles);
        summary.put("total_del_roles", totalDelRoles);
        summary.put("total_add_users", totalAddUsers);
        summary.put("total_del_users", totalDelUsers);
        summary.put("total_role_changes", totalAddRoles + totalDelRoles);
        summary.put("total_user_changes", totalAddUsers + totalDelUsers);
        summary.put("average_success_rate", avgSuccessRate);
        
        return summary;
    }
    
    /**
     * 优化的文件删除按系统统计方法
     */
    private Flux<Object> calculateFileDeletionSystemStatsOptimized(String systemNameFilter, 
                                                                  LocalDateTime startDateTime, 
                                                                  LocalDateTime endDateTime) {
        return kafkaMessageLogRepository.findDistinctSystemNamesByMessageTypeAndCreatedAtBetween("FILE_DEL", startDateTime, endDateTime)
                .filter(systemName -> systemNameFilter == null || systemNameFilter.equals(systemName))
                .flatMap(systemName -> {
                    return kafkaMessageLogRepository.countFileDeletionLogs(systemName, startDateTime, endDateTime)
                            .flatMap(totalCount -> {
                                // 分批处理成功数量统计，避免内存溢出
                                return calculateSystemDeletionSuccessCount(systemName, startDateTime, endDateTime)
                                        .map(successCount -> {
                                            double successRate = totalCount > 0 ? 
                                                    Math.round(successCount * 100.0 / totalCount * 100.0) / 100.0 : 0.0;
                                            
                                            Map<String, Object> stats = new HashMap<>();
                                            stats.put("system_name", systemName);
                                            stats.put("total_count", totalCount);
                                            stats.put("success_count", successCount);
                                            stats.put("success_rate", successRate);
                                            
                                            return (Object) stats;
                                        });
                            });
                });
    }
    
    /**
     * 计算系统删除成功数量
     */
    private Mono<Long> calculateSystemDeletionSuccessCount(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime) {
        int batchSize = 1000;
        return kafkaMessageLogRepository.countFileDeletionLogs(systemName, startDateTime, endDateTime)
                .flatMap(totalCount -> {
                    int totalPages = (int) Math.ceil((double) totalCount / batchSize);
                    
                    return Flux.range(0, totalPages)
                            .flatMap(page -> {
                                org.springframework.data.domain.PageRequest pageRequest = 
                                        org.springframework.data.domain.PageRequest.of(page, batchSize);
                                        
                                return kafkaMessageLogRepository.findFileDeletionLogs(systemName, startDateTime, endDateTime, pageRequest)
                                        .flatMap(kafkaLog -> {
                                            try {
                                                Map<String, Object> messageContent = parseKafkaJson(kafkaLog.getMessageContent());
                                                @SuppressWarnings("unchecked")
                                                Map<String, Object> fileMetadata = (Map<String, Object>) messageContent.get("fileMetadata");
                                                
                                                if (fileMetadata != null) {
                                                    String fileNumber = (String) fileMetadata.get("fileNumber");
                                                    if (fileNumber != null && !fileNumber.isEmpty()) {
                                                        return documentStatusRepository.findByFileNumber(fileNumber)
                                                                .map(status -> status.getStatus() == 1 ? 1L : 0L)
                                                                .defaultIfEmpty(0L);
                                                    }
                                                }
                                            } catch (Exception e) {
                                                log.error("解析删除消息失败: {}", e.getMessage());
                                            }
                                            return Mono.just(0L);
                                        });
                            })
                            .reduce(0L, Long::sum);
                });
    }
    
    /**
     * 计算单个系统的每日统计数据
     */
    private Mono<SystemDailyStats> calculateSystemDailyStats(String systemName, 
                                                      LocalDateTime startDateTime, 
                                                      LocalDateTime endDateTime,
                                                      Map<String, Map<String, Long>> kafkaStatsMap,
                                                      Map<String, Map<String, Long>> syncStatsMap,
                                                      Map<String, Map<String, Long>> virtualGroupStatsMap) {
        
        Map<String, Long> systemKafkaStats = kafkaStatsMap.getOrDefault(systemName, new HashMap<>());
        Map<String, Long> systemSyncStats = syncStatsMap.getOrDefault(systemName, new HashMap<>());
        Map<String, Long> systemVirtualGroupStats = virtualGroupStatsMap.getOrDefault(systemName, new HashMap<>());
        
        return Mono.zip(
                getSystemIncrementalStats(systemName, startDateTime, endDateTime),
                parseFilePermissionUsers(systemName, startDateTime, endDateTime),
                parseRoleUserChangeUsers(systemName, startDateTime, endDateTime),
                parseFileUpdateStats(systemName, startDateTime, endDateTime),
                parseVirtualGroupUserStats(systemName, startDateTime, endDateTime)
        ).map(tuple -> {
            Map<String, Integer> incrementalStats = tuple.getT1();
            FilePermissionUserStats filePermissionUserStats = tuple.getT2();
            RoleUserChangeStats roleUserChangeStats = tuple.getT3();
            FileUpdateStats fileUpdateWithPermissions = tuple.getT4();
            VirtualGroupUserStats virtualGroupUserStats = tuple.getT5();
            
            // 构建Kafka消息统计
            int fileAddMessages = systemKafkaStats.getOrDefault("FILE_ADD", 0L).intValue();
            int fileDelMessages = systemKafkaStats.getOrDefault("FILE_DEL", 0L).intValue();
            int fileNotChangeMessages = systemKafkaStats.getOrDefault("FILE_NOT_CHANGE", 0L).intValue();
            int addRoleMessages = systemKafkaStats.getOrDefault("ADD_ROLE", 0L).intValue();
            int delRoleMessages = systemKafkaStats.getOrDefault("DEL_ROLE", 0L).intValue();
            int roleAddUserMessages = systemKafkaStats.getOrDefault("ROLE_ADD_USER", 0L).intValue();
            int roleDelUserMessages = systemKafkaStats.getOrDefault("ROLE_DEL_USER", 0L).intValue();
            
            KafkaMessageStats kafkaMessageStats = KafkaMessageStats.builder()
                    .fileAddMessages(fileAddMessages)
                    .fileDeleteMessages(fileDelMessages)
                    .fileUpdateMessages(fileNotChangeMessages)
                    .roleGroupCreateMessages(addRoleMessages)
                    .roleGroupDeleteMessages(delRoleMessages)
                    .roleAddUserMessages(roleAddUserMessages)
                    .roleDeleteUserMessages(roleDelUserMessages)
                    .fileUpdate(filePermissionUserStats)
                    .roleUserChange(roleUserChangeStats)
                    .build();
            
            // 构建文件操作统计
            int fileAdd = incrementalStats.get("add_count") + incrementalStats.get("update_count");
            
            FileOpsStats fileOpsStats = FileOpsStats.builder()
                    .add(fileAdd)
                    .delete(fileDelMessages)
                    .build();
            
            // 构建文件更新统计
            int fileUpdateRecords = systemSyncStats.getOrDefault("FILE_NOT_CHANGE", 0L).intValue();
            
            FileUpdateStats fileUpdateStats = FileUpdateStats.builder()
                    .fileUpdateRecords(fileUpdateRecords)
                    .roleAdd(fileUpdateWithPermissions.getRoleAdd())
                    .roleDelete(fileUpdateWithPermissions.getRoleDelete())
                    .userAdd(fileUpdateWithPermissions.getUserAdd())
                    .userDelete(fileUpdateWithPermissions.getUserDelete())
                    .build();
            
            // 构建虚拟组操作统计
            int roleGroupCreate = systemVirtualGroupStats.getOrDefault("ADD_ROLE", 0L).intValue();
            int roleGroupDelete = systemVirtualGroupStats.getOrDefault("DEL_ROLE", 0L).intValue();
            int roleAddRecords = systemVirtualGroupStats.getOrDefault("ROLE_ADD_USER", 0L).intValue();
            int roleDelRecords = systemVirtualGroupStats.getOrDefault("ROLE_DEL_USER", 0L).intValue();
            
            VirtualGroupOpsStats virtualGroupOpsStats = VirtualGroupOpsStats.builder()
                    .roleGroupCreate(roleGroupCreate)
                    .roleGroupCreateUsers(virtualGroupUserStats.getRoleGroupCreateUsers())
                    .roleGroupDelete(roleGroupDelete)
                    .roleAddRecords(roleAddRecords)
                    .roleAddUsers(virtualGroupUserStats.getRoleAddUsers())
                    .roleDeleteRecords(roleDelRecords)
                    .roleDeleteUsers(virtualGroupUserStats.getRoleDeleteUsers())
                    .build();
            
            return SystemDailyStats.builder()
                    .systemName(systemName)
                    .fileOps(fileOpsStats)
                    .fileUpdate(fileUpdateStats)
                    .virtualGroupOps(virtualGroupOpsStats)
                    .kafkaMessageStats(kafkaMessageStats)
                    .build();
        });
    }
    
    /**
     * 解析FILE_NOT_CHANGE消息中的用户列表
     */
    private Mono<FilePermissionUserStats> parseFilePermissionUsers(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return kafkaMessageLogRepository.findBySystemAndMessageType(systemName, "FILE_NOT_CHANGE", startDateTime, endDateTime)
                .map(msg -> {
                    try {
                        Map<String, Object> messageContent = parseKafkaJson(msg.getMessageContent());
                        
                        @SuppressWarnings("unchecked")
                        List<Object> addUsers = (List<Object>) messageContent.get("fileAddUserList");
                        @SuppressWarnings("unchecked")
                        List<Object> delUsers = (List<Object>) messageContent.get("fileDelUserList");
                        
                        int addUserCount = addUsers != null ? addUsers.size() : 0;
                        int delUserCount = delUsers != null ? delUsers.size() : 0;
                        
                        return new int[]{addUserCount, delUserCount};
                    } catch (Exception e) {
                        log.error("解析FILE_NOT_CHANGE消息内容失败: {}, 消息ID: {}", e.getMessage(), msg.getId());
                        return new int[]{0, 0};
                    }
                })
                .reduce(new int[]{0, 0}, (acc, curr) -> new int[]{acc[0] + curr[0], acc[1] + curr[1]})
                .map(result -> FilePermissionUserStats.builder()
                        .addUsers(result[0])
                        .deleteUsers(result[1])
                        .build());
    }
    
    /**
     * 解析ROLE_ADD_USER和ROLE_DEL_USER消息中的用户列表
     */
    private Mono<RoleUserChangeStats> parseRoleUserChangeUsers(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return Mono.zip(
                kafkaMessageLogRepository.findBySystemAndMessageType(systemName, "ROLE_ADD_USER", startDateTime, endDateTime)
                        .map(msg -> {
                            try {
                                Map<String, Object> messageContent = parseKafkaJson(msg.getMessageContent());
                                List<Object> userList = findUserListInMessage(messageContent, 
                                        new String[]{"addUserList", "user_list", "UserList", "users"});
                                return userList != null ? userList.size() : 0;
                            } catch (Exception e) {
                                log.error("解析ROLE_ADD_USER消息内容失败: {}, 消息ID: {}", e.getMessage(), msg.getId());
                                return 0;
                            }
                        })
                        .reduce(0, Integer::sum),
                kafkaMessageLogRepository.findBySystemAndMessageType(systemName, "ROLE_DEL_USER", startDateTime, endDateTime)
                        .map(msg -> {
                            try {
                                Map<String, Object> messageContent = parseKafkaJson(msg.getMessageContent());
                                List<Object> userList = findUserListInMessage(messageContent, 
                                        new String[]{"delUserList", "user_list", "UserList", "users"});
                                return userList != null ? userList.size() : 0;
                            } catch (Exception e) {
                                log.error("解析ROLE_DEL_USER消息内容失败: {}, 消息ID: {}", e.getMessage(), msg.getId());
                                return 0;
                            }
                        })
                        .reduce(0, Integer::sum)
        ).map(tuple -> RoleUserChangeStats.builder()
                .addUsers(tuple.getT1())
                .deleteUsers(tuple.getT2())
                .build());
    }
    
    /**
     * 解析文件更新统计（角色和用户变更）
     */
    private Mono<FileUpdateStats> parseFileUpdateStats(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return knowledgeRoleSyncLogRepository.findSyncLogsBySystemAndMessageType(systemName, "FILE_NOT_CHANGE", startDateTime, endDateTime)
                .map(syncLog -> {
                    try {
                        int addRoleCount = syncLog.getAddRoleList() != null ? syncLog.getAddRoleList().size() : 0;
                        int delRoleCount = syncLog.getDelRoleList() != null ? syncLog.getDelRoleList().size() : 0;
                        int addUserCount = syncLog.getAddUserList() != null ? syncLog.getAddUserList().size() : 0;
                        int delUserCount = syncLog.getDelUserList() != null ? syncLog.getDelUserList().size() : 0;
                        
                        return new int[]{addRoleCount, delRoleCount, addUserCount, delUserCount};
                    } catch (Exception e) {
                        log.error("解析FILE_NOT_CHANGE同步日志失败: {}, 日志ID: {}", e.getMessage(), syncLog.getId());
                        return new int[]{0, 0, 0, 0};
                    }
                })
                .reduce(new int[]{0, 0, 0, 0}, (acc, curr) -> new int[]{
                        acc[0] + curr[0], acc[1] + curr[1], acc[2] + curr[2], acc[3] + curr[3]
                })
                .map(result -> FileUpdateStats.builder()
                        .fileUpdateRecords(0) // 这里会在调用方设置
                        .roleAdd(result[0])
                        .roleDelete(result[1])
                        .userAdd(result[2])
                        .userDelete(result[3])
                        .build());
    }
    
    /**
     * 虚拟组用户统计内部类
     */
    private static class VirtualGroupUserStats {
        private int roleGroupCreateUsers;
        private int roleAddUsers;
        private int roleDeleteUsers;
        
        public VirtualGroupUserStats(int roleGroupCreateUsers, int roleAddUsers, int roleDeleteUsers) {
            this.roleGroupCreateUsers = roleGroupCreateUsers;
            this.roleAddUsers = roleAddUsers;
            this.roleDeleteUsers = roleDeleteUsers;
        }
        
        public int getRoleGroupCreateUsers() { return roleGroupCreateUsers; }
        public int getRoleAddUsers() { return roleAddUsers; }
        public int getRoleDeleteUsers() { return roleDeleteUsers; }
    }
    
    /**
     * 解析虚拟组用户统计
     */
    private Mono<VirtualGroupUserStats> parseVirtualGroupUserStats(String systemName, LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return Mono.zip(
                virtualGroupSyncLogRepository.findVirtualGroupSyncLogsBySystemAndMessageType(systemName, "ADD_ROLE", startDateTime, endDateTime)
                        .map(vgLog -> {
                            try {
                                if (vgLog.getAddUserList() != null) {
                                    return vgLog.getAddUserList().size();
                                }
                            } catch (Exception e) {
                                log.error("解析ADD_ROLE虚拟组同步日志失败: {}, 日志ID: {}", e.getMessage(), vgLog.getId());
                            }
                            return 0;
                        })
                        .reduce(0, Integer::sum),
                virtualGroupSyncLogRepository.findVirtualGroupSyncLogsBySystemAndMessageType(systemName, "ROLE_ADD_USER", startDateTime, endDateTime)
                        .map(vgLog -> {
                            try {
                                if (vgLog.getAddUserList() != null) {
                                    return vgLog.getAddUserList().size();
                                }
                            } catch (Exception e) {
                                log.error("解析ROLE_ADD_USER虚拟组同步日志失败: {}, 日志ID: {}", e.getMessage(), vgLog.getId());
                            }
                            return 0;
                        })
                        .reduce(0, Integer::sum),
                virtualGroupSyncLogRepository.findVirtualGroupSyncLogsBySystemAndMessageType(systemName, "ROLE_DEL_USER", startDateTime, endDateTime)
                        .map(vgLog -> {
                            try {
                                if (vgLog.getDelUserList() != null) {
                                    return vgLog.getDelUserList().size();
                                }
                            } catch (Exception e) {
                                log.error("解析ROLE_DEL_USER虚拟组同步日志失败: {}, 日志ID: {}", e.getMessage(), vgLog.getId());
                            }
                            return 0;
                        })
                        .reduce(0, Integer::sum)
        ).map(tuple -> new VirtualGroupUserStats(tuple.getT1(), tuple.getT2(), tuple.getT3()));
    }
    
    /**
     * 在消息内容中查找用户列表
     */
    @SuppressWarnings("unchecked")
    private List<Object> findUserListInMessage(Map<String, Object> messageContent, String[] fieldNames) {
        for (String fieldName : fieldNames) {
            if (messageContent.containsKey(fieldName) && messageContent.get(fieldName) != null) {
                Object userListObj = messageContent.get(fieldName);
                if (userListObj instanceof List) {
                    return (List<Object>) userListObj;
                } else if (userListObj instanceof String) {
                    try {
                        Object parsed = parseKafkaJson((String) userListObj);
                        if (parsed instanceof List) {
                            return (List<Object>) parsed;
                        }
                    } catch (Exception e) {
                        log.error("解析用户列表字符串失败: {}", e.getMessage());
                    }
                }
            }
        }
        return null;
    }
    
    /**
     * 计算总计统计
     */
    private SystemDailyStats calculateTotalStats(List<SystemDailyStats> systemStatsList) {
        // 初始化累加器变量
        int totalFileAdd = 0, totalFileDelete = 0;
        int totalFileUpdateRecords = 0, totalFileRoleAdd = 0, totalFileRoleDelete = 0, totalFileUserAdd = 0, totalFileUserDelete = 0;
        int totalVirtualRoleGroupCreate = 0, totalVirtualRoleGroupCreateUsers = 0, totalVirtualRoleGroupDelete = 0;
        int totalVirtualRoleAddRecords = 0, totalVirtualRoleAddUsers = 0, totalVirtualRoleDeleteRecords = 0, totalVirtualRoleDeleteUsers = 0;
        int totalKafkaFileAdd = 0, totalKafkaFileDelete = 0, totalKafkaFileUpdate = 0;
        int totalKafkaRoleGroupCreate = 0, totalKafkaRoleGroupDelete = 0, totalKafkaRoleAddUser = 0, totalKafkaRoleDeleteUser = 0;
        int totalFilePermissionAdd = 0, totalFilePermissionDelete = 0;
        int totalRoleUserChangeAdd = 0, totalRoleUserChangeDelete = 0;
        
        for (SystemDailyStats systemStats : systemStatsList) {
            // 累加到总计
            totalFileAdd += systemStats.getFileOps().getAdd();
            totalFileDelete += systemStats.getFileOps().getDelete();
            
            totalFileUpdateRecords += systemStats.getFileUpdate().getFileUpdateRecords();
            totalFileRoleAdd += systemStats.getFileUpdate().getRoleAdd();
            totalFileRoleDelete += systemStats.getFileUpdate().getRoleDelete();
            totalFileUserAdd += systemStats.getFileUpdate().getUserAdd();
            totalFileUserDelete += systemStats.getFileUpdate().getUserDelete();
            
            totalVirtualRoleGroupCreate += systemStats.getVirtualGroupOps().getRoleGroupCreate();
            totalVirtualRoleGroupCreateUsers += systemStats.getVirtualGroupOps().getRoleGroupCreateUsers();
            totalVirtualRoleGroupDelete += systemStats.getVirtualGroupOps().getRoleGroupDelete();
            totalVirtualRoleAddRecords += systemStats.getVirtualGroupOps().getRoleAddRecords();
            totalVirtualRoleAddUsers += systemStats.getVirtualGroupOps().getRoleAddUsers();
            totalVirtualRoleDeleteRecords += systemStats.getVirtualGroupOps().getRoleDeleteRecords();
            totalVirtualRoleDeleteUsers += systemStats.getVirtualGroupOps().getRoleDeleteUsers();
            
            totalKafkaFileAdd += systemStats.getKafkaMessageStats().getFileAddMessages();
            totalKafkaFileDelete += systemStats.getKafkaMessageStats().getFileDeleteMessages();
            totalKafkaFileUpdate += systemStats.getKafkaMessageStats().getFileUpdateMessages();
            totalKafkaRoleGroupCreate += systemStats.getKafkaMessageStats().getRoleGroupCreateMessages();
            totalKafkaRoleGroupDelete += systemStats.getKafkaMessageStats().getRoleGroupDeleteMessages();
            totalKafkaRoleAddUser += systemStats.getKafkaMessageStats().getRoleAddUserMessages();
            totalKafkaRoleDeleteUser += systemStats.getKafkaMessageStats().getRoleDeleteUserMessages();
            
            if (systemStats.getKafkaMessageStats().getFileUpdate() != null) {
                totalFilePermissionAdd += systemStats.getKafkaMessageStats().getFileUpdate().getAddUsers();
                totalFilePermissionDelete += systemStats.getKafkaMessageStats().getFileUpdate().getDeleteUsers();
            }
            
            if (systemStats.getKafkaMessageStats().getRoleUserChange() != null) {
                totalRoleUserChangeAdd += systemStats.getKafkaMessageStats().getRoleUserChange().getAddUsers();
                totalRoleUserChangeDelete += systemStats.getKafkaMessageStats().getRoleUserChange().getDeleteUsers();
            }
        }
        
        return SystemDailyStats.builder()
                .systemName("总计")
                .fileOps(FileOpsStats.builder()
                        .add(totalFileAdd)
                        .delete(totalFileDelete)
                        .build())
                .fileUpdate(FileUpdateStats.builder()
                        .fileUpdateRecords(totalFileUpdateRecords)
                        .roleAdd(totalFileRoleAdd)
                        .roleDelete(totalFileRoleDelete)
                        .userAdd(totalFileUserAdd)
                        .userDelete(totalFileUserDelete)
                        .build())
                .virtualGroupOps(VirtualGroupOpsStats.builder()
                        .roleGroupCreate(totalVirtualRoleGroupCreate)
                        .roleGroupCreateUsers(totalVirtualRoleGroupCreateUsers)
                        .roleGroupDelete(totalVirtualRoleGroupDelete)
                        .roleAddRecords(totalVirtualRoleAddRecords)
                        .roleAddUsers(totalVirtualRoleAddUsers)
                        .roleDeleteRecords(totalVirtualRoleDeleteRecords)
                        .roleDeleteUsers(totalVirtualRoleDeleteUsers)
                        .build())
                .kafkaMessageStats(KafkaMessageStats.builder()
                        .fileAddMessages(totalKafkaFileAdd)
                        .fileDeleteMessages(totalKafkaFileDelete)
                        .fileUpdateMessages(totalKafkaFileUpdate)
                        .roleGroupCreateMessages(totalKafkaRoleGroupCreate)
                        .roleGroupDeleteMessages(totalKafkaRoleGroupDelete)
                        .roleAddUserMessages(totalKafkaRoleAddUser)
                        .roleDeleteUserMessages(totalKafkaRoleDeleteUser)
                        .fileUpdate(FilePermissionUserStats.builder()
                                .addUsers(totalFilePermissionAdd)
                                .deleteUsers(totalFilePermissionDelete)
                                .build())
                        .roleUserChange(RoleUserChangeStats.builder()
                                .addUsers(totalRoleUserChangeAdd)
                                .deleteUsers(totalRoleUserChangeDelete)
                                .build())
                        .build())
                .build();
    }
}

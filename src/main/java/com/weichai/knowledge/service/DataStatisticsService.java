package com.weichai.knowledge.service;

import com.weichai.knowledge.dto.*;
import reactor.core.publisher.Mono;
import java.time.LocalDate;

/**
 * 数据统计服务接口
 * 提供数据接入和成功量的统计功能
 */
public interface DataStatisticsService {
    
    /**
     * 获取每日统计数据
     */
    Mono<DailyStatisticsResponse> getDailyStatistics(
            LocalDate startDate, LocalDate endDate);
    
    /**
     * 获取汇总统计数据
     */
    Mono<SummaryStatisticsResponse> getSummaryStatistics(
            Integer days, LocalDate startDate, LocalDate endDate);
    
    /**
     * 获取接入数据名称列表
     */
    Mono<DataNamesResponse> getIncomingDataNames(
            Integer limit, Integer offset, String systemName, 
            LocalDate startDate, LocalDate endDate);
    
    /**
     * 获取未成功接入的数据
     */
    Mono<FailedIngestionResponse> getFailedIngestionData(
            Integer limit, Integer offset, String systemName,
            LocalDate startDate, LocalDate endDate);
    
    /**
     * 获取文件详细状态
     */
    Mono<FileStatusResponse> getDetailedFileStatus(String fileNumber);
    
    /**
     * 通过文件名获取文件状态
     */
    Mono<FileStatusResponse> getFileStatusByName(String fileName);
    
    /**
     * 获取文件权限统计
     */
    Mono<FilePermissionStatisticsResponse> getFilePermissionStatistics(
            LocalDate startDate, LocalDate endDate);
    
    /**
     * 获取角色用户统计
     */
    Mono<RoleUserStatisticsResponse> getRoleUserStatistics(
            LocalDate startDate, LocalDate endDate);
    
    /**
     * 获取综合统计
     */
    Mono<ComprehensiveStatisticsResponse> getComprehensiveStatistics(
            LocalDate startDate, LocalDate endDate, Integer days, String systemName);
    
    /**
     * 获取文件删除统计
     */
    Mono<FileDeletionStatisticsResponse> getFileDeletionStatistics(
            Integer limit, Integer offset, String systemName,
            LocalDate startDate, LocalDate endDate);
    
    /**
     * 获取系统每日统计
     */
    Mono<SystemDailyStatisticsResponse> getSystemDailyStatistics();
} 
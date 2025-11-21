package com.weichai.knowledge.controller;

import com.weichai.knowledge.service.DataStatisticsService;
import com.weichai.knowledge.dto.*;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.LocalDate;

@RestController
@RequestMapping("/api/v1/statistics")
@RequiredArgsConstructor
public class DataStatisticsController {
    
    private final DataStatisticsService dataStatisticsService;
    
    @GetMapping("/daily")
    public Mono<ResponseEntity<DailyStatisticsResponse>> getDailyStatistics(
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        return dataStatisticsService.getDailyStatistics(startDate, endDate)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/summary")
    public Mono<ResponseEntity<SummaryStatisticsResponse>> getSummaryStatistics(
            @RequestParam(required = false, defaultValue = "30") Integer days,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        return dataStatisticsService.getSummaryStatistics(days, startDate, endDate)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/data-names")
    public Mono<ResponseEntity<DataNamesResponse>> getIncomingDataNames(
            @RequestParam(required = false, defaultValue = "100") Integer limit,
            @RequestParam(required = false, defaultValue = "0") Integer offset,
            @RequestParam(required = false) String systemName,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        return dataStatisticsService.getIncomingDataNames(
                limit, offset, systemName, startDate, endDate)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/failed-ingestion")
    public Mono<ResponseEntity<FailedIngestionResponse>> getFailedIngestionData(
            @RequestParam(required = false, defaultValue = "100") Integer limit,
            @RequestParam(required = false, defaultValue = "0") Integer offset,
            @RequestParam(required = false) String systemName,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        return dataStatisticsService.getFailedIngestionData(
                limit, offset, systemName, startDate, endDate)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/file-status/{fileNumber}")
    public Mono<ResponseEntity<FileStatusResponse>> getDetailedFileStatus(
            @PathVariable String fileNumber) {
        
        return dataStatisticsService.getDetailedFileStatus(fileNumber)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/file-status")
    public Mono<ResponseEntity<FileStatusResponse>> getFileStatusByName(
            @RequestParam String fileName) {
        
        return dataStatisticsService.getFileStatusByName(fileName)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/file-permission")
    public Mono<ResponseEntity<FilePermissionStatisticsResponse>> getFilePermissionStatistics(
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        return dataStatisticsService.getFilePermissionStatistics(startDate, endDate)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/role-user")
    public Mono<ResponseEntity<RoleUserStatisticsResponse>> getRoleUserStatistics(
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        return dataStatisticsService.getRoleUserStatistics(startDate, endDate)
                .map(ResponseEntity::ok);
    }
    
    // @GetMapping("/comprehensive")
    // public Mono<ResponseEntity<ComprehensiveStatisticsResponse>> getComprehensiveStatistics(
    //         @RequestParam(required = false) 
    //         @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
    //         @RequestParam(required = false) 
    //         @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate,
    //         @RequestParam(required = false, defaultValue = "30") Integer days,
    //         @RequestParam(required = false) String systemName) {
    //     
    //     return dataStatisticsService.getComprehensiveStatistics(
    //             startDate, endDate, days, systemName)
    //             .map(ResponseEntity::ok);
    // }
    
    @GetMapping("/file-deletion")
    public Mono<ResponseEntity<FileDeletionStatisticsResponse>> getFileDeletionStatistics(
            @RequestParam(required = false, defaultValue = "100") Integer limit,
            @RequestParam(required = false, defaultValue = "0") Integer offset,
            @RequestParam(required = false) String systemName,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam(required = false) 
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        return dataStatisticsService.getFileDeletionStatistics(
                limit, offset, systemName, startDate, endDate)
                .map(ResponseEntity::ok);
    }
    
    @GetMapping("/system-daily")
    public Mono<ResponseEntity<SystemDailyStatisticsResponse>> getSystemDailyStatistics() {
        return dataStatisticsService.getSystemDailyStatistics()
                .map(ResponseEntity::ok);
    }

    /**
     * 基于Redis优先的数据源获取当天各系统综合统计，
     * 无Redis数据时自动降级为数据库查询，结构与 /system-daily 保持一致。
     */
    @GetMapping("/redis/system-daily")
    public Mono<ResponseEntity<SystemDailyStatisticsResponse>> getRedisSystemDailyStatistics() {
        return dataStatisticsService.getSystemDailyStatisticsWithRedis()
                .map(ResponseEntity::ok);
    }
}

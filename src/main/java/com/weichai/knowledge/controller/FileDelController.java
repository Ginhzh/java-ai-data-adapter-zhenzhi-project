package com.weichai.knowledge.controller;

import com.weichai.knowledge.dto.FileDelRequest;
import com.weichai.knowledge.dto.FileDelResponse;
import com.weichai.knowledge.entity.FileDel;
import com.weichai.knowledge.service.FileDelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

/**
 * 文件删除服务API控制器
 * 提供文件删除的RESTful接口
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/file-deletion")
@Validated
public class FileDelController {
    
    @Autowired
    private FileDelService fileDelService;
    
    /**
     * 删除单个文件
     */
    @PostMapping(value = "/delete", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<FileDelResponse>> deleteFile(
            @Valid @RequestBody FileDelRequest request) {
        
        log.info("接收到文件删除请求: fileId={}, systemName={}", 
            request.getFileId(), request.getFileMetadata().getSystemName());
        
        // 转换DTO为实体类
        FileDel fileDel = convertToEntity(request);
        
        // 调用服务层处理
        return fileDelService.processFileDelMessage(fileDel)
            .map(this::convertToResponse)
            .map(ResponseEntity::ok)
            .onErrorResume(error -> {
                log.error("处理文件删除请求时发生错误", error);
                FileDelResponse errorResponse = FileDelResponse.simpleError("处理请求失败: " + error.getMessage());
                return Mono.just(ResponseEntity.status(500).body(errorResponse));
            });
    }
    
    /**
     * 批量删除文件
     */
    @PostMapping(value = "/batch-delete", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Map<String, Object>>> batchDeleteFiles(
            @Valid @RequestBody FileDelRequest[] requests) {
        
        log.info("接收到批量文件删除请求，文件数量: {}", requests.length);
        
        if (requests.length == 0) {
            return Mono.just(ResponseEntity.badRequest()
                .body(Map.of("status", "error", "message", "请求列表不能为空")));
        }
        
        if (requests.length > 100) {
            return Mono.just(ResponseEntity.badRequest()
                .body(Map.of("status", "error", "message", "单次批量删除文件数量不能超过100个")));
        }
        
        // 批量处理
        return Mono.fromCallable(() -> requests)
            .flatMapMany(reqArray -> reactor.core.publisher.Flux.fromArray(reqArray))
            .flatMap(request -> {
                FileDel fileDel = convertToEntity(request);
                return fileDelService.processFileDelMessage(fileDel)
                    .map(result -> Map.of("fileId", request.getFileId(), "result", result))
                    .onErrorReturn(Map.of("fileId", request.getFileId(), 
                        "result", Map.of("status", "error", "message", "处理失败")));
            })
            .collectList()
            .map(results -> {
                long successCount = results.stream()
                    .mapToLong(result -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> resultMap = (Map<String, Object>) result.get("result");
                        return "success".equals(resultMap.get("status")) ? 1 : 0;
                    })
                    .sum();
                
                return Map.of(
                    "status", "completed",
                    "total", requests.length,
                    "success", successCount,
                    "failed", requests.length - successCount,
                    "results", results
                );
            })
            .map(ResponseEntity::ok)
            .onErrorResume(error -> {
                log.error("批量删除文件时发生错误", error);
                return Mono.just(ResponseEntity.status(500)
                    .body(Map.of("status", "error", "message", "批量处理失败: " + error.getMessage())));
            });
    }
    
    /**
     * 查询文件删除状态
     */
    @GetMapping("/status")
    public Mono<ResponseEntity<Map<String, Object>>> getFileDeleteStatus(
            @RequestParam @NotBlank(message = "文件ID不能为空") String fileId,
            @RequestParam @NotBlank(message = "系统名称不能为空") String systemName) {
        
        log.info("查询文件删除状态: fileId={}, systemName={}", fileId, systemName);
        
        return Mono.fromCallable(() -> {
                Map<String, Object> response = Map.of(
                    "fileId", fileId,
                    "systemName", systemName,
                    "status", "unknown",
                    "message", "状态查询功能待实现",
                    "timestamp", java.time.LocalDateTime.now().toString()
                );
                return response;
            })
            .map(ResponseEntity::ok)
            .onErrorResume(error -> {
                log.error("查询文件删除状态时发生错误", error);
                return Mono.just(ResponseEntity.status(500)
                    .body(Map.of("status", "error", "message", "查询失败: " + error.getMessage())));
            });
    }
    
    /**
     * 健康检查接口
     */
    @GetMapping("/health")
    public Mono<ResponseEntity<Map<String, Object>>> healthCheck() {
        return Mono.fromCallable(() -> {
                Map<String, Object> response = Map.of(
                    "status", "healthy",
                    "service", "FileDelService",
                    "timestamp", java.time.LocalDateTime.now().toString(),
                    "version", "1.0.0"
                );
                return response;
            })
            .map(ResponseEntity::ok);
    }
    
    /**
     * 转换DTO为实体类
     */
    private FileDel convertToEntity(FileDelRequest request) {
        FileDel fileDel = new FileDel();
        fileDel.setFileId(request.getFileId());
        
        FileDel.FileMetadata metadata = new FileDel.FileMetadata();
        metadata.setSystemName(request.getFileMetadata().getSystemName());
        metadata.setFileNumber(request.getFileMetadata().getFileNumber());
        metadata.setFileName(request.getFileMetadata().getFileName());
        metadata.setFilePath(request.getFileMetadata().getFilePath());
        metadata.setVersion(request.getFileMetadata().getVersion());
        
        fileDel.setFileMetadata(metadata);
        return fileDel;
    }
    
    /**
     * 转换服务结果为响应DTO
     */
    private FileDelResponse convertToResponse(Map<String, Object> result) {
        String status = (String) result.get("status");
        
        if ("success".equals(status)) {
            return FileDelResponse.success(
                (String) result.get("system_name"),
                (String) result.get("file_id"),
                (String) result.get("file_number"),
                (String) result.get("file_name"),
                (String) result.get("file_zhenzhi_id"),
                (String) result.get("version")
            );
        } else {
            return FileDelResponse.error(
                (String) result.get("system_name"),
                (String) result.get("file_id"),
                (String) result.get("file_number"),
                (String) result.get("file_name"),
                (String) result.get("message")
            );
        }
    }
}
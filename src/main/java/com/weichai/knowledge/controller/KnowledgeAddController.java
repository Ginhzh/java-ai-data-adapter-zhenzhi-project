package com.weichai.knowledge.controller;

import com.weichai.knowledge.dto.FileAddRequest;
import com.weichai.knowledge.dto.ApiResponse;
import com.weichai.knowledge.exception.BusinessException;
import com.weichai.knowledge.service.ReactiveKnowledgeAddService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.Map;
import java.util.List;
import reactor.core.publisher.Mono;

/**
 * 知识库添加控制器
 */
@Slf4j
@RestController
@RequestMapping("/api/knowledge")
public class KnowledgeAddController {
    
    @Autowired
    private ReactiveKnowledgeAddService knowledgeAddService;
    
    /**
     * 处理文件添加请求
     * 
     * @param request 文件添加请求
     * @return 处理结果
     */
    @PostMapping("/add-file")
    public Mono<ResponseEntity<ApiResponse<Map<String, Object>>>> addFile(
            @Valid @RequestBody FileAddRequest request) {
        
        log.info("收到文件添加请求: fileId={}, systemName={}, messageType={}", 
            request.getFileMetadata().getFileId(), 
            request.getFileMetadata().getSystemName(),
            request.getMessageType());
        
        log.debug("完整请求数据: {}", request);
        
        return knowledgeAddService.processFileAddMessage(request.toMap())
            .map(result -> {
                String status = (String) result.get("status");
                if ("success".equals(status)) {
                    log.info("文件添加处理成功: fileId={}", request.getFileMetadata().getFileId());
                    ApiResponse<Map<String, Object>> response = new ApiResponse<>(200, "处理成功", result, null, null, null, null, result, true);
                    return ResponseEntity.ok(response);
                } else {
                    String message = (String) result.get("message");
                    log.error("文件添加处理失败: fileId={}, error={}", 
                        request.getFileMetadata().getFileId(), message);
                    // Throw a business exception, which will be caught by the global exception handler.
                    throw new BusinessException(message, 400);
                }
            });
    }
    
    /**
     * 批量处理文件添加请求
     * 
     * @param requests 文件添加请求列表
     * @return 处理结果列表
     */
    @PostMapping("/add-files-batch")
    public Mono<ResponseEntity<ApiResponse<Map<String, Object>>>> addFilesBatch(
            @Valid @RequestBody List<FileAddRequest> requests) {
        
        log.info("收到批量文件添加请求，数量: {}", requests.size());
        
        // 使用reactive处理批量请求
        return reactor.core.publisher.Flux.fromIterable(requests)
            .flatMap(request -> knowledgeAddService.processFileAddMessage(request.toMap()))
            .collectList()
            .map(results -> {
                Map<String, Object> batchResult = new java.util.HashMap<>();
                batchResult.put("total", requests.size());
                batchResult.put("processed", results.size());
                batchResult.put("message", "批量处理完成");
                
                log.info("批量文件添加处理完成，总数: {}", requests.size());
                ApiResponse<Map<String, Object>> response = new ApiResponse<>(200, "批量处理完成", batchResult, null, null, null, null, batchResult, true);
                return ResponseEntity.ok(response);
            });
    }
    
    /**
     * 健康检查接口
     */
    @GetMapping("/health")
    public Mono<ResponseEntity<ApiResponse<String>>> health() {
        return Mono.fromCallable(() -> {
            ApiResponse<String> response = new ApiResponse<>(200, "Knowledge Add Service is running", "OK", null, null, null, null, "OK", true);
            return ResponseEntity.ok(response);
        });
    }
}
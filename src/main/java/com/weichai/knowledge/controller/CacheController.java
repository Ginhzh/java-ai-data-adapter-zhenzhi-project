package com.weichai.knowledge.controller;

import com.weichai.knowledge.service.KnowledgeStatusCheckService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.Map;

/**
 * 缓存管理控制器
 * 用于管理Redis缓存的清理操作
 */
@Slf4j
@RestController
@RequestMapping("/api/cache")
public class


CacheController {

    @Autowired
    private KnowledgeStatusCheckService knowledgeStatusCheckService;

    /**
     * 清理指定文档的检查缓存
     * @param zhenzhiFileId 文档ID
     * @return 清理结果
     */
    @DeleteMapping("/document-check/{zhenzhiFileId}")
    public Mono<ResponseEntity<Map<String, Object>>> clearDocumentCheckCache(@PathVariable String zhenzhiFileId) {
        return Mono.fromCallable(() -> {
            log.info("收到清理文档检查缓存请求，文档ID: {}", zhenzhiFileId);
            
            Map<String, Object> response = new HashMap<>();
            
            try {
                boolean result = knowledgeStatusCheckService.clearDocumentCheckCache(zhenzhiFileId);
                
                response.put("success", result);
                response.put("message", result ? "清理成功" : "清理失败");
                response.put("zhenzhiFileId", zhenzhiFileId);
                
                return ResponseEntity.ok(response);
                
            } catch (Exception e) {
                log.error("清理文档检查缓存失败", e);
                response.put("success", false);
                response.put("message", "清理失败: " + e.getMessage());
                response.put("zhenzhiFileId", zhenzhiFileId);
                
                return ResponseEntity.status(500).body(response);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 批量清理文档检查缓存
     * @param request 包含文档ID列表的请求体
     * @return 清理结果
     */
    @DeleteMapping("/document-check/batch")
    public Mono<ResponseEntity<Map<String, Object>>> clearBatchDocumentCheckCache(@RequestBody Map<String, Object> request) {
        return Mono.fromCallable(() -> {
            Map<String, Object> response = new HashMap<>();
            
            try {
                @SuppressWarnings("unchecked")
                java.util.List<String> zhenzhiFileIds = (java.util.List<String>) request.get("zhenzhiFileIds");
                
                if (zhenzhiFileIds == null || zhenzhiFileIds.isEmpty()) {
                    response.put("success", false);
                    response.put("message", "文档ID列表不能为空");
                    return ResponseEntity.badRequest().body(response);
                }
                
                log.info("收到批量清理文档检查缓存请求，文档数量: {}", zhenzhiFileIds.size());
                
                int successCount = 0;
                int failedCount = 0;
                
                for (String zhenzhiFileId : zhenzhiFileIds) {
                    try {
                        boolean result = knowledgeStatusCheckService.clearDocumentCheckCache(zhenzhiFileId);
                        if (result) {
                            successCount++;
                        } else {
                            failedCount++;
                        }
                    } catch (Exception e) {
                        log.error("清理文档 {} 的缓存失败", zhenzhiFileId, e);
                        failedCount++;
                    }
                }
                
                response.put("success", failedCount == 0);
                response.put("message", String.format("批量清理完成，成功: %d，失败: %d", successCount, failedCount));
                response.put("successCount", successCount);
                response.put("failedCount", failedCount);
                response.put("totalCount", zhenzhiFileIds.size());
                
                return ResponseEntity.ok(response);
                
            } catch (Exception e) {
                log.error("批量清理文档检查缓存失败", e);
                response.put("success", false);
                response.put("message", "批量清理失败: " + e.getMessage());
                
                return ResponseEntity.status(500).body(response);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 清理所有文档检查缓存
     * @return 清理结果
     */
    @DeleteMapping("/document-check/all")
    public Mono<ResponseEntity<Map<String, Object>>> clearAllDocumentCheckCache() {
        return Mono.fromCallable(() -> {
            log.info("收到清理所有文档检查缓存请求");
            
            Map<String, Object> response = new HashMap<>();
            
            try {
                int clearCount = knowledgeStatusCheckService.clearAllDocumentCheckCache();
                
                response.put("success", true);
                response.put("message", "清理所有文档检查缓存成功");
                response.put("clearCount", clearCount);
                
                return ResponseEntity.ok(response);
                
            } catch (Exception e) {
                log.error("清理所有文档检查缓存失败", e);
                response.put("success", false);
                response.put("message", "清理失败: " + e.getMessage());
                
                return ResponseEntity.status(500).body(response);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 获取文档检查缓存信息
     * @param zhenzhiFileId 文档ID
     * @return 缓存信息
     */
    @GetMapping("/document-check/{zhenzhiFileId}")
    public Mono<ResponseEntity<Map<String, Object>>> getDocumentCheckCacheInfo(@PathVariable String zhenzhiFileId) {
        return Mono.fromCallable(() -> {
            log.info("查询文档检查缓存信息，文档ID: {}", zhenzhiFileId);
            
            Map<String, Object> response = new HashMap<>();
            
            try {
                Map<String, Object> cacheInfo = knowledgeStatusCheckService.getDocumentCheckCacheInfo(zhenzhiFileId);
                
                response.put("success", true);
                response.put("zhenzhiFileId", zhenzhiFileId);
                response.put("cacheInfo", cacheInfo);
                
                return ResponseEntity.ok(response);
                
            } catch (Exception e) {
                log.error("查询文档检查缓存信息失败", e);
                response.put("success", false);
                response.put("message", "查询失败: " + e.getMessage());
                response.put("zhenzhiFileId", zhenzhiFileId);
                
                return ResponseEntity.status(500).body(response);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 清理Redis所有键 - 危险操作
     * @param request 包含确认信息的请求体
     * @return 清理结果
     */
    @DeleteMapping("/redis/all")
    public Mono<ResponseEntity<Map<String, Object>>> clearAllRedisKeys(@RequestBody Map<String, Object> request) {
        return Mono.fromCallable(() -> {
            Map<String, Object> response = new HashMap<>();
            
            try {
                // 安全确认机制
                String confirmation = (String) request.get("confirmation");
                if (!"I_CONFIRM_DELETE_ALL_REDIS_KEYS".equals(confirmation)) {
                    response.put("success", false);
                    response.put("message", "危险操作需要确认，请在请求体中设置 confirmation: 'I_CONFIRM_DELETE_ALL_REDIS_KEYS'");
                    return ResponseEntity.badRequest().body(response);
                }
                
                log.warn("收到清理Redis所有键的危险操作请求");
                
                long deletedCount = knowledgeStatusCheckService.clearAllRedisKeys();
                
                response.put("success", true);
                response.put("message", "清理Redis所有键成功");
                response.put("deletedCount", deletedCount);
                
                log.warn("Redis所有键已被清理，删除数量: {}", deletedCount);
                
                return ResponseEntity.ok(response);
                
            } catch (Exception e) {
                log.error("清理Redis所有键失败", e);
                response.put("success", false);
                response.put("message", "清理失败: " + e.getMessage());
                
                return ResponseEntity.status(500).body(response);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 按模式清理Redis键
     * @param request 包含模式信息的请求体
     * @return 清理结果
     */
    @DeleteMapping("/redis/pattern")
    public Mono<ResponseEntity<Map<String, Object>>> clearRedisByPattern(@RequestBody Map<String, Object> request) {
        return Mono.fromCallable(() -> {
            Map<String, Object> response = new HashMap<>();
            
            try {
                String pattern = (String) request.get("pattern");
                if (pattern == null || pattern.trim().isEmpty()) {
                    response.put("success", false);
                    response.put("message", "模式不能为空");
                    return ResponseEntity.badRequest().body(response);
                }
                
                log.info("收到按模式清理Redis键请求，模式: {}", pattern);
                
                long deletedCount = knowledgeStatusCheckService.clearRedisByPattern(pattern);
                
                response.put("success", true);
                response.put("message", "按模式清理Redis键成功");
                response.put("pattern", pattern);
                response.put("deletedCount", deletedCount);
                
                return ResponseEntity.ok(response);
                
            } catch (Exception e) {
                log.error("按模式清理Redis键失败", e);
                response.put("success", false);
                response.put("message", "清理失败: " + e.getMessage());
                
                return ResponseEntity.status(500).body(response);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 获取Redis键统计信息
     * @return 键统计信息
     */
    @GetMapping("/redis/stats")
    public Mono<ResponseEntity<Map<String, Object>>> getRedisStats() {
        return Mono.fromCallable(() -> {
            log.info("查询Redis键统计信息");
            
            Map<String, Object> response = new HashMap<>();
            
            try {
                Map<String, Object> stats = knowledgeStatusCheckService.getRedisStats();
                
                response.put("success", true);
                response.put("stats", stats);
                
                return ResponseEntity.ok(response);
                
            } catch (Exception e) {
                log.error("查询Redis键统计信息失败", e);
                response.put("success", false);
                response.put("message", "查询失败: " + e.getMessage());
                
                return ResponseEntity.status(500).body(response);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
} 
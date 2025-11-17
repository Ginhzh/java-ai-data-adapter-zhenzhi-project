package com.weichai.knowledge.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 主页控制器
 */
@RestController
public class MainController {
    
    /**
     * 根路径接口
     */
    @GetMapping("/")
    public Mono<Map<String, Object>> home() {
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();
            result.put("service", "知识库服务 Java版本");
            result.put("description", "基于Spring Boot的知识库管理服务");
            result.put("version", "1.0.0");
            result.put("timestamp", LocalDateTime.now());
            result.put("status", "运行中");
            
            Map<String, String> endpoints = new HashMap<>();
            endpoints.put("健康检查", "/api/health");
            endpoints.put("数据库状态", "/api/health/database");
            endpoints.put("配置信息", "/api/health/config");
            endpoints.put("监控端点", "/actuator/health");
            
            result.put("available_endpoints", endpoints);
            
            return result;
        });
    }
    
    /**
     * 简单的ping接口
     */
    @GetMapping("/ping")
    public Mono<Map<String, Object>> ping() {
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();
            result.put("message", "pong");
            result.put("timestamp", LocalDateTime.now());
            return result;
        });
    }
} 
package com.weichai.knowledge.controller;

import com.weichai.knowledge.config.ApplicationProperties;
import com.weichai.knowledge.service.ReactiveKnowledgeHandler;
import io.r2dbc.spi.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 健康检查控制器
 * 用于验证项目运行状态和外部依赖连接
 */
@Slf4j
@RestController
@RequestMapping("/api")
public class HealthController {

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private ReactiveKnowledgeHandler reactiveKnowledgeHandler;
    
    /**
     * 简单的健康检查接口
     */
    @GetMapping("/health")
    public Mono<Map<String, Object>> health() {
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();
            result.put("status", "UP");
            result.put("timestamp", LocalDateTime.now());
            result.put("service", "知识库服务");
            result.put("version", "1.0.0");
            return result;
        });
    }
    
    /**
     * 数据库连接检查
     */
    @GetMapping("/health/database")
    public Mono<Map<String, Object>> databaseHealth() {
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();
            result.put("timestamp", LocalDateTime.now());
            return result;
        })
        .flatMap(result -> {
            return Mono.from(connectionFactory.create())
                .flatMap(connection -> {
                    // 执行简单的查询来测试连接
                    return Mono.from(connection.createStatement("SELECT 1 as test").execute())
                        .flatMap(queryResult -> Mono.from(queryResult.map((row, metadata) -> row.get("test"))))
                        .doOnNext(testResult -> {
                            result.put("database_status", "CONNECTED");
                            result.put("test_query_result", testResult);
                        })
                        .doFinally(signalType -> connection.close())
                        .thenReturn(result);
                })
                .onErrorResume(error -> {
                    result.put("database_status", "DISCONNECTED");
                    result.put("error", error.getMessage());
                    return Mono.just(result);
                });
        });
    }
    
    /**
     * 配置信息检查
     */
    @GetMapping("/health/config")
    public Mono<Map<String, Object>> configHealth() {
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();

            ApplicationProperties.Database dbConfig = applicationProperties.getDatabase();
            ApplicationProperties.Redis redisConfig = applicationProperties.getRedis();
            ApplicationProperties.Kafka kafkaConfig = applicationProperties.getKafka();
            ApplicationProperties.Api apiConfig = applicationProperties.getApi();

            result.put("database_host", dbConfig.getHost());
            result.put("database_port", dbConfig.getPort());
            result.put("database_name", dbConfig.getDatabase());
            result.put("redis_host", redisConfig.getHost());
            result.put("redis_port", redisConfig.getPort());
            result.put("kafka_servers", kafkaConfig.getBootstrapServers());
            result.put("api_base_url", apiConfig.getBaseUrl());
            result.put("timestamp", LocalDateTime.now());

            return result;
        });
    }

    /**
     * 响应式服务健康检查
     */
    @GetMapping("/health/reactive")
    public Mono<ResponseEntity<Map<String, Object>>> reactiveServiceHealth() {
        return reactiveKnowledgeHandler.healthCheck()
            .map(health -> {
                String status = (String) health.get("status");
                if ("healthy".equals(status)) {
                    return ResponseEntity.ok(health);
                } else if ("degraded".equals(status)) {
                    return ResponseEntity.status(503).body(health); // Service Unavailable
                } else {
                    return ResponseEntity.status(500).body(health); // Internal Server Error
                }
            })
            .onErrorResume(error -> {
                log.error("响应式服务健康检查失败", error);
                Map<String, Object> errorHealth = new HashMap<>();
                errorHealth.put("status", "DOWN");
                errorHealth.put("service", "ReactiveKnowledgeHandler");
                errorHealth.put("error", error.getMessage());
                errorHealth.put("timestamp", LocalDateTime.now());
                return Mono.just(ResponseEntity.status(500).body(errorHealth));
            });
    }

    /**
     * 测试系统信息API连接
     */
    @GetMapping("/health/api-test")
    public Mono<ResponseEntity<Map<String, Object>>> testApiConnection() {
        return reactiveKnowledgeHandler.querySystemInfo("health_test")
            .map(result -> {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "success");
                response.put("message", "API连接测试成功");
                response.put("data", result);
                response.put("timestamp", LocalDateTime.now());
                return ResponseEntity.ok(response);
            })
            .onErrorResume(error -> {
                log.error("API连接测试失败", error);
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "API连接测试失败: " + error.getMessage());
                response.put("timestamp", LocalDateTime.now());
                return Mono.just(ResponseEntity.status(500).body(response));
            });
    }
}
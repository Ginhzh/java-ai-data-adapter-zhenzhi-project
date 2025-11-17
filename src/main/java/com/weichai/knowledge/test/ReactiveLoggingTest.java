package com.weichai.knowledge.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 响应式日志测试类
 * 用于验证LoggingAspect对响应式方法的处理
 */
@Slf4j
@Component
public class ReactiveLoggingTest {
    
    /**
     * 测试响应式方法的日志记录
     */
    public Mono<Map<String, Object>> testReactiveMethod(String testId) {
        log.info("开始测试响应式方法，测试ID: {}", testId);
        
        return Mono.fromCallable(() -> {
                log.info("步骤1: 准备测试数据");
                Map<String, Object> result = new HashMap<>();
                result.put("test_id", testId);
                result.put("step", "data_preparation");
                return result;
            })
            .delayElement(Duration.ofSeconds(2)) // 模拟耗时操作
            .map(result -> {
                log.info("步骤2: 处理测试数据");
                result.put("step", "data_processing");
                result.put("processed_at", System.currentTimeMillis());
                return result;
            })
            .delayElement(Duration.ofSeconds(1)) // 再次模拟耗时操作
            .map(result -> {
                log.info("步骤3: 完成测试处理");
                result.put("step", "completed");
                result.put("status", "success");
                result.put("completed_at", System.currentTimeMillis());
                return result;
            })
            .doOnSuccess(result -> {
                log.info("✅ 响应式测试方法执行成功，测试ID: {}", testId);
            })
            .doOnError(error -> {
                log.error("❌ 响应式测试方法执行失败，测试ID: {}, 错误: {}", testId, error.getMessage());
            });
    }
    
    /**
     * 测试同步方法的日志记录
     */
    public Map<String, Object> testSyncMethod(String testId) {
        log.info("开始测试同步方法，测试ID: {}", testId);
        
        try {
            Thread.sleep(1000); // 模拟耗时操作
            
            Map<String, Object> result = new HashMap<>();
            result.put("test_id", testId);
            result.put("method_type", "sync");
            result.put("status", "success");
            result.put("completed_at", System.currentTimeMillis());
            
            log.info("✅ 同步测试方法执行成功，测试ID: {}", testId);
            return result;
            
        } catch (InterruptedException e) {
            log.error("❌ 同步测试方法执行失败，测试ID: {}, 错误: {}", testId, e.getMessage());
            Thread.currentThread().interrupt();
            throw new RuntimeException("同步方法执行失败", e);
        }
    }
    
    /**
     * 测试会抛出异常的响应式方法
     */
    public Mono<Map<String, Object>> testReactiveMethodWithError(String testId) {
        log.info("开始测试会出错的响应式方法，测试ID: {}", testId);
        
        return Mono.fromCallable(() -> {
                log.info("步骤1: 准备会出错的测试数据");
                if ("error".equals(testId)) {
                    throw new RuntimeException("模拟的测试错误");
                }
                
                Map<String, Object> result = new HashMap<>();
                result.put("test_id", testId);
                result.put("step", "error_test");
                return result;
            })
            .delayElement(Duration.ofMillis(500))
            .map(result -> {
                log.info("步骤2: 这里不应该被执行到");
                result.put("should_not_reach", true);
                return result;
            });
    }
}

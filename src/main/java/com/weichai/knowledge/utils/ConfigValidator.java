package com.weichai.knowledge.utils;

import com.weichai.knowledge.config.ApplicationProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * 配置验证工具类
 * 用于验证应用配置的有效性和网络连接
 */
@Slf4j
@Component
public class ConfigValidator {
    
    /**
     * 验证应用配置
     */
    public List<String> validateConfiguration(ApplicationProperties properties) {
        List<String> errors = new ArrayList<>();
        
        // 验证数据库配置
        errors.addAll(validateDatabaseConfig(properties.getDatabase()));
        
        // 验证Redis配置
        errors.addAll(validateRedisConfig(properties.getRedis()));
        
        // 验证API配置
        errors.addAll(validateApiConfig(properties.getApi()));
        
        // 验证Kafka配置
        errors.addAll(validateKafkaConfig(properties.getKafka()));
        
        return errors;
    }
    
    /**
     * 验证数据库配置
     */
    private List<String> validateDatabaseConfig(ApplicationProperties.Database dbConfig) {
        List<String> errors = new ArrayList<>();
        
        if (dbConfig.getHost() == null || dbConfig.getHost().trim().isEmpty()) {
            errors.add("数据库主机地址不能为空");
        }
        
        if (dbConfig.getPort() <= 0 || dbConfig.getPort() > 65535) {
            errors.add("数据库端口号无效: " + dbConfig.getPort());
        }
        
        if (dbConfig.getDatabase() == null || dbConfig.getDatabase().trim().isEmpty()) {
            errors.add("数据库名称不能为空");
        }
        
        if (dbConfig.getUsername() == null || dbConfig.getUsername().trim().isEmpty()) {
            errors.add("数据库用户名不能为空");
        }
        
        // 测试数据库连接
        if (errors.isEmpty()) {
            if (!testConnection(dbConfig.getHost(), dbConfig.getPort())) {
                errors.add(String.format("无法连接到数据库 %s:%d", dbConfig.getHost(), dbConfig.getPort()));
            }
        }
        
        return errors;
    }
    
    /**
     * 验证Redis配置
     */
    private List<String> validateRedisConfig(ApplicationProperties.Redis redisConfig) {
        List<String> errors = new ArrayList<>();
        
        if (redisConfig.getHost() == null || redisConfig.getHost().trim().isEmpty()) {
            errors.add("Redis主机地址不能为空");
        }
        
        if (redisConfig.getPort() <= 0 || redisConfig.getPort() > 65535) {
            errors.add("Redis端口号无效: " + redisConfig.getPort());
        }
        
        if (redisConfig.getDatabase() < 0 || redisConfig.getDatabase() > 15) {
            errors.add("Redis数据库索引无效: " + redisConfig.getDatabase());
        }
        
        // 测试Redis连接
        if (errors.isEmpty()) {
            if (!testConnection(redisConfig.getHost(), redisConfig.getPort())) {
                errors.add(String.format("无法连接到Redis %s:%d", redisConfig.getHost(), redisConfig.getPort()));
            }
        }
        
        return errors;
    }
    
    /**
     * 验证API配置
     */
    private List<String> validateApiConfig(ApplicationProperties.Api apiConfig) {
        List<String> errors = new ArrayList<>();
        
        if (apiConfig.getBaseUrl() == null || apiConfig.getBaseUrl().trim().isEmpty()) {
            errors.add("API基础URL不能为空");
        } else {
            if (!apiConfig.getBaseUrl().startsWith("http://") && !apiConfig.getBaseUrl().startsWith("https://")) {
                errors.add("API基础URL格式无效，必须以http://或https://开头");
            }
        }
        
        if (apiConfig.getMatchSignature() == null || apiConfig.getMatchSignature().trim().isEmpty()) {
            errors.add("API签名不能为空");
        }
        
        return errors;
    }
    
    /**
     * 验证Kafka配置
     */
    private List<String> validateKafkaConfig(ApplicationProperties.Kafka kafkaConfig) {
        List<String> errors = new ArrayList<>();
        
        if (kafkaConfig.getBootstrapServers() == null || kafkaConfig.getBootstrapServers().trim().isEmpty()) {
            errors.add("Kafka服务器地址不能为空");
        }
        
        if (kafkaConfig.getGroupId() == null || kafkaConfig.getGroupId().trim().isEmpty()) {
            errors.add("Kafka消费者组ID不能为空");
        }
        
        if (kafkaConfig.getTopicPrefix() == null || kafkaConfig.getTopicPrefix().trim().isEmpty()) {
            errors.add("Kafka主题前缀不能为空");
        }
        
        return errors;
    }
    
    /**
     * 测试网络连接
     */
    private boolean testConnection(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000); // 5秒超时
            return true;
        } catch (Exception e) {
            log.warn("连接测试失败: {}:{} - {}", host, port, e.getMessage());
            return false;
        }
    }
    
    /**
     * 打印配置验证结果
     */
    public void printValidationResults(List<String> errors) {
        if (errors.isEmpty()) {
            log.info("✅ 所有配置验证通过");
        } else {
            log.error("❌ 发现 {} 个配置错误:", errors.size());
            for (int i = 0; i < errors.size(); i++) {
                log.error("  {}. {}", i + 1, errors.get(i));
            }
        }
    }
}

package com.weichai.knowledge.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

/**
 * WebClient配置类 - 采用非阻塞I/O模型
 */
@Configuration
public class WebClientConfig {
    
    @Autowired
    private ApplicationProperties applicationProperties;
    
    /**
     * 创建WebClient Bean，替换RestTemplate
     * 基于响应式编程，提供更高的并发性能
     */
    @Bean
    public WebClient webClient() {
        // 配置连接池
        ConnectionProvider connectionProvider = ConnectionProvider.builder("custom")
                .maxConnections(100)  // 最大连接数
                .maxIdleTime(Duration.ofSeconds(20))  // 最大空闲时间
                .maxLifeTime(Duration.ofSeconds(60))  // 连接最大生命周期
                .pendingAcquireTimeout(Duration.ofSeconds(60))  // 获取连接超时时间
                .evictInBackground(Duration.ofSeconds(120))  // 后台清理周期
                .build();
        
        // 配置HttpClient
        HttpClient httpClient = HttpClient.create(connectionProvider)
                .responseTimeout(Duration.ofSeconds(30))  // 响应超时
                .keepAlive(true);  // 启用keep-alive
        
        return WebClient.builder()
                .baseUrl(applicationProperties.getApi().getBaseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeaders(headers -> {
                    headers.add("Content-Type", "application/json");
                    headers.add("superhelper-login-state", "matchApi");
                    // 注意：动态签名header将在每个请求中单独设置
                })
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10MB
                .build();
    }
} 
package com.weichai.knowledge.config;

import com.weichai.knowledge.config.ApplicationProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * 响应式Redis配置类
 * 配置响应式Redis连接和模板
 */
@Configuration
public class ReactiveRedisConfig {
    
    @Autowired
    private ApplicationProperties applicationProperties;
    
    /**
     * 配置Lettuce连接工厂（用于响应式Redis）
     */
    @Bean
    @org.springframework.context.annotation.Primary
    public LettuceConnectionFactory lettuceConnectionFactory() {
        ApplicationProperties.Redis redisConfig = applicationProperties.getRedis();
        
        // Redis连接配置
        RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
        redisStandaloneConfiguration.setHostName(redisConfig.getHost());
        redisStandaloneConfiguration.setPort(redisConfig.getPort());
        redisStandaloneConfiguration.setDatabase(redisConfig.getDatabase());
        
        // 设置密码（如果有）
        if (redisConfig.getPassword() != null && !redisConfig.getPassword().isEmpty()) {
            redisStandaloneConfiguration.setPassword(redisConfig.getPassword());
        }
        
        return new LettuceConnectionFactory(redisStandaloneConfiguration);
    }
    
    /**
     * 配置ReactiveStringRedisTemplate
     * 用于响应式操作字符串类型的数据
     */
    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(@Qualifier("lettuceConnectionFactory") LettuceConnectionFactory lettuceConnectionFactory) {
        return new ReactiveStringRedisTemplate(lettuceConnectionFactory);
    }
    
    /**
     * 配置ReactiveRedisTemplate
     * 用于响应式操作对象类型的数据
     */
    @Bean
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(@Qualifier("lettuceConnectionFactory") LettuceConnectionFactory lettuceConnectionFactory) {
        // 创建序列化器
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
        
        // 构建序列化上下文
        RedisSerializationContext<String, Object> serializationContext = RedisSerializationContext
                .<String, Object>newSerializationContext()
                .key(stringRedisSerializer)
                .value(jackson2JsonRedisSerializer)
                .hashKey(stringRedisSerializer)
                .hashValue(jackson2JsonRedisSerializer)
                .build();
        
        return new ReactiveRedisTemplate<>(lettuceConnectionFactory, serializationContext);
    }
    
    /**
     * 配置缓存管理器
     * 使用主要的连接工厂
     */
    @Bean
    public CacheManager cacheManager(RedisConnectionFactory redisConnectionFactory) {
        return RedisCacheManager.RedisCacheManagerBuilder
                .fromConnectionFactory(redisConnectionFactory)
                .build();
    }
}

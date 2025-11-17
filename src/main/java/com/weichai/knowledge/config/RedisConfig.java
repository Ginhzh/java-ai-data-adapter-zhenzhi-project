package com.weichai.knowledge.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * Redis配置类
 * 配置Redis连接和序列化方式
 */
@Configuration
@EnableCaching
public class RedisConfig extends CachingConfigurerSupport {
    
    @Autowired
    private ApplicationProperties applicationProperties;
    
    /**
     * 配置Redis连接工厂
     */
    @Bean
    public JedisConnectionFactory jedisConnectionFactory() {
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
        
        // 连接池配置
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(50);           // 最大连接数
        jedisPoolConfig.setMaxIdle(20);            // 最大空闲连接数
        jedisPoolConfig.setMinIdle(5);             // 最小空闲连接数
        jedisPoolConfig.setMaxWait(Duration.ofMillis(3000));    // 获取连接时的最大等待时间
        jedisPoolConfig.setTestOnBorrow(true);     // 获取连接时检测有效性
        jedisPoolConfig.setTestOnReturn(true);     // 归还连接时检测有效性
        jedisPoolConfig.setTestWhileIdle(true);    // 空闲时检测有效性
        jedisPoolConfig.setMinEvictableIdleTime(Duration.ofMillis(60000));      // 连接最小空闲时间
        jedisPoolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(30000));   // 空闲连接检测周期
        
        // Jedis客户端配置
        JedisClientConfiguration.JedisClientConfigurationBuilder jedisClientConfiguration = 
            JedisClientConfiguration.builder();
        jedisClientConfiguration.connectTimeout(Duration.ofMillis(3000));
        jedisClientConfiguration.readTimeout(Duration.ofMillis(3000));
        jedisClientConfiguration.usePooling().poolConfig(jedisPoolConfig);
        
        return new JedisConnectionFactory(redisStandaloneConfiguration, jedisClientConfiguration.build());
    }
    
    /**
     * 配置RedisTemplate
     * 用于操作对象类型的数据
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(@Qualifier("jedisConnectionFactory") RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // Jackson序列化配置
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        objectMapper.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL);
        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);
        
        // String序列化
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        
        // key采用String的序列化方式
        template.setKeySerializer(stringRedisSerializer);
        // hash的key也采用String的序列化方式
        template.setHashKeySerializer(stringRedisSerializer);
        // value序列化方式采用jackson
        template.setValueSerializer(jackson2JsonRedisSerializer);
        // hash的value序列化方式采用jackson
        template.setHashValueSerializer(jackson2JsonRedisSerializer);
        
        template.afterPropertiesSet();
        return template;
    }
    
    /**
     * 配置StringRedisTemplate
     * 用于操作字符串类型的数据
     */
    @Bean
    public StringRedisTemplate stringRedisTemplate(@Qualifier("jedisConnectionFactory") RedisConnectionFactory connectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(connectionFactory);
        
        // 设置序列化方式
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringRedisSerializer);
        template.setValueSerializer(stringRedisSerializer);
        template.setHashKeySerializer(stringRedisSerializer);
        template.setHashValueSerializer(stringRedisSerializer);
        
        template.afterPropertiesSet();
        return template;
    }

}
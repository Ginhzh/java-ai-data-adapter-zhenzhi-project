package com.weichai.knowledge.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.lang.NonNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

/**
 * 响应式数据库配置类
 */
@Configuration
@EnableTransactionManagement
@EnableR2dbcRepositories(basePackages = "com.weichai.knowledge.repository")
public class DatabaseConfig extends AbstractR2dbcConfiguration {
    
    @Autowired
    private ApplicationProperties applicationProperties;

    /**
     * 配置R2DBC连接工厂
     * 对应Python项目中的数据库连接配置，但使用响应式连接
     */
    @Override
    @Bean
    @NonNull
    public ConnectionFactory connectionFactory() {
        ApplicationProperties.Database dbConfig = applicationProperties.getDatabase();

        // 构建R2DBC连接选项
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, dbConfig.getHost())
                .option(PORT, dbConfig.getPort())
                .option(USER, dbConfig.getUsername())
                .option(PASSWORD, dbConfig.getPassword())
                .option(DATABASE, dbConfig.getDatabase())
                .build();

        // 创建基础连接工厂
        ConnectionFactory connectionFactory = ConnectionFactories.get(options);

        // 配置连接池
        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxIdleTime(Duration.ofMinutes(5))
                .maxLifeTime(Duration.ofMinutes(20))
                .maxAcquireTime(Duration.ofSeconds(20))
                .maxCreateConnectionTime(Duration.ofSeconds(20))
                .initialSize(5)
                .maxSize(20)
                .validationQuery("SELECT 1")
                .build();

        return new ConnectionPool(poolConfig);
    }

    /**
     * 配置自定义转换器
     */
    @Override
    @NonNull
    protected List<Object> getCustomConverters() {
        List<Object> converters = new ArrayList<>();
        converters.add(new JsonNodeToStringConverter());
        converters.add(new StringToJsonNodeConverter());
        return converters;
    }

    /**
     * JsonNode 转 String 转换器（写入数据库时使用）
     */
    @WritingConverter
    static class JsonNodeToStringConverter implements Converter<JsonNode, String> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public String convert(@NonNull JsonNode source) {
            if (source.isNull()) {
                return null;
            }
            try {
                return objectMapper.writeValueAsString(source);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to convert JsonNode to String", e);
            }
        }
    }

    /**
     * String 转 JsonNode 转换器（从数据库读取时使用）
     */
    @ReadingConverter
    static class StringToJsonNodeConverter implements Converter<String, JsonNode> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        @NonNull
        public JsonNode convert(@NonNull String source) {
            if (source.trim().isEmpty()) {
                return objectMapper.createArrayNode(); // 返回空数组而不是null
            }
            try {
                return objectMapper.readTree(source);
            } catch (JsonProcessingException e) {
                // 如果解析失败，尝试作为简单字符串处理
                return objectMapper.valueToTree(source);
            }
        }
    }
}

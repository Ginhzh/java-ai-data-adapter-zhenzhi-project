package com.weichai.knowledge;

import com.weichai.knowledge.config.ApplicationProperties;
import com.weichai.knowledge.utils.ConfigValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * 知识库服务主应用类
 * 对应Python项目中的app.py
 */
@Slf4j
@SpringBootApplication
@EnableKafka
@EnableConfigurationProperties
@ComponentScan(basePackages = {
    "com.weichai.knowledge"
})
public class KnowledgeServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KnowledgeServiceApplication.class, args);
    }

    /**
     * 应用启动后执行配置验证
     */
    @Bean
    public CommandLineRunner configValidationRunner(ApplicationProperties properties, ConfigValidator validator) {
        return args -> {
            log.info("🚀 知识库服务启动完成，开始验证配置...");

            var errors = validator.validateConfiguration(properties);
            validator.printValidationResults(errors);

            if (!errors.isEmpty()) {
                log.warn("⚠️  发现配置问题，但服务将继续运行。建议检查并修复上述问题。");
            } else {
                log.info("✅ 配置验证通过，服务已就绪！");
            }
        };
    }
}
package com.weichai.knowledge.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 应用配置属性类
 * 对应Python项目中的Settings类
 */
@Data
@Component
@ConfigurationProperties(prefix = "app")
public class ApplicationProperties {
    
    private Database database = new Database();
    private Redis redis = new Redis();
    private Kafka kafka = new Kafka();
    private Api api = new Api();
    private Space space = new Space();
    
    @Data
    public static class Database {
        private String host = "10.3.80.24";
        private int port = 32647;
        private String username = "knowledge";
        private String password = "Weichai@123";
        private String database = "knowledge_db";
        private String charset = "utf8";
        
        /**
         * 获取数据库连接URL
         */
        public String getUrl() {
            return String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&autoReconnect=true",
                    host, port, database);
        }
    }
    
    @Data
    public static class Redis {
        private String host = "10.3.80.24";
        private int port = 30223;
        private String password = "";
        private int database = 0;
    }
    
    @Data
    public static class Kafka {
        private String bootstrapServers = "10.3.80.24:31946";
        private String groupId = "external-test-groupnew";
        private String topicPrefix = "ZHENZHI_DATA_TEST_";
        private String autoOffsetReset = "latest";
        private boolean enableAutoCommit = false;
    }
    
    @Data
    public static class Api {
        private String baseUrl = "http://10.74.32.195";
        private String matchSignature = "5+fAMdUvi9IlKcuodRPaGAFRot/twrIq1hfRc5KYkFnVDoGI+q5TseWFxOQG5RBBHc2d0XTLoWHjXWX94rFOiPndW6//DI2txYIavp6KtDm7xNg23h7noGCCW/anHpdI";
    }
    
    @Data
    public static class Space {
        private String guid = "G6pmhUbcb5";
    }
} 
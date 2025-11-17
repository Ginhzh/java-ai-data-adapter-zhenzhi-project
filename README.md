# 知识库服务 Java版本

这是Python知识库服务项目的Java重写版本，使用Spring Boot框架开发。

## 📋 项目简介

本项目是基于Spring Boot的知识库管理服务，提供文档管理、角色权限管理、Kafka消息处理等功能。

## 🏗️ 技术栈

- **框架**: Spring Boot 3.2.0
- **数据库**: MySQL 8.0+
- **ORM**: Spring Data JPA + Hibernate
- **缓存**: Redis
- **消息队列**: Apache Kafka
- **构建工具**: Maven
- **Java版本**: 17+

## 📂 项目结构

```
knowledge-service-java/
├── src/main/java/com/weichai/knowledge/
│   ├── KnowledgeServiceApplication.java    # 主应用类
│   ├── config/                            # 配置类
│   │   ├── ApplicationProperties.java     # 应用配置属性
│   │   └── DatabaseConfig.java           # 数据库配置
│   ├── controller/                        # 控制器层
│   │   └── HealthController.java         # 健康检查接口
│   ├── entity/                           # 实体类
│   │   ├── DocumentMetadata.java
│   │   ├── UnstructuredDocument.java
│   │   ├── DocumentPermission.java
│   │   ├── KafkaMessageLog.java
│   │   └── KnowledgeRoleSyncLog.java
│   └── repository/                       # 数据访问层
│       ├── DocumentMetadataRepository.java
│       └── UnstructuredDocumentRepository.java
├── src/main/resources/
│   └── application.yml                   # 应用配置文件
├── src/test/                            # 测试代码
└── pom.xml                              # Maven依赖配置
```

## 🚀 快速开始

### 环境要求

- JDK 17+
- Maven 3.6+
- MySQL 8.0+
- Redis 5.0+
- Apache Kafka 2.8+

### 配置数据库

确保MySQL数据库运行在 `10.3.80.24:32647`，数据库名为 `knowledge_db`。

### 运行项目

1. **克隆项目到本地**
```bash
cd knowledge-service-java
```

2. **编译项目**
```bash
mvn clean compile
```

3. **运行测试**
```bash
mvn test
```

4. **启动应用**
```bash
mvn spring-boot:run
```

### 在IDEA中运行

1. 用IDEA打开 `knowledge-service-java` 文件夹
2. 确保JDK设置为17+
3. 等待Maven依赖下载完成
4. 右键运行 `KnowledgeServiceApplication.java`

## 🔍 验证运行状态

项目启动后，访问以下URL验证：

- **基本健康检查**: http://localhost:8080/api/health
- **数据库连接检查**: http://localhost:8080/api/health/database  
- **配置信息检查**: http://localhost:8080/api/health/config
- **Spring Boot健康检查**: http://localhost:8080/actuator/health

## 📝 配置说明

主要配置在 `application.yml` 中：

```yaml
# 数据库配置
spring:
  datasource:
    url: jdbc:mysql://10.3.80.24:32647/knowledge_db
    username: knowledge
    password: Weichai@123

# Redis配置  
  data:
    redis:
      host: 10.3.80.24
      port: 30223

# Kafka配置
  kafka:
    bootstrap-servers: 10.3.80.24:31946
```

## 🔧 开发说明

### 添加新的实体类

1. 在 `entity` 包下创建JPA实体类
2. 在 `repository` 包下创建对应的Repository接口
3. 使用JPA注解进行数据库映射

### 添加新的API接口

1. 在 `controller` 包下创建控制器类
2. 使用Spring Web注解定义REST接口
3. 在 `service` 包下创建业务逻辑类

## 🐛 常见问题

### 数据库连接失败
- 检查数据库服务是否运行
- 验证IP和端口是否正确
- 确认用户名密码是否正确

### 端口冲突
- 修改 `application.yml` 中的 `server.port`
- 或使用命令行参数：`--server.port=8081`

### Maven依赖下载失败
- 检查网络连接
- 配置Maven国内镜像源
- 执行 `mvn clean compile` 重新下载

## 📄 许可证

本项目采用内部许可证。

## 👥 维护者

- 开发团队
- 联系邮箱: [开发团队邮箱] 
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Spring Boot-based knowledge management service (知识库服务) written in Java 17. It's a Java rewrite of an existing Python knowledge service, providing document management, role-based permissions, and Kafka message processing capabilities.

**Key Technologies:**
- Spring Boot 3.0.0 with WebFlux (reactive)
- R2DBC for reactive database access (MySQL)
- Redis for caching (both blocking and reactive)
- Apache Kafka for message processing
- Docker containerization

## Development Commands

### Building and Running
```bash
# Clean and compile
mvn clean compile

# Run tests
mvn test

# Start the application
mvn spring-boot:run

# Build Docker image
docker build -t knowledge-service .

# Run with custom port
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082
```

### Development in IDE
- Use IntelliJ IDEA
- Ensure JDK 17+ is configured
- Run `KnowledgeServiceApplication.java` main class

### Health Checks
After startup, verify the service at:
- http://localhost:8081/api/health (basic health)
- http://localhost:8081/api/health/database (database connectivity)
- http://localhost:8081/actuator/health (Spring actuator)

## Architecture

### Reactive Architecture Pattern
This service follows a reactive programming model using Spring WebFlux and R2DBC:

- **Controllers**: Handle HTTP requests reactively, returning `Mono<>` and `Flux<>` types
- **Services**: Business logic with reactive operations
- **Repositories**: R2DBC-based reactive data access using `ReactiveRepository` interfaces
- **Redis**: Dual setup with both blocking (`RedisManager`) and reactive (`ReactiveRedisManager`) access patterns

### Core Components

**Configuration Layer** (`config/`):
- `ReactiveRedisConfig` and `RedisConfig` - Dual Redis setup for different access patterns  
- `KafkaConfig` - Message processing configuration
- `DatabaseConfig` - R2DBC database setup
- `GlobalExceptionHandler` - Centralized error handling

**Data Layer** (`entity/`, `repository/`):
- Uses R2DBC entities (no JPA annotations)
- Reactive repositories extending `ReactiveCrudRepository`
- Key entities: `UnstructuredDocument`, `DocumentStatus`, `KafkaMessageLog`

**Service Layer** (`service/`):
- Reactive services with `Mono`/`Flux` return types
- Message handlers for Kafka processing
- Statistics and monitoring services

**Redis Management**:
- `SyncManager` and `ReactiveSyncManager` - Synchronization utilities
- `DBRedisFactory` - Multi-database Redis connections

### Message Processing
Kafka integration handles external data synchronization with topics prefixed by `ZHENZHI_DATA_TEST_`. The system processes:
- Document status updates
- Role synchronization messages  
- File operation tracking

### Environment Configuration
Uses environment variables with defaults in `application.yml`:
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE` - Database connection
- `REDIS_HOST`, `REDIS_PORT` - Redis connection
- `KAFKA_TEST_BOOTSTRAP_SERVERS` - Kafka brokers
- `API_PORT` - Service port (default: 8081)

### Docker Deployment
Multi-stage build using custom base image `middleware/knowledge-base:maven` with optimized dependency caching and parallel Maven execution (`-T 4C`).
# 多阶段构建Dockerfile
# 阶段1: 构建阶段 - 使用自定义基础镜像
FROM 10.3.80.25:30003/middleware/knowledge-base:maven AS builder

# 设置工作目录
WORKDIR /app

# 创建Maven目录并复制配置文件
RUN mkdir -p /root/.m2
COPY settings.xml /root/.m2/settings.xml

# 复制Maven配置文件
COPY pom.xml .

# 下载依赖（利用Docker层缓存，并行下载提升速度）
RUN mvn dependency:go-offline -B -T 4C --fail-at-end

# 复制源代码
COPY src ./src

# 构建应用
RUN mvn clean package -DskipTests -B

# 阶段2: 运行阶段 - 使用自定义基础镜像
FROM 10.3.80.25:30003/middleware/knowledge-base:runtime

# 从构建阶段复制JAR文件
COPY --from=builder /app/target/knowledge-service-*.jar app.jar

# 创建日志目录
RUN mkdir -p logs && chown -R app:app /app

# 切换到非root用户
USER app

# 暴露端口
EXPOSE 8081

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8081/actuator/health || exit 1

# JVM配置和启动命令 (兼容Java 11+)
ENV JAVA_OPTS="-Xms512m -Xmx1024m -XX:+UseG1GC -Xlog:gc*:logs/gc.log:time -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"] 

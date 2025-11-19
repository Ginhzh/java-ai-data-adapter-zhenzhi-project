package com.weichai.knowledge.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;

/**
 * 响应式Redis缓存管理器
 * 提供统一的响应式Redis操作接口，实现完全非阻塞的缓存操作
 * 
 * 主要特性：
 * - 完全响应式架构，基于Mono/Flux
 * - 非阻塞I/O操作
 * - 背压支持
 * - 链式错误处理
 */
@Slf4j
@Component
public class ReactiveRedisManager {
    
    @Autowired
    private ReactiveStringRedisTemplate reactiveStringRedisTemplate;
    
    @Autowired
    private ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // 默认过期时间（24小时）
    private static final Duration DEFAULT_EXPIRE = Duration.ofHours(24);
    
    // 缓存键前缀
    private static final String KAFKA_MESSAGE_PREFIX = "kafka:message:";
    private static final String KAFKA_STATUS_PREFIX = "kafka:status:";
    private static final String PROCESSING_QUEUE_KEY = "kafka:processing_queue";
    private static final String SYNC_LIST_SUFFIX = ":sync_list";
    
    @PostConstruct
    public Mono<Void> init() {
        return Mono.fromRunnable(() -> log.info("ReactiveRedisManager初始化完成"));
    }
    
    /**
     * 设置值 - 响应式版本
     *
     * @param key    键
     * @param value  值
     * @param expire 过期时间，null则使用默认值
     * @return 是否成功
     */
    public Mono<Boolean> set(String key, Object value, Duration expire) {
        return Mono.fromCallable(() -> {
                if (value instanceof String) {
                    return (String) value;
                } else {
                    return objectMapper.writeValueAsString(value);
                }
            })
            .flatMap(jsonValue -> {
                Duration finalExpire = expire != null ? expire : DEFAULT_EXPIRE;
                return reactiveStringRedisTemplate.opsForValue()
                    .set(key, jsonValue, finalExpire);
            })
            .doOnSuccess(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.debug("Redis set操作成功: key={}", key);
                }
            })
            .onErrorResume(error -> {
                log.error("Redis set操作失败: key={}, error={}", key, error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 设置值（使用默认过期时间）
     */
    public Mono<Boolean> set(String key, Object value) {
        return set(key, value, null);
    }
    
    /**
     * 获取值 - 响应式版本
     */
    public <T> Mono<T> get(String key, Class<T> clazz) {
        return reactiveStringRedisTemplate.opsForValue().get(key)
            .flatMap(value -> {
                if (clazz == String.class) {
                    return Mono.just(clazz.cast(value));
                }
                
                return Mono.fromCallable(() -> objectMapper.readValue(value, clazz));
            })
            .onErrorResume(error -> {
                log.error("Redis get操作失败: key={}, error={}", key, error.getMessage());
                return Mono.empty();
            });
    }
    
    /**
     * 获取字符串值
     */
    public Mono<String> get(String key) {
        return get(key, String.class);
    }
    
    /**
     * 获取Map类型的值 - 响应式版本
     */
    @SuppressWarnings("unchecked")
    public Mono<Map<String, Object>> getMap(String key) {
        return reactiveStringRedisTemplate.opsForValue().get(key)
            .flatMap(value -> Mono.fromCallable(() -> 
                (Map<String, Object>) objectMapper.readValue(value, 
                    TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class))))
            .onErrorResume(error -> {
                log.error("Redis getMap操作失败: key={}, error={}", key, error.getMessage());
                return Mono.empty();
            });
    }
    
    /**
     * 获取List类型的值 - 响应式版本
     */
    @SuppressWarnings("unchecked")
    public <T> Mono<List<T>> getList(String key, Class<T> elementClass) {
        return reactiveStringRedisTemplate.opsForValue().get(key)
            .flatMap(value -> Mono.fromCallable(() -> {
                if (elementClass == String.class) {
                    return (List<T>) objectMapper.readValue(value, 
                        TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, String.class));
                }
                
                return (List<T>) objectMapper.readValue(value, 
                    TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, elementClass));
            }))
            .onErrorResume(error -> {
                log.error("Redis getList操作失败: key={}, error={}", key, error.getMessage());
                return Mono.empty();
            });
    }
    
    /**
     * 删除键 - 响应式版本
     */
    public Mono<Boolean> delete(String key) {
        return reactiveStringRedisTemplate.delete(key)
            .map(count -> count > 0)
            .doOnSuccess(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.debug("Redis delete操作成功: key={}", key);
                }
            })
            .onErrorResume(error -> {
                log.error("Redis delete操作失败: key={}, error={}", key, error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 批量删除键 - 响应式版本
     */
    public Mono<Long> delete(Collection<String> keys) {
        return Flux.fromIterable(keys)
            .flatMap(this::delete)
            .filter(result -> result)
            .count()
            .onErrorResume(error -> {
                log.error("Redis批量delete操作失败: error={}", error.getMessage());
                return Mono.just(0L);
            });
    }
    
    /**
     * 检查键是否存在 - 响应式版本
     */
    public Mono<Boolean> exists(String key) {
        return reactiveStringRedisTemplate.hasKey(key)
            .onErrorResume(error -> {
                log.error("Redis exists操作失败: key={}, error={}", key, error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 设置过期时间 - 响应式版本
     */
    public Mono<Boolean> expire(String key, Duration duration) {
        return reactiveStringRedisTemplate.expire(key, duration)
            .onErrorResume(error -> {
                log.error("Redis expire操作失败: key={}, error={}", key, error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 获取剩余过期时间 - 响应式版本
     *
     * @return 剩余时间，如果键不存在或没有过期时间则返回负值
     */
    public Mono<Duration> ttl(String key) {
        return reactiveStringRedisTemplate.getExpire(key)
            .onErrorResume(error -> {
                log.error("Redis ttl操作失败: key={}, error={}", key, error.getMessage());
                return Mono.just(Duration.ofSeconds(-2));
            });
    }
    
    /**
     * 设置Hash字段 - 响应式版本
     */
    public Mono<Boolean> hset(String key, String field, Object value) {
        return Mono.fromCallable(() -> 
                value instanceof String ? (String) value : objectMapper.writeValueAsString(value))
            .flatMap(jsonValue -> 
                reactiveStringRedisTemplate.opsForHash().put(key, field, jsonValue))
            .onErrorResume(error -> {
                log.error("Redis hset操作失败: key={}, field={}, error={}", key, field, error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 批量设置Hash字段 - 响应式版本
     */
    public Mono<Boolean> hmset(String key, Map<String, Object> map) {
        return Flux.fromIterable(map.entrySet())
            .flatMap(entry -> Mono.fromCallable(() -> {
                Object value = entry.getValue();
                String jsonValue = value instanceof String ? (String) value : objectMapper.writeValueAsString(value);
                return Map.entry(entry.getKey(), jsonValue);
            }))
            .collectMap(Map.Entry::getKey, Map.Entry::getValue)
            .flatMap(stringMap -> 
                reactiveStringRedisTemplate.opsForHash().putAll(key, stringMap))
            .onErrorResume(error -> {
                log.error("Redis hmset操作失败: key={}, error={}", key, error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 获取Hash字段值 - 响应式版本
     */
    public Mono<Object> hget(String key, String field) {
        return reactiveStringRedisTemplate.opsForHash().get(key, field)
            .cast(String.class)
            .flatMap(strValue -> Mono.fromCallable(() -> {
                // 尝试解析JSON
                if ((strValue.startsWith("{") || strValue.startsWith("["))) {
                    try {
                        return objectMapper.readValue(strValue, Object.class);
                    } catch (Exception ignored) {
                        return strValue;
                    }
                }
                return strValue;
            }))
            .onErrorResume(error -> {
                log.error("Redis hget操作失败: key={}, field={}, error={}", key, field, error.getMessage());
                return Mono.empty();
            });
    }
    
    /**
     * 获取Hash所有字段 - 响应式版本
     */
    public Mono<Map<String, Object>> hgetall(String key) {
        return reactiveStringRedisTemplate.opsForHash().entries(key)
            .cast(Map.Entry.class)
            .flatMap(entry -> Mono.fromCallable(() -> {
                String field = entry.getKey().toString();
                String value = entry.getValue().toString();
                
                // 尝试解析JSON
                Object parsedValue;
                if ((value.startsWith("{") || value.startsWith("["))) {
                    try {
                        parsedValue = objectMapper.readValue(value, Object.class);
                    } catch (Exception ignored) {
                        parsedValue = value;
                    }
                } else {
                    parsedValue = value;
                }
                
                return Map.entry(field, parsedValue);
            }))
            .collectMap(Map.Entry::getKey, Map.Entry::getValue)
            .onErrorResume(error -> {
                log.error("Redis hgetall操作失败: key={}, error={}", key, error.getMessage());
                return Mono.just(new HashMap<>());
            });
    }
    
    /**
     * 生成缓存键
     */
    public String generateKey(String prefix, String... parts) {
        List<String> keyParts = new ArrayList<>();
        keyParts.add(prefix);
        for (String part : parts) {
            if (part != null) {
                keyParts.add(part);
            }
        }
        return String.join(":", keyParts);
    }
    
    // ========== 消息缓存相关方法 ==========
    
    /**
     * 缓存消息数据 - 响应式版本
     */
    public Mono<Boolean> cacheMessage(String messageId, Map<String, Object> messageData, Duration expire) {
        String key = KAFKA_MESSAGE_PREFIX + messageId;
        return set(key, messageData, expire)
            .doOnSuccess(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.info("消息已缓存到Redis: {}", key);
                }
            });
    }
    
    /**
     * 缓存消息数据（默认1小时过期）
     */
    public Mono<Boolean> cacheMessage(String messageId, Map<String, Object> messageData) {
        return cacheMessage(messageId, messageData, Duration.ofHours(1));
    }
    
    /**
     * 获取缓存的消息数据 - 响应式版本
     */
    public Mono<Map<String, Object>> getMessage(String messageId) {
        String key = KAFKA_MESSAGE_PREFIX + messageId;
        return getMap(key);
    }
    
    /**
     * 删除缓存的消息数据 - 响应式版本
     */
    public Mono<Boolean> removeMessage(String messageId) {
        String key = KAFKA_MESSAGE_PREFIX + messageId;
        return delete(key)
            .doOnSuccess(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.info("已从Redis删除消息: {}", key);
                }
            });
    }
    
    // ========== 处理队列相关方法 ==========
    
    /**
     * 添加到处理队列 - 响应式版本
     *
     * @param messageId 消息ID
     * @param priority  优先级，值越小优先级越高
     */
    public Mono<Boolean> addToProcessingQueue(String messageId, double priority) {
        return reactiveStringRedisTemplate.opsForZSet().add(PROCESSING_QUEUE_KEY, messageId, priority)
            .doOnSuccess(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.info("消息已添加到处理队列: {}, 优先级: {}", messageId, priority);
                }
            })
            .onErrorResume(error -> {
                log.error("添加消息到处理队列失败: messageId={}, error={}", messageId, error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 获取处理队列中的消息ID列表 - 响应式版本
     */
    public Flux<String> getProcessingQueue(int count) {
        return reactiveStringRedisTemplate.opsForZSet().range(PROCESSING_QUEUE_KEY, 
                org.springframework.data.domain.Range.closed(0L, (long) count - 1))
            .onErrorResume(error -> {
                log.error("获取处理队列失败: error={}", error.getMessage());
                return Flux.empty();
            });
    }
    
    /**
     * 从处理队列中移除消息 - 响应式版本
     */
    public Mono<Boolean> removeFromProcessingQueue(String messageId) {
        return reactiveStringRedisTemplate.opsForZSet().remove(PROCESSING_QUEUE_KEY, messageId)
            .map(removed -> removed > 0)
            .doOnSuccess(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.info("已从处理队列移除消息: {}", messageId);
                }
            })
            .onErrorResume(error -> {
                log.error("从处理队列移除消息失败: messageId={}, error={}", messageId, error.getMessage());
                return Mono.just(false);
            });
    }

    /**
     * 自增Hash字段 - 响应式版
     */
    public Mono<Long> hincrBy(String key, String field, long delta) {
        return reactiveStringRedisTemplate.opsForHash()
            .increment(key, field, delta)
            .doOnError(e -> log.warn("Redis HINCRBY失败: key={}, field={}, error={}", key, field, e.getMessage()))
            .onErrorResume(e -> Mono.empty());
    }
    
    /**
     * 设置消息处理状态 - 响应式版本
     */
    public Mono<Boolean> setProcessingStatus(String messageId, String status) {
        String key = KAFKA_STATUS_PREFIX + messageId;
        return set(key, status, Duration.ofHours(1)) // 状态信息保留1小时
            .doOnSuccess(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.info("已设置消息处理状态: {}, 状态: {}", messageId, status);
                }
            });
    }
    
    /**
     * 获取消息处理状态 - 响应式版本
     */
    public Mono<String> getProcessingStatus(String messageId) {
        String key = KAFKA_STATUS_PREFIX + messageId;
        return get(key);
    }
    
    // ========== 数据同步相关方法 ==========
    
    /**
     * 标记记录需要同步到数据库 - 响应式版本
     */
    public Mono<Void> markForSync(String redisKeyPrefix, String recordId) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        return getList(syncKey, String.class)
            .defaultIfEmpty(new ArrayList<>())
            .flatMap(syncList -> {
                if (!syncList.contains(recordId)) {
                    syncList.add(recordId);
                    return set(syncKey, syncList)
                        .doOnSuccess(result -> log.debug("记录 {} 已标记为需要同步", recordId));
                }
                return Mono.just(true);
            })
            .then();
    }
    
    /**
     * 获取需要同步的记录ID列表 - 响应式版本
     */
    public Mono<List<String>> getSyncList(String redisKeyPrefix) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        return getList(syncKey, String.class)
            .defaultIfEmpty(new ArrayList<>());
    }
    
    /**
     * 从同步列表中移除记录 - 响应式版本
     */
    public Mono<Void> removeFromSyncList(String redisKeyPrefix, String recordId) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        return getList(syncKey, String.class)
            .flatMap(syncList -> {
                if (syncList != null && syncList.contains(recordId)) {
                    syncList.remove(recordId);
                    if (syncList.isEmpty()) {
                        return delete(syncKey).then();
                    } else {
                        return set(syncKey, syncList).then();
                    }
                }
                return Mono.empty();
            });
    }
    
    /**
     * 清空同步列表 - 响应式版本
     */
    public Mono<Void> clearSyncList(String redisKeyPrefix) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        return delete(syncKey).then();
    }
    
    /**
     * 获取或设置缓存值 - 响应式版本
     * 如果缓存存在则返回，不存在则通过getter获取并设置缓存
     */
    public <T> Mono<T> getOrSet(String key, Class<T> clazz, ReactiveCacheGetter<T> getter, Duration expireTime) {
        return get(key, clazz)
            .doOnNext(cachedValue -> log.debug("从缓存获取数据: {}", key))
            .switchIfEmpty(
                getter.get()
                    .flatMap(value -> 
                        set(key, value, expireTime)
                            .doOnSuccess(result -> log.debug("将数据存入缓存: {}", key))
                            .thenReturn(value)
                    )
            );
    }
    
    /**
     * 响应式缓存获取器函数式接口
     */
    @FunctionalInterface
    public interface ReactiveCacheGetter<T> {
        Mono<T> get();
    }
    
    /**
     * 批量获取缓存值 - 响应式版本
     */
    public <T> Mono<Map<String, T>> multiGet(Collection<String> keys, Class<T> clazz) {
        return Flux.fromIterable(keys)
            .flatMap(key -> get(key, clazz)
                .map(value -> Map.entry(key, value)))
            .collectMap(Map.Entry::getKey, Map.Entry::getValue)
            .onErrorResume(error -> {
                log.error("批量获取缓存失败: error={}", error.getMessage());
                return Mono.just(new HashMap<>());
            });
    }
    
    /**
     * 批量设置缓存值 - 响应式版本
     */
    public Mono<Boolean> multiSet(Map<String, Object> map, Duration expireTime) {
        return Flux.fromIterable(map.entrySet())
            .flatMap(entry -> set(entry.getKey(), entry.getValue(), expireTime))
            .all(result -> result)
            .onErrorResume(error -> {
                log.error("批量设置缓存失败: error={}", error.getMessage());
                return Mono.just(false);
            });
    }
    
    /**
     * 关闭资源 - 响应式版本
     */
    @PreDestroy
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> log.info("ReactiveRedisManager资源已释放"));
    }
}

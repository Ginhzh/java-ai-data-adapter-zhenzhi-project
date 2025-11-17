package com.weichai.knowledge.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Redis缓存管理器
 * 提供统一的Redis操作接口，包括基础操作、消息缓存、处理队列等功能
 */
@Slf4j
@Component
public class RedisManager {
    
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // 默认过期时间（24小时）
    private static final long DEFAULT_EXPIRE_SECONDS = 86400L;
    
    // 缓存键前缀
    private static final String KAFKA_MESSAGE_PREFIX = "kafka:message:";
    private static final String KAFKA_STATUS_PREFIX = "kafka:status:";
    private static final String PROCESSING_QUEUE_KEY = "kafka:processing_queue";
    private static final String SYNC_LIST_SUFFIX = ":sync_list";
    
    @PostConstruct
    public void init() {
        log.info("Redis管理器初始化完成");
    }
    
    /**
     * 设置字符串值
     *
     * @param key    键
     * @param value  值
     * @param expire 过期时间（秒），null则使用默认值
     * @return 是否成功
     */
    public boolean set(String key, Object value, Long expire) {
        try {
            String jsonValue;
            if (value instanceof String) {
                jsonValue = (String) value;
            } else {
                jsonValue = objectMapper.writeValueAsString(value);
            }
            
            if (expire != null) {
                stringRedisTemplate.opsForValue().set(key, jsonValue, expire, TimeUnit.SECONDS);
            } else {
                stringRedisTemplate.opsForValue().set(key, jsonValue, DEFAULT_EXPIRE_SECONDS, TimeUnit.SECONDS);
            }
            return true;
        } catch (Exception e) {
            log.error("Redis set操作失败: key={}, error={}", key, e.getMessage());
            return false;
        }
    }
    
    /**
     * 设置字符串值（使用默认过期时间）
     */
    public boolean set(String key, Object value) {
        return set(key, value, null);
    }
    
    /**
     * 获取值
     *
     * @param key   键
     * @param clazz 值的类型
     * @return 值，不存在返回null
     */
    public <T> T get(String key, Class<T> clazz) {
        try {
            String value = stringRedisTemplate.opsForValue().get(key);
            if (value == null) {
                return null;
            }
            
            if (clazz == String.class) {
                return clazz.cast(value);
            }
            
            return objectMapper.readValue(value, clazz);
        } catch (Exception e) {
            log.error("Redis get操作失败: key={}, error={}", key, e.getMessage());
            return null;
        }
    }
    
    /**
     * 获取字符串值
     */
    public String get(String key) {
        return get(key, String.class);
    }
    
    /**
     * 获取Map类型的值
     */
    public Map<String, Object> getMap(String key) {
        try {
            String value = stringRedisTemplate.opsForValue().get(key);
            if (value == null) {
                return null;
            }
            
            return objectMapper.readValue(value, 
                TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class));
        } catch (Exception e) {
            log.error("Redis getMap操作失败: key={}, error={}", key, e.getMessage());
            return null;
        }
    }
    
    /**
     * 获取List类型的值
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String key, Class<T> elementClass) {
        try {
            String value = stringRedisTemplate.opsForValue().get(key);
            if (value == null) {
                return null;
            }
            
            if (elementClass == String.class) {
                return (List<T>) objectMapper.readValue(value, 
                    TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, String.class));
            }
            
            return objectMapper.readValue(value, 
                TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, elementClass));
        } catch (Exception e) {
            log.error("Redis getList操作失败: key={}, error={}", key, e.getMessage());
            return null;
        }
    }
    
    /**
     * 删除键
     */
    public boolean delete(String key) {
        try {
            return Boolean.TRUE.equals(stringRedisTemplate.delete(key));
        } catch (Exception e) {
            log.error("Redis delete操作失败: key={}, error={}", key, e.getMessage());
            return false;
        }
    }
    
    /**
     * 批量删除键
     */
    public long delete(Collection<String> keys) {
        try {
            Long count = stringRedisTemplate.delete(keys);
            return count != null ? count : 0;
        } catch (Exception e) {
            log.error("Redis批量delete操作失败: error={}", e.getMessage());
            return 0;
        }
    }
    
    /**
     * 检查键是否存在
     */
    public boolean exists(String key) {
        try {
            return Boolean.TRUE.equals(stringRedisTemplate.hasKey(key));
        } catch (Exception e) {
            log.error("Redis exists操作失败: key={}, error={}", key, e.getMessage());
            return false;
        }
    }
    
    /**
     * 设置过期时间
     */
    public boolean expire(String key, long seconds) {
        try {
            return Boolean.TRUE.equals(stringRedisTemplate.expire(key, seconds, TimeUnit.SECONDS));
        } catch (Exception e) {
            log.error("Redis expire操作失败: key={}, error={}", key, e.getMessage());
            return false;
        }
    }
    
    /**
     * 获取剩余过期时间
     *
     * @return 剩余秒数，-2表示键不存在，-1表示键存在但没有过期时间
     */
    public long ttl(String key) {
        try {
            Long ttl = stringRedisTemplate.getExpire(key, TimeUnit.SECONDS);
            return ttl != null ? ttl : -2;
        } catch (Exception e) {
            log.error("Redis ttl操作失败: key={}, error={}", key, e.getMessage());
            return -2;
        }
    }
    
    /**
     * 设置Hash字段
     */
    public boolean hset(String key, String field, Object value) {
        try {
            String jsonValue = value instanceof String ? (String) value : objectMapper.writeValueAsString(value);
            stringRedisTemplate.opsForHash().put(key, field, jsonValue);
            return true;
        } catch (Exception e) {
            log.error("Redis hset操作失败: key={}, field={}, error={}", key, field, e.getMessage());
            return false;
        }
    }
    
    /**
     * 批量设置Hash字段
     */
    public boolean hmset(String key, Map<String, Object> map) {
        try {
            Map<String, String> stringMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                Object value = entry.getValue();
                String jsonValue = value instanceof String ? (String) value : objectMapper.writeValueAsString(value);
                stringMap.put(entry.getKey(), jsonValue);
            }
            stringRedisTemplate.opsForHash().putAll(key, stringMap);
            return true;
        } catch (Exception e) {
            log.error("Redis hmset操作失败: key={}, error={}", key, e.getMessage());
            return false;
        }
    }
    
    /**
     * 获取Hash字段值
     */
    public Object hget(String key, String field) {
        try {
            Object value = stringRedisTemplate.opsForHash().get(key, field);
            if (value == null) {
                return null;
            }
            
            String strValue = value.toString();
            // 尝试解析JSON
            if ((strValue.startsWith("{") || strValue.startsWith("["))) {
                try {
                    return objectMapper.readValue(strValue, Object.class);
                } catch (Exception ignored) {
                    return strValue;
                }
            }
            return strValue;
        } catch (Exception e) {
            log.error("Redis hget操作失败: key={}, field={}, error={}", key, field, e.getMessage());
            return null;
        }
    }
    
    /**
     * 获取Hash所有字段
     */
    public Map<String, Object> hgetall(String key) {
        try {
            Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(key);
            Map<String, Object> result = new HashMap<>();
            
            for (Map.Entry<Object, Object> entry : entries.entrySet()) {
                String field = entry.getKey().toString();
                String value = entry.getValue().toString();
                
                // 尝试解析JSON
                if ((value.startsWith("{") || value.startsWith("["))) {
                    try {
                        result.put(field, objectMapper.readValue(value, Object.class));
                    } catch (Exception ignored) {
                        result.put(field, value);
                    }
                } else {
                    result.put(field, value);
                }
            }
            
            return result;
        } catch (Exception e) {
            log.error("Redis hgetall操作失败: key={}, error={}", key, e.getMessage());
            return new HashMap<>();
        }
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
     * 缓存消息数据
     */
    public boolean cacheMessage(String messageId, Map<String, Object> messageData, int expireSeconds) {
        String key = KAFKA_MESSAGE_PREFIX + messageId;
        boolean result = set(key, messageData, (long) expireSeconds);
        if (result) {
            log.info("消息已缓存到Redis: {}", key);
        }
        return result;
    }
    
    /**
     * 缓存消息数据（默认1小时过期）
     */
    public boolean cacheMessage(String messageId, Map<String, Object> messageData) {
        return cacheMessage(messageId, messageData, 3600);
    }
    
    /**
     * 获取缓存的消息数据
     */
    public Map<String, Object> getMessage(String messageId) {
        String key = KAFKA_MESSAGE_PREFIX + messageId;
        return getMap(key);
    }
    
    /**
     * 删除缓存的消息数据
     */
    public boolean removeMessage(String messageId) {
        String key = KAFKA_MESSAGE_PREFIX + messageId;
        boolean result = delete(key);
        if (result) {
            log.info("已从Redis删除消息: {}", key);
        }
        return result;
    }
    
    // ========== 处理队列相关方法 ==========
    
    /**
     * 添加到处理队列
     *
     * @param messageId 消息ID
     * @param priority  优先级，值越小优先级越高
     */
    public boolean addToProcessingQueue(String messageId, double priority) {
        try {
            Boolean result = stringRedisTemplate.opsForZSet().add(PROCESSING_QUEUE_KEY, messageId, priority);
            if (Boolean.TRUE.equals(result)) {
                log.info("消息已添加到处理队列: {}, 优先级: {}", messageId, priority);
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("添加消息到处理队列失败: messageId={}, error={}", messageId, e.getMessage());
            return false;
        }
    }
    
    /**
     * 获取处理队列中的消息ID列表
     */
    public List<String> getProcessingQueue(int count) {
        try {
            Set<String> messageIds = stringRedisTemplate.opsForZSet().range(PROCESSING_QUEUE_KEY, 0, count - 1);
            return messageIds != null ? new ArrayList<>(messageIds) : new ArrayList<>();
        } catch (Exception e) {
            log.error("获取处理队列失败: error={}", e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * 从处理队列中移除消息
     */
    public boolean removeFromProcessingQueue(String messageId) {
        try {
            Long removed = stringRedisTemplate.opsForZSet().remove(PROCESSING_QUEUE_KEY, messageId);
            if (removed != null && removed > 0) {
                log.info("已从处理队列移除消息: {}", messageId);
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("从处理队列移除消息失败: messageId={}, error={}", messageId, e.getMessage());
            return false;
        }
    }
    
    /**
     * 设置消息处理状态
     */
    public boolean setProcessingStatus(String messageId, String status) {
        String key = KAFKA_STATUS_PREFIX + messageId;
        boolean result = set(key, status, 3600L); // 状态信息保留1小时
        if (result) {
            log.info("已设置消息处理状态: {}, 状态: {}", messageId, status);
        }
        return result;
    }
    
    /**
     * 获取消息处理状态
     */
    public String getProcessingStatus(String messageId) {
        String key = KAFKA_STATUS_PREFIX + messageId;
        return get(key);
    }
    
    // ========== 数据同步相关方法 ==========
    
    /**
     * 标记记录需要同步到数据库
     *
     * @param redisKeyPrefix Redis键前缀
     * @param recordId       记录ID
     */
    public void markForSync(String redisKeyPrefix, String recordId) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        List<String> syncList = getList(syncKey, String.class);
        if (syncList == null) {
            syncList = new ArrayList<>();
        }
        
        if (!syncList.contains(recordId)) {
            syncList.add(recordId);
            set(syncKey, syncList);
            log.debug("记录 {} 已标记为需要同步", recordId);
        }
    }
    
    /**
     * 获取需要同步的记录ID列表
     */
    public List<String> getSyncList(String redisKeyPrefix) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        List<String> syncList = getList(syncKey, String.class);
        return syncList != null ? syncList : new ArrayList<>();
    }
    
    /**
     * 从同步列表中移除记录
     */
    public void removeFromSyncList(String redisKeyPrefix, String recordId) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        List<String> syncList = getList(syncKey, String.class);
        if (syncList != null && syncList.contains(recordId)) {
            syncList.remove(recordId);
            if (syncList.isEmpty()) {
                delete(syncKey);
            } else {
                set(syncKey, syncList);
            }
        }
    }
    
    /**
     * 清空同步列表
     */
    public void clearSyncList(String redisKeyPrefix) {
        String syncKey = redisKeyPrefix + SYNC_LIST_SUFFIX;
        delete(syncKey);
    }
    
    /**
     * 获取或设置缓存值
     * 如果缓存存在则返回，不存在则通过getter获取并设置缓存
     */
    public <T> T getOrSet(String key, Class<T> clazz, CacheGetter<T> getter, long expireSeconds) {
        T cachedValue = get(key, clazz);
        if (cachedValue != null) {
            log.debug("从缓存获取数据: {}", key);
            return cachedValue;
        }
        
        T value = getter.get();
        if (value != null) {
            set(key, value, expireSeconds);
            log.debug("将数据存入缓存: {}", key);
        }
        
        return value;
    }
    
    /**
     * 缓存获取器函数式接口
     */
    @FunctionalInterface
    public interface CacheGetter<T> {
        T get();
    }
    
    /**
     * 批量获取缓存值
     */
    public <T> Map<String, T> multiGet(Collection<String> keys, Class<T> clazz) {
        Map<String, T> result = new HashMap<>();
        if (keys == null || keys.isEmpty()) {
            return result;
        }
        
        try {
            List<String> values = stringRedisTemplate.opsForValue().multiGet(keys);
            if (values == null) {
                return result;
            }
            
            int index = 0;
            for (String key : keys) {
                String value = values.get(index++);
                if (value != null) {
                    if (clazz == String.class) {
                        result.put(key, clazz.cast(value));
                    } else {
                        try {
                            result.put(key, objectMapper.readValue(value, clazz));
                        } catch (Exception e) {
                            log.warn("解析缓存值失败: key={}, error={}", key, e.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("批量获取缓存失败: error={}", e.getMessage());
        }
        
        return result;
    }
    
    /**
     * 批量设置缓存值
     */
    public <T> boolean multiSet(Map<String, T> map, long expireSeconds) {
        if (map == null || map.isEmpty()) {
            return true;
        }
        
        try {
            Map<String, String> stringMap = new HashMap<>();
            for (Map.Entry<String, T> entry : map.entrySet()) {
                String key = entry.getKey();
                T value = entry.getValue();
                if (value instanceof String) {
                    stringMap.put(key, (String) value);
                } else {
                    stringMap.put(key, objectMapper.writeValueAsString(value));
                }
            }
            
            stringRedisTemplate.opsForValue().multiSet(stringMap);
            
            // 设置过期时间
            for (String key : map.keySet()) {
                expire(key, expireSeconds);
            }
            
            return true;
        } catch (Exception e) {
            log.error("批量设置缓存失败: error={}", e.getMessage());
            return false;
        }
    }
}
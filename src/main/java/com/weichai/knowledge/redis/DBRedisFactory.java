package com.weichai.knowledge.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.annotation.Id;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 数据库Redis工厂类
 * 提供透明的缓存层，先从Redis获取数据，不存在则从数据库获取并缓存
 * 
 * @param <T> 实体类型
 */
@Slf4j
@Component
public class DBRedisFactory<T> {
    
    @Autowired
    private RedisManager redisManager;
    
    @Autowired
    private SyncManager syncManager;
    
    @Autowired
    private R2dbcEntityTemplate r2dbcEntityTemplate;
    
    private Class<T> modelClass;
    private String redisPrefix;
    private String primaryKeyName;
    private Field primaryKeyField;
    private long defaultExpire = 86400L; // 默认24小时
    
    /**
     * 创建工厂实例
     *
     * @param modelClass   实体类
     * @param redisPrefix  Redis键前缀，如果为null则使用表名
     * @return 工厂实例
     */
    public static <T> DBRedisFactory<T> create(Class<T> modelClass, String redisPrefix) {
        DBRedisFactory<T> factory = new DBRedisFactory<>();
        factory.initialize(modelClass, redisPrefix);
        return factory;
    }
    
    /**
     * 初始化工厂
     */
    private void initialize(Class<T> modelClass, String redisPrefix) {
        this.modelClass = modelClass;
        
        // 获取表名作为默认前缀
        if (redisPrefix == null) {
            Table tableAnnotation = modelClass.getAnnotation(Table.class);
            this.redisPrefix = tableAnnotation != null ? tableAnnotation.value() : modelClass.getSimpleName().toLowerCase();
        } else {
            this.redisPrefix = redisPrefix;
        }
        
        // 查找主键字段
        findPrimaryKey();
        
        // 注册同步任务
        syncManager.registerSyncTask(
            "sync_" + this.redisPrefix,
            modelClass,
            this.redisPrefix
        );
    }
    
    /**
     * 查找主键字段
     */
    private void findPrimaryKey() {
        Class<?> currentClass = modelClass;
        while (currentClass != null) {
            for (Field field : currentClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(Id.class)) {
                    this.primaryKeyField = field;
                    this.primaryKeyField.setAccessible(true);
                    this.primaryKeyName = field.getName();
                    return;
                }
            }
            currentClass = currentClass.getSuperclass();
        }
        throw new IllegalArgumentException("实体类没有@Id注解的主键字段: " + modelClass.getName());
    }
    
    /**
     * 将实体转换为Map
     */
    private Map<String, Object> entityToMap(T entity) {
        if (entity == null) {
            return null;
        }
        
        Map<String, Object> map = new HashMap<>();
        Class<?> currentClass = entity.getClass();
        
        while (currentClass != null) {
            for (Field field : currentClass.getDeclaredFields()) {
                try {
                    field.setAccessible(true);
                    Object value = field.get(entity);
                    if (value != null) {
                        // 处理特殊类型
                        if (value instanceof LocalDateTime) {
                            map.put(field.getName(), value.toString());
                        } else if (value instanceof Date) {
                            map.put(field.getName(), ((Date) value).getTime());
                        } else {
                            map.put(field.getName(), value);
                        }
                    }
                } catch (IllegalAccessException e) {
                    log.warn("无法访问字段: {}", field.getName());
                }
            }
            currentClass = currentClass.getSuperclass();
        }
        
        return map;
    }
    
    /**
     * 生成Redis键
     */
    private String generateKey(Object id) {
        return redisManager.generateKey(redisPrefix, id.toString());
    }
    
    /**
     * 根据ID获取实体
     *
     * @param id 主键值
     * @return 实体Map，不存在则返回null
     */
    public Map<String, Object> getById(Object id) {
        if (id == null) {
            return null;
        }
        
        // 生成Redis键
        String redisKey = generateKey(id);
        
        // 先从Redis获取
        Map<String, Object> cachedData = redisManager.getMap(redisKey);
        if (cachedData != null && !cachedData.isEmpty()) {
            log.debug("从Redis获取记录: {}", redisKey);
            return cachedData;
        }
        
        // Redis没有则从数据库获取
        log.debug("从数据库获取记录: {}", id);
        try {
            T entity = r2dbcEntityTemplate.selectOne(
                org.springframework.data.relational.core.query.Query.query(
                    org.springframework.data.relational.core.query.Criteria.where(primaryKeyName).is(id)
                ), 
                modelClass
            ).block(); // 阻塞等待结果
            
            if (entity != null) {
                // 转换为Map并缓存到Redis
                Map<String, Object> entityMap = entityToMap(entity);
                redisManager.set(redisKey, entityMap, defaultExpire);
                return entityMap;
            }
        } catch (Exception e) {
            log.error("从数据库查询记录失败: {}", e.getMessage(), e);
        }
        
        return null;
    }
    
    /**
     * 根据ID获取实体对象
     *
     * @param id 主键值
     * @return 实体对象，不存在则返回null
     */
    @Transactional(readOnly = true)
    public T getEntityById(Object id) {
        if (id == null) {
            return null;
        }
        
        // 先尝试从缓存获取
        Map<String, Object> data = getById(id);
        if (data != null) {
            // 从数据库重新加载以获得完整的实体对象
            try {
                return r2dbcEntityTemplate.selectOne(
                    org.springframework.data.relational.core.query.Query.query(
                        org.springframework.data.relational.core.query.Criteria.where(primaryKeyName).is(id)
                    ), 
                    modelClass
                ).block();
            } catch (Exception e) {
                log.error("从数据库加载实体对象失败: {}", e.getMessage(), e);
            }
        }
        
        return null;
    }
    
    /**
     * 根据条件查询实体列表（简化版本，不支持复杂查询）
     *
     * @return 实体Map列表
     */
    @Transactional(readOnly = true)
    public List<Map<String, Object>> getAll() {
        try {
            List<T> entities = r2dbcEntityTemplate.select(modelClass).all().collectList().block();
            
            // 转换为Map列表并缓存
            List<Map<String, Object>> result = new ArrayList<>();
            if (entities != null) {
                for (T entity : entities) {
                    Map<String, Object> entityMap = entityToMap(entity);
                    if (entityMap != null) {
                        // 缓存到Redis
                        try {
                            Object id = primaryKeyField.get(entity);
                            String redisKey = generateKey(id);
                            redisManager.set(redisKey, entityMap, defaultExpire);
                        } catch (IllegalAccessException e) {
                            log.warn("无法获取主键值进行缓存");
                        }
                        result.add(entityMap);
                    }
                }
            }
            
            return result;
        } catch (Exception e) {
            log.error("查询所有实体失败: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 创建实体
     *
     * @param data 实体数据
     * @return 创建后的实体Map
     */
    public Map<String, Object> create(Map<String, Object> data) {
        // 如果没有提供ID，则生成UUID
        if (!data.containsKey(primaryKeyName) || data.get(primaryKeyName) == null) {
            data.put(primaryKeyName, UUID.randomUUID().toString());
        }
        
        // 添加创建时间
        if (!data.containsKey("createdAt")) {
            data.put("createdAt", LocalDateTime.now().toString());
        }
        
        // 存储到Redis
        String redisKey = generateKey(data.get(primaryKeyName));
        redisManager.set(redisKey, data, defaultExpire);
        
        // 标记为需要同步到数据库
        redisManager.markForSync(redisPrefix, data.get(primaryKeyName).toString());
        
        log.debug("创建记录: {}", redisKey);
        return data;
    }
    
    /**
     * 更新实体
     *
     * @param id   主键值
     * @param data 更新数据
     * @return 更新后的实体Map，如果实体不存在则返回null
     */
    public Map<String, Object> update(Object id, Map<String, Object> data) {
        if (id == null) {
            return null;
        }
        
        // 先检查实体是否存在
        Map<String, Object> existingData = getById(id);
        if (existingData == null) {
            return null;
        }
        
        // 合并数据
        existingData.putAll(data);
        
        // 确保ID不变
        existingData.put(primaryKeyName, id);
        
        // 更新时间戳
        existingData.put("updatedAt", LocalDateTime.now().toString());
        
        // 更新Redis
        String redisKey = generateKey(id);
        redisManager.set(redisKey, existingData, defaultExpire);
        
        // 标记为需要同步到数据库
        redisManager.markForSync(redisPrefix, id.toString());
        
        log.debug("更新记录: {}", redisKey);
        return existingData;
    }
    
    /**
     * 删除实体
     *
     * @param id 主键值
     * @return 是否成功删除
     */
    @Transactional
    public boolean delete(Object id) {
        if (id == null) {
            return false;
        }
        
        // 先检查实体是否存在
        Map<String, Object> existingData = getById(id);
        if (existingData == null) {
            return false;
        }
        
        // 从Redis删除
        String redisKey = generateKey(id);
        redisManager.delete(redisKey);
        
        // 从数据库删除（直接执行，不使用同步机制）
        try {
            Long deletedRowsLong = r2dbcEntityTemplate.delete(modelClass)
                .matching(org.springframework.data.relational.core.query.Query.query(
                    org.springframework.data.relational.core.query.Criteria.where(primaryKeyName).is(id)
                ))
                .all()
                .block();

            int deletedRows = deletedRowsLong != null ? deletedRowsLong.intValue() : 0;
            
            if (deletedRows > 0) {
                log.debug("删除记录: {}", id);
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("删除记录失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 批量获取
     *
     * @param ids ID列表
     * @return 实体Map列表
     */
    public List<Map<String, Object>> getByIds(Collection<?> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<Map<String, Object>> result = new ArrayList<>();
        List<Object> missedIds = new ArrayList<>();
        
        // 先从Redis批量获取
        for (Object id : ids) {
            String redisKey = generateKey(id);
            Map<String, Object> data = redisManager.getMap(redisKey);
            if (data != null && !data.isEmpty()) {
                result.add(data);
            } else {
                missedIds.add(id);
            }
        }
        
        // 从数据库获取缓存未命中的数据
        if (!missedIds.isEmpty()) {
            try {
                List<T> entities = r2dbcEntityTemplate.select(modelClass)
                    .matching(org.springframework.data.relational.core.query.Query.query(
                        org.springframework.data.relational.core.query.Criteria.where(primaryKeyName).in(missedIds)
                    ))
                    .all()
                    .collectList()
                    .block();
                
                if (entities != null) {
                    for (T entity : entities) {
                        Map<String, Object> entityMap = entityToMap(entity);
                        if (entityMap != null) {
                            result.add(entityMap);
                            // 缓存到Redis
                            try {
                                Object id = primaryKeyField.get(entity);
                                String redisKey = generateKey(id);
                                redisManager.set(redisKey, entityMap, defaultExpire);
                            } catch (IllegalAccessException e) {
                                log.warn("无法获取主键值进行缓存");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("批量查询数据库失败: {}", e.getMessage(), e);
            }
        }
        
        return result;
    }
    
    /**
     * 立即同步到数据库
     *
     * @return 是否成功同步
     */
    public boolean syncToDb() {
        try {
            syncManager.forceSyncNow();
            return true;
        } catch (Exception e) {
            log.error("同步数据到数据库失败: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 清除缓存
     *
     * @param id 主键值
     */
    public void clearCache(Object id) {
        if (id != null) {
            String redisKey = generateKey(id);
            redisManager.delete(redisKey);
        }
    }
    
    /**
     * 清除所有缓存
     */
    public void clearAllCache() {
        // 获取所有相关的键并删除
        // 注意：这个操作可能会影响性能，在生产环境中谨慎使用
        log.warn("清除所有 {} 相关的缓存", redisPrefix);
    }
    
    /**
     * 设置默认过期时间
     *
     * @param seconds 过期时间（秒）
     */
    public void setDefaultExpire(long seconds) {
        this.defaultExpire = seconds;
    }
}
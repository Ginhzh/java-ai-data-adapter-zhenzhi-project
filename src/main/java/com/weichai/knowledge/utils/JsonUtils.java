// JsonUtils.java
package com.weichai.knowledge.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class JsonUtils {
    
    private final ObjectMapper objectMapper;
    
    /**
     * 将对象转换为JSON字符串
     */
    public String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("序列化JSON失败: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 将JSON字符串转换为对象
     */
    public <T> T parseJson(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            log.error("解析JSON失败: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 将JSON字符串转换为Map
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> parseJsonToMap(String json) {
        return parseJson(json, Map.class);
    }
    
    /**
     * 将JSON字符串转换为List
     */
    public <T> List<T> parseJsonToList(String json, Class<T> elementClass) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        
        try {
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            return objectMapper.readValue(json, 
                    typeFactory.constructCollectionType(List.class, elementClass));
        } catch (IOException e) {
            log.error("解析JSON数组失败: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 安全地解析JSON，返回默认值
     */
    public <T> T parseJsonSafe(String json, Class<T> clazz, T defaultValue) {
        T result = parseJson(json, clazz);
        return result != null ? result : defaultValue;
    }
}
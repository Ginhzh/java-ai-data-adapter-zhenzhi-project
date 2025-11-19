package com.weichai.knowledge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weichai.knowledge.entity.RoleUserMessage;
import com.weichai.knowledge.utils.ErrorHandler;
import com.weichai.knowledge.config.ApplicationProperties;
import com.weichai.knowledge.redis.ReactiveRedisManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.Arrays;

import java.util.stream.Collectors;

/**
 * 虚拟用户组处理器
 * 负责处理虚拟用户组的创建、删除、查询以及用户绑定/解绑操作
 */
@Slf4j
@Component
public class RoleUserHandler {
    
    // 错误类型常量 - 对应Python中的错误类型
    private static final int ERROR_TYPE_QUERY_VIRTUAL_GROUP = 10;     // 查询虚拟群组
    private static final int ERROR_TYPE_CREATE_VIRTUAL_GROUP = 11;    // 创建虚拟群组
    private static final int ERROR_TYPE_DELETE_VIRTUAL_GROUP = 12;    // 删除虚拟群组
    private static final int ERROR_TYPE_BIND_USER = 13;               // 绑定用户到虚拟群组
    private static final int ERROR_TYPE_UNBIND_USER = 14;             // 解绑用户到虚拟群组
    
    private static final int MAX_RETRIES = 5;
    private static final int BATCH_SIZE = 100;
    private static final Duration RETRY_DELAY = Duration.ofSeconds(2);
    private static final String DEFAULT_TENANT_ID = "85d5572986784a53a649726085691974";

    private static final String ADD_USER_VERIFICATION_FMT = "%s:role_user:add_user:verification:passed:%s";
    private static final String ADD_USER_SUCCESS_FMT = "%s:role_user:add_user:operation:success:%s";
    private static final String DEL_USER_VERIFICATION_FMT = "%s:role_user:del_user:verification:passed:%s";
    private static final String DEL_USER_SUCCESS_FMT = "%s:role_user:del_user:operation:success:%s";
    private static final String HASH_MESSAGE_FIELD = "message_count";
    private static final String HASH_USER_FIELD = "user_count";
    
    private final WebClient webClient;
    private final ErrorHandler errorHandler;
    private final ObjectMapper objectMapper;
    private final ApplicationProperties applicationProperties;
    private final ReactiveRedisManager reactiveRedisManager;
    
    public RoleUserHandler(WebClient.Builder webClientBuilder, 
                          ErrorHandler errorHandler,
                          ObjectMapper objectMapper,
                          ApplicationProperties applicationProperties,
                          ReactiveRedisManager reactiveRedisManager) {
        this.errorHandler = errorHandler;
        this.objectMapper = objectMapper;
        this.applicationProperties = applicationProperties;
        this.reactiveRedisManager = reactiveRedisManager;
        
        // 配置WebClient
        this.webClient = webClientBuilder
            .baseUrl(applicationProperties.getApi().getBaseUrl())
            .defaultHeader("Content-Type", "application/json")
            .defaultHeader("superhelper-login-state", "matchApi")
            .defaultHeader("x-match-api-signature", applicationProperties.getApi().getMatchSignature())
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
            .build();
    }
    
    /**
     * 处理角色用户消息
     */
    public Mono<Boolean> handleMessage(RoleUserMessage message) {
        return Mono.fromRunnable(() -> validateInput(message))
            .thenReturn(message)
            .flatMap(validatedMessage -> {
                // 根据消息类型处理
                return switch (validatedMessage.getMessageType()) {
                    case "ADD_ROLE" -> handleCreate(validatedMessage);
                    case "DEL_ROLE" -> handleDelete(validatedMessage);
                    case "ROLE_ADD_USER", "ROLE_DEL_USER" -> handleUpdate(validatedMessage);
                    default -> Mono.just(false);
                };
            });
    }
    
    /**
     * 处理创建虚拟用户组
     */
    private Mono<Boolean> handleCreate(RoleUserMessage message) {
        return queryVirtualGroup(message.getRoleId(), message)
            .flatMap(existingGroupId -> {
                if (existingGroupId != null) {
                    log.info("虚拟用户组 {} 已存在", existingGroupId);
                    return Mono.just(true);
                }
                return createVirtualGroup(message);
            })
            .flatMap(success -> {
                if (success && message.getAddUserList() != null && !message.getAddUserList().isEmpty()) {
                    return bindUsersToVirtualGroup(message.getRoleId(), message.getAddUserList(), message);
                }
                return Mono.just(success);
            });
    }
    
    /**
     * 处理删除虚拟用户组
     */
    private Mono<Boolean> handleDelete(RoleUserMessage message) {
        return deleteVirtualGroup(message);
    }
    
    /**
     * 处理更新虚拟用户组
     */
    private Mono<Boolean> handleUpdate(RoleUserMessage message) {
        List<Mono<Boolean>> operations = new ArrayList<>();
        
        // 处理用户绑定 (ROLE_ADD_USER)
        if ("ROLE_ADD_USER".equals(message.getMessageType())) {
            List<String> addUsers = message.getAddUserList() != null ? message.getAddUserList() : Collections.emptyList();
            int userInc = addUsers.size();
            Mono<Boolean> op = incrRoleUserCounters(addUserVerificationKey(message.getSystemName()), userInc,
                    message.getSystemName(), "ROLE_ADD_USER verification")
                .then(Mono.defer(() -> {
                    if (addUsers.isEmpty()) {
                        log.info("ROLE_ADD_USER: 无需绑定用户，roleId={}", message.getRoleId());
                        return Mono.just(true);
                    }
                    return bindUsersToVirtualGroup(message.getRoleId(), addUsers, message)
                        .flatMap(success -> {
                            if (Boolean.TRUE.equals(success)) {
                                return incrRoleUserCounters(addUserSuccessKey(message.getSystemName()), userInc,
                                        message.getSystemName(), "ROLE_ADD_USER success")
                                    .thenReturn(true);
                            }
                            return Mono.just(false);
                        });
                }));
            operations.add(op);
        }

        // 处理用户解绑 (ROLE_DEL_USER)
        if ("ROLE_DEL_USER".equals(message.getMessageType())) {
            List<String> delUsers = message.getDelUserList() != null ? message.getDelUserList() : Collections.emptyList();
            int userInc = delUsers.size();
            Mono<Boolean> op = incrRoleUserCounters(delUserVerificationKey(message.getSystemName()), userInc,
                    message.getSystemName(), "ROLE_DEL_USER verification")
                .then(Mono.defer(() -> {
                    if (delUsers.isEmpty()) {
                        log.info("ROLE_DEL_USER: 无需解绑用户，roleId={}", message.getRoleId());
                        return Mono.just(true);
                    }
                    return unbindUsersFromVirtualGroup(message.getRoleId(), delUsers, message)
                        .flatMap(success -> {
                            if (Boolean.TRUE.equals(success)) {
                                return incrRoleUserCounters(delUserSuccessKey(message.getSystemName()), userInc,
                                        message.getSystemName(), "ROLE_DEL_USER success")
                                    .thenReturn(true);
                            }
                            return Mono.just(false);
                        });
                }));
            operations.add(op);
        }
        
        if (operations.isEmpty()) {
            log.info("没有需要处理的用户操作");
            return Mono.just(true);
        }
        
        return Mono.when(operations)
            .then(Mono.just(true));
    }
    
    /**
     * 查询虚拟用户组
     */
    private Mono<String> queryVirtualGroup(String roleId, RoleUserMessage message) {
        Map<String, Object> requestBody = Map.of(
            "tenantId", DEFAULT_TENANT_ID,
            "virtualGroupIds", List.of(roleId)
        );

        return makeRequest(
            "/custom/openapi/v1/virtualGroup/get",
            requestBody,
            ERROR_TYPE_QUERY_VIRTUAL_GROUP,
            "query_virtual_group",
            message
        ).map(response -> {
            if (response == null) return null;

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> data = (List<Map<String, Object>>) response.get("data");
            if (data == null || data.isEmpty()) {
                log.info("{} 对应的虚拟用户组不存在", roleId);
                return null;
            }

            Map<String, Object> groupInfo = data.get(0);
            Boolean deleted = (Boolean) groupInfo.get("deleted");
            if (Boolean.TRUE.equals(deleted)) {
                log.info("{} 对应的虚拟用户组已删除", roleId);
                return null;
            }

            String virtualGroupId = (String) groupInfo.get("virtualGroupId");
            log.info("虚拟用户组 {} 存在", virtualGroupId);
            return virtualGroupId;
        });
    }

    
    /**
     * 创建虚拟用户组
     */
    private Mono<Boolean> createVirtualGroup(RoleUserMessage message) {
        Map<String, Object> virtualGroup = Map.of(
            "virtualGroupId", message.getRoleId(),
            "tenantId", DEFAULT_TENANT_ID,
            "virtualGroupName", message.getRoleName(),
            "deleted", 0,
            "system", message.getSystemName()
        );
        
        Map<String, Object> requestBody = Map.of(
            "openVirtualGroupDTOs", List.of(virtualGroup)
        );
        
        return makeRequest(
            "/custom/openapi/v1/virtualGroup/sync",
            requestBody,
            ERROR_TYPE_CREATE_VIRTUAL_GROUP,
            "create_virtual_group",
            message
        ).map(response -> {
            if (response != null) {
                log.info("创建虚拟用户组 {} 成功!", message.getRoleId());
                return true;
            }
            return false;
        });
    }
    
    /**
     * 删除虚拟用户组
     */
    private Mono<Boolean> deleteVirtualGroup(RoleUserMessage message) {
        Map<String, Object> virtualGroup = Map.of(
            "virtualGroupId", message.getRoleId(),
            "tenantId", DEFAULT_TENANT_ID,
            "virtualGroupName", message.getRoleName(),
            "deleted", 1,
            "system", message.getSystemName()
        );
        
        Map<String, Object> requestBody = Map.of(
            "openVirtualGroupDTOs", List.of(virtualGroup)
        );
        
        return makeRequest(
            "/custom/openapi/v1/virtualGroup/sync",
            requestBody,
            ERROR_TYPE_DELETE_VIRTUAL_GROUP,
            "delete_virtual_group",
            message
        ).map(response -> {
            if (response != null) {
                log.info("删除虚拟用户组 {} 成功!", message.getRoleId());
                return true;
            }
            return false;
        });
    }
    
    /**
     * 批量绑定用户到虚拟用户组
     */
    private Mono<Boolean> bindUsersToVirtualGroup(String virtualGroupId, 
                                                               List<String> userIds, 
                                                               RoleUserMessage message) {
        return processBatchOperation(
            virtualGroupId,
            userIds,
            "CREATE",
            ERROR_TYPE_BIND_USER,
            "bind_user",
            message
        );
    }
    
    /**
     * 批量解绑用户从虚拟用户组
     */
    private Mono<Boolean> unbindUsersFromVirtualGroup(String virtualGroupId, 
                                                                   List<String> userIds, 
                                                                   RoleUserMessage message) {
        return processBatchOperation(
            virtualGroupId,
            userIds,
            "DELETE",
            ERROR_TYPE_UNBIND_USER,
            "unbind_user",
            message
        );
    }
    
    /**
     * 处理批量操作（绑定/解绑）
     */
    private Mono<Boolean> processBatchOperation(String virtualGroupId,
                                                            List<String> userIds,
                                                            String method,
                                                            int errorType,
                                                            String step,
                                                            RoleUserMessage message) {
        if (userIds == null || userIds.isEmpty()) {
            log.info("没有需要处理的用户");
            return Mono.just(true);
        }
        
        int totalUsers = userIds.size();
        List<Mono<Boolean>> batchMonos = new ArrayList<>();
        
        // 分批处理
        for (int i = 0; i < totalUsers; i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, totalUsers);
            List<String> batchUsers = userIds.subList(i, endIndex);
            int batchNum = i / BATCH_SIZE + 1;
            int totalBatches = (totalUsers + BATCH_SIZE - 1) / BATCH_SIZE;
            
            log.info("处理第 {}/{} 批用户，本批数量: {}", batchNum, totalBatches, batchUsers.size());
            
            List<Map<String, String>> relations = batchUsers.stream()
                .map(userId -> Map.of(
                    "userOpenId", userId,
                    "tenantId", DEFAULT_TENANT_ID,
                    "virtualGroupId", virtualGroupId
                ))
                .collect(Collectors.toList());
            
            Map<String, Object> requestBody = Map.of(
                "method", method,
                "openVirtualGroupRelationDTOs", relations
            );
            
            Mono<Boolean> batchMono = makeRequest(
                "/custom/openapi/v1/virtualGroup/relation",
                requestBody,
                errorType,
                step + "_batch_" + batchNum,
                message
            ).map(response -> {
                if (response != null) {
                    String action = method.equals("CREATE") ? "绑定" : "解绑";
                    batchUsers.forEach(userId -> 
                        log.info("用户 {} {} 虚拟用户组 {} 成功！", userId, action, virtualGroupId)
                    );
                    return true;
                }
                log.error("第 {} 批用户处理失败", batchNum);
                return false;
            });
            
            batchMonos.add(batchMono);
        }
        
        return Mono.zip(batchMonos, results -> 
            Arrays.stream(results).allMatch(result -> Boolean.TRUE.equals(result))
        );
    }
    
    /**
     * 通用的HTTP请求方法（带重试）
     */
    @SuppressWarnings("unchecked")
    private Mono<Map<String, Object>> makeRequest(String endpoint,
                                                              Object requestBody,
                                                              int errorType,
                                                              String step,
                                                              RoleUserMessage message) {
        return webClient.post()
            .uri(endpoint)
            .bodyValue(requestBody)
            .retrieve()
            .bodyToMono(Map.class)
            .retryWhen(Retry.fixedDelay(MAX_RETRIES, RETRY_DELAY)
                .doBeforeRetry(retrySignal -> 
                    log.info("{} 失败，重试次数: {}/{}", step, retrySignal.totalRetries() + 1, MAX_RETRIES)
                )
            )
            .doOnSuccess(response -> {
                if (!isSuccessResponse(response)) {
                    throw new RuntimeException("API调用失败: " + response.get("message"));
                }
            })
            .onErrorResume(error -> {
                logError(errorType, step, message, error);
                return Mono.empty();
            })
            .map(response -> isSuccessResponse(response) ? response : null);
    }
    
    /**
     * 验证输入参数
     */
    private void validateInput(RoleUserMessage message) {
        if (message.getRoleId() == null || message.getRoleId().trim().isEmpty()) {
            throw new IllegalArgumentException("roleId 不能为空");
        }
        
        if (message.getRoleName() == null || message.getRoleName().trim().isEmpty()) {
            throw new IllegalArgumentException("roleName 不能为空");
        }
        
        Set<String> validTypes = Set.of("ADD_ROLE", "DEL_ROLE", "ROLE_ADD_USER", "ROLE_DEL_USER");
        if (!validTypes.contains(message.getMessageType())) {
            throw new IllegalArgumentException("不支持的消息类型: " + message.getMessageType());
        }
    }
    
    /**
     * 检查响应是否成功
     */
    private boolean isSuccessResponse(Map<String, Object> response) {
        return response != null && Boolean.TRUE.equals(response.get("success"));
    }

    /**
     * 记录错误日志
     */
    private void logError(int errorType, String step, RoleUserMessage message, Throwable error) {
        String errorMsg = String.format("%s 失败，已重试 %d 次", step, MAX_RETRIES);
        Map<String, Object> params = Map.of(
            "task_id", message.getMessageTaskId(),
            "message_type", message.getMessageType(),
            "role_id", message.getRoleId(),
            "system_name", message.getSystemName(),
            "error_data", error.getMessage()
        );

        errorHandler.logError(
            errorType,
            message.getRoleId(),
            step,
            errorMsg,
            params
        );
    }

    private String today() {
        return LocalDate.now().toString();
    }

    private String addUserVerificationKey(String systemName) {
        return String.format(ADD_USER_VERIFICATION_FMT, systemName, today());
    }

    private String addUserSuccessKey(String systemName) {
        return String.format(ADD_USER_SUCCESS_FMT, systemName, today());
    }

    private String delUserVerificationKey(String systemName) {
        return String.format(DEL_USER_VERIFICATION_FMT, systemName, today());
    }

    private String delUserSuccessKey(String systemName) {
        return String.format(DEL_USER_SUCCESS_FMT, systemName, today());
    }

    private Mono<Void> incrRoleUserCounters(String key, int userIncrement, String systemName, String stage) {
        return reactiveRedisManager.hincrBy(key, HASH_MESSAGE_FIELD, 1)
            .then(reactiveRedisManager.hincrBy(key, HASH_USER_FIELD, userIncrement))
            .doOnSuccess(v -> log.info("Redis计数成功 [{}] 系统={} key={} user+{}", stage, systemName, key, userIncrement))
            .doOnError(e -> log.warn("Redis计数失败 [{}] 系统={} key={} err={}", stage, systemName, key, e.getMessage()))
            .onErrorResume(e -> Mono.empty())
            .then();
    }
}

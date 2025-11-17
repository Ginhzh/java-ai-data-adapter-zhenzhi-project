package com.weichai.knowledge.service;

import com.weichai.knowledge.entity.FileNotChange;
import com.weichai.knowledge.entity.RoleUserMessage;
import com.weichai.knowledge.entity.KnowledgeRoleSyncLog;
import com.weichai.knowledge.utils.ErrorHandler;
import com.weichai.knowledge.repository.ReactiveKnowledgeRoleSyncLogRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;
import java.time.Duration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weichai.knowledge.entity.UnstructuredDocument;
import com.weichai.knowledge.repository.ReactiveUnstructuredDocumentRepository;
import com.weichai.knowledge.redis.ReactiveRedisManager;
import java.util.stream.Stream;

/**
 * 知识库角色权限服务
 * 处理文件权限变更相关的业务逻辑
 * 与现有的FileNotChange实体类和MessageHandler兼容
 */
@Slf4j
@Service
public class KnowledgeRoleService {
    
    // 错误类型常量
    private static final int ERROR_TYPE_FILE_NOT_IMPORTED = 5;
    
    // 最大查询次数
    private static final int MAX_QUERY_ATTEMPTS = 5;
    
    // Redis键格式常量
    private static final String VERIFICATION_COUNT_KEY_FORMAT = "%s:knowledge:verification:passed:%s";
    private static final String PERMISSION_SUCCESS_KEY_FORMAT = "%s:knowledge:operation:success:%s";
    
    @Autowired
    private ReactiveKnowledgeHandler reactiveKnowledgeHandler;
    
    @Autowired
    private RoleUserHandler roleUserHandler;
    
    @Autowired
    private ErrorHandler errorHandler;
    
    @Autowired
    private ReactiveKnowledgeRoleSyncLogRepository syncLogRepository;

    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private DatabaseClient databaseClient;
    
    @Autowired
    private ReactiveUnstructuredDocumentRepository unstructuredDocumentRepository;
    
    @Autowired
    private ReactiveRedisManager reactiveRedisManager;
    
    /**
     * 格式化知识库名称
     * 
     * @param filePath 文件路径
     * @param systemName 系统名称
     * @return 格式化后的知识库名称
     */
    private String formatRepoName(String filePath, String systemName) {
        // 处理空路径的情况
        if (filePath == null || filePath.isEmpty() || "/".equals(filePath)) {
            return "默认路径";
        }
        
        String pathOnly = filePath;
        
        // 如果最后一个部分包含扩展名，则移除它
        String basename = filePath.substring(filePath.lastIndexOf('/') + 1);
        if (basename.contains(".") && basename.split("\\.").length > 1) {
            // 只有当最后部分是文件名（包含扩展名）时才移除
            int lastSlashIndex = filePath.lastIndexOf('/');
            pathOnly = lastSlashIndex > 0 ? filePath.substring(0, lastSlashIndex) : filePath;
        }
        
        // 按斜杠分割路径
        String[] pathParts = pathOnly.replaceAll("^/|/$", "").split("/");
        
        // 过滤掉空字符串
        List<String> validParts = Arrays.stream(pathParts)
            .filter(part -> !part.isEmpty())
            .collect(Collectors.toList());
        
        // 处理没有有效路径片段的情况
        if (validParts.isEmpty()) {
            return "默认路径";
        }
        
        // 根据系统名称选择目录
        List<String> selectedParts;
        if ("WPROS".equals(systemName) || "WPROS_STRUCT".equals(systemName)) {
            // 取前两层目录，如果不足两层则全部保留
            int endIndex = Math.min(2, validParts.size());
            selectedParts = validParts.subList(0, endIndex);
        } else {
            // 取后三层目录，如果不足三层则全部保留
            int startIndex = Math.max(0, validParts.size() - 3);
            selectedParts = validParts.subList(startIndex, validParts.size());
        }
        
        // 将路径片段用短横线连接
        return String.join("-", selectedParts);
    }
    
    @Transactional
public Mono<Map<String, Object>> processFileNotChangeMessage(FileNotChange fileNotChange) {
    log.info("开始处理FILE_NOT_CHANGE消息: {}", fileNotChange.getMessageTaskId());
    
    // 获取角色和用户列表
    List<String> addRoleList = fileNotChange.getAddRoleList();
    List<String> addUserList = fileNotChange.getAddUserList();
    List<String> delRoleList = fileNotChange.getDelRoleList();
    List<String> delUserList = fileNotChange.getDelUserList();

    // 查询甄知文件ID - 使用fileMetadata中的fileNumber
    return reactiveKnowledgeHandler.queryZhenzhiFileId(
        fileNotChange.getFileMetadata().getFileNumber(),
        fileNotChange.getFileMetadata().getSystemName()
    )
    .switchIfEmpty(Mono.defer(() -> {
        String errorMsg = String.format("未找到 zhenzhi_file_id，file_id: %s", fileNotChange.getFileId());
        log.error(errorMsg);
        errorHandler.logError(ERROR_TYPE_FILE_NOT_IMPORTED, fileNotChange.getFileId(), 
            "query_zhenzhi_file_id", errorMsg,
            Map.of("file_id", fileNotChange.getFileId(), "file_name", fileNotChange.getFileMetadata().getFileName())
        );
        return Mono.just(createErrorResponse(errorMsg));
    }))
    .flatMap(zhenzhiFileIdInfo -> {
        if (zhenzhiFileIdInfo == null) {
            String errorMsg = String.format("未找到 zhenzhi_file_id，file_id: %s", fileNotChange.getFileId());
            log.error(errorMsg);
            return Mono.just(createErrorResponse(errorMsg));
        }
        
        String zhenzhiFileId = (String) zhenzhiFileIdInfo.get("zhenzhi_file_id");
        String fileNumber = (String) zhenzhiFileIdInfo.get("file_number");
        log.info("获取到甄知文档ID: {}, 文档编号: {}", zhenzhiFileId, fileNumber);
        
        // 查询知识库映射 - 使用formatRepoName处理文件路径
        String filePath = fileNotChange.getFileMetadata().getFilePath();
        String systemName = fileNotChange.getFileMetadata().getSystemName();
        String repoName;
        
        try {
            // 处理特殊系统名
            String processedFilePath = filePath;
            if ("WPROS_STRUCT".equals(systemName)) {
                processedFilePath = "WPROS_STRUCT_" + filePath;
                log.info("WPROS_STRUCT系统特殊处理: 原路径={}, 处理后路径={}", filePath, processedFilePath);
            }
            
            repoName = formatRepoName(processedFilePath, systemName);
        } catch (Exception e) {
            log.error("处理WPROS_STRUCT系统文件路径时发生错误: {}", e.getMessage(), e);
            repoName = formatRepoName(filePath, systemName);
        }
        
        return reactiveKnowledgeHandler.queryRepoMapping(
            fileNotChange.getFileMetadata().getSystemName(), repoName)
            .flatMap(repoMapping -> {
                // 从查询结果中获取repoId和departmentId
                final String repoId = repoMapping != null ? (String) repoMapping.get("repo_id") : null;
                final String departmentId = repoMapping != null ? (String) repoMapping.get("department_id") : null;
                
                if (repoMapping != null) {
                    log.info("已找到知识库映射，知识库ID: {}, 部门ID: {}", repoId, departmentId);
                } else {
                    log.warn("未找到知识库映射");
                }
                
                // 只有当有用户列表时才创建虚拟用户组消息（用于用户绑定/解绑）
                RoleUserMessage roleUserMessage = null;
                if ((addUserList != null && !addUserList.isEmpty()) || 
                    (delUserList != null && !delUserList.isEmpty())) {
                    roleUserMessage = new RoleUserMessage();
                    roleUserMessage.setRoleId("virtual_" + zhenzhiFileId);
                    roleUserMessage.setMessageTaskId(fileNotChange.getMessageTaskId());
                    roleUserMessage.setMessageType("ADD_ROLE");
                    roleUserMessage.setRoleName(fileNotChange.getFileMetadata().getFileName());
                    roleUserMessage.setSystemName(fileNotChange.getFileMetadata().getSystemName());
                    roleUserMessage.setMessageDateTime(fileNotChange.getMessageDateTime());
                    roleUserMessage.setAddUserList(addUserList);
                    roleUserMessage.setDelUserList(delUserList);
                }
                
                // 创建final变量以在lambda中使用
                final RoleUserMessage finalRoleUserMessage = roleUserMessage;
                final String finalZhenzhiFileId = zhenzhiFileId;
                final String finalFileNumber = fileNumber;
                final String finalRepoId = repoId;
                final String finalDepartmentId = departmentId;
                
                // 生成任务专用的请求头
                return reactiveKnowledgeHandler.generateHeadersWithSignature(
                    "system_" + fileNotChange.getFileMetadata().getSystemName())
                    .flatMap(taskHeaders -> {
                        // 查询文档状态并处理（传入taskHeaders）
                        return processDocumentStatusReactiveEnhanced(finalZhenzhiFileId, finalRoleUserMessage, 
                            addRoleList, delRoleList, addUserList, delUserList, fileNotChange, 
                            finalRepoId, finalDepartmentId, taskHeaders)
                            .flatMap(documentProcessed -> {
                                if (documentProcessed) {
                                    // 记录同步日志
                                    return recordSyncLogReactive(fileNotChange, finalZhenzhiFileId, finalFileNumber, finalRepoId, 
                                        addRoleList, addUserList, delRoleList, delUserList)
                                        .then(Mono.just(createSuccessResponse()));
                                } else {
                                    return Mono.just(createErrorResponse("文档处理失败"));
                                }
                            });
                    })
                    .onErrorResume(signatureError -> {
                        log.error("生成签名失败，后续需要签名的操作将无法执行: {}", signatureError.getMessage());
                        return Mono.just(createErrorResponse("生成签名失败: " + signatureError.getMessage()));
                    });
            });
    })
    .onErrorMap(e -> {
        log.error("处理FILE_NOT_CHANGE消息时发生错误", e);
        return new RuntimeException("处理FILE_NOT_CHANGE消息失败: " + e.getMessage(), e);
    });
}

    /**
     * 处理文档状态 - 反应式版本
     */
    private Mono<Boolean> processDocumentStatusReactive(String zhenzhiFileId, RoleUserMessage roleUserMessage,
            List<String> addRoleList, List<String> delRoleList, List<String> addUserList, 
            List<String> delUserList, FileNotChange fileNotChange, String repoId, String departmentId) {
        
        // 查询文档状态
        return reactiveKnowledgeHandler.queryFileStatus(Arrays.asList(zhenzhiFileId), 1)
            .flatMap(fileStatus -> {
                if (Boolean.TRUE.equals(fileStatus.get("success"))) {
                    log.info("文件 {} 文档成功上线", zhenzhiFileId);
                    return processOnlineDocumentReactive(roleUserMessage, addRoleList, delRoleList, 
                        fileNotChange, repoId, departmentId);
                } else {
                    // 未入库时，重试查询
                    return retryDocumentProcessingReactive(zhenzhiFileId, roleUserMessage, addRoleList, 
                        delRoleList, fileNotChange, repoId, departmentId);
                }
            })
            .onErrorReturn(false);
    }

    
    /**
     * 处理文档状态 - 增强版本，集成数据库和Redis逻辑
     */
    private Mono<Boolean> processDocumentStatusReactiveEnhanced(String zhenzhiFileId, RoleUserMessage roleUserMessage,
            List<String> addRoleList, List<String> delRoleList, List<String> addUserList, 
            List<String> delUserList, FileNotChange fileNotChange, String repoId, String departmentId, 
            HttpHeaders taskHeaders) {
        
        String systemName = fileNotChange.getFileMetadata().getSystemName();
        String fileNumber = fileNotChange.getFileMetadata().getFileNumber();
        
        // 查询文档状态
        return reactiveKnowledgeHandler.queryFileStatus(Arrays.asList(zhenzhiFileId), 1)
            .flatMap(fileStatus -> {
                if (Boolean.TRUE.equals(fileStatus.get("success"))) {
                    log.info("文件 {} 文档成功上线", zhenzhiFileId);
                    
                    // 记录验证成功统计
                    String today = LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    String verificationKey = getVerificationCountKey(systemName, today);
                    
                    return incrementRedisCounter(verificationKey, "权限校验通过（文档状态查询成功）", systemName)
                        .then(processOnlineDocumentEnhanced(roleUserMessage, addRoleList, delRoleList, 
                            fileNotChange, repoId, departmentId, zhenzhiFileId, fileNumber, taskHeaders));
                } else {
                    // 未入库时，重试查询并同步数据库状态
                    return retryDocumentProcessingEnhanced(zhenzhiFileId, roleUserMessage, addRoleList, 
                        delRoleList, fileNotChange, repoId, departmentId, fileNumber, taskHeaders);
                }
            })
            .onErrorReturn(false);
    }
    
    /**
     * 处理已上线文档 - 增强版本
     */
    private Mono<Boolean> processOnlineDocumentEnhanced(RoleUserMessage roleUserMessage, 
            List<String> addRoleList, List<String> delRoleList, FileNotChange fileNotChange, 
            String repoId, String departmentId, String zhenzhiFileId, String fileNumber, 
            HttpHeaders taskHeaders) {
        
        String systemName = fileNotChange.getFileMetadata().getSystemName();
        String adminOpenId = "system_" + systemName;
        
        // 处理用户绑定/解绑（只有当有用户列表且roleUserMessage不为空时才处理）
        Mono<Boolean> userProcessingMono = Mono.just(true);
        if (roleUserMessage != null) {
            userProcessingMono = roleUserHandler.handleMessage(roleUserMessage)
                .flatMap(userProcessingResult -> {
                    if (!Boolean.TRUE.equals(userProcessingResult)) {
                        log.error("用户处理失败");
                        return Mono.just(false);
                    }
                    
                    // 记录用户操作成功的Redis统计
                    String today = LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    String permissionKey = getPermissionSuccessCountKey(systemName, today);
                    return incrementRedisCounter(permissionKey, "用户绑定/解绑成功", systemName)
                        .then(Mono.just(true));
                })
                .onErrorReturn(false);
        }
        
        // 处理角色权限（独立于用户处理）
        return userProcessingMono.flatMap(userResult -> {
            Mono<Boolean> addRolesMono = Mono.just(true);
            Mono<Boolean> delRolesMono = Mono.just(true);
            
            // 添加角色权限（使用增强的包装器）
            if (addRoleList != null && !addRoleList.isEmpty()) {
                log.info("需要添加角色权限: {}", addRoleList);
                addRolesMono = setDocAdminWrapper(zhenzhiFileId, systemName, fileNumber,
                    repoId, departmentId, adminOpenId, addRoleList, "add", taskHeaders);
            }

            // 删除角色权限（使用增强的包装器）
            if (delRoleList != null && !delRoleList.isEmpty()) {
                log.info("需要删除角色权限: {}", delRoleList);
                delRolesMono = setDocAdminWrapper(zhenzhiFileId, systemName, fileNumber,
                    repoId, departmentId, adminOpenId, delRoleList, "del", taskHeaders);
            }
            
            return Mono.zip(addRolesMono, delRolesMono)
                .map(tuple -> tuple.getT1() && tuple.getT2() && userResult);
        })
        .onErrorReturn(false);
    }
    
    /**
     * 重试文档处理 - 增强版本
     */
    private Mono<Boolean> retryDocumentProcessingEnhanced(String zhenzhiFileId, RoleUserMessage roleUserMessage,
            List<String> addRoleList, List<String> delRoleList, FileNotChange fileNotChange,
            String repoId, String departmentId, String fileNumber, HttpHeaders taskHeaders) {
        
        String systemName = fileNotChange.getFileMetadata().getSystemName();
        
        return Mono.fromCallable(() -> 0) // 初始重试次数
            .expand(attempts -> {
                if (attempts >= MAX_QUERY_ATTEMPTS) {
                    return Mono.empty(); // 停止重试
                }
                
                return Mono.delay(Duration.ofSeconds(1))
                    .then(reactiveKnowledgeHandler.queryFileStatus(Arrays.asList(zhenzhiFileId), 1))
                    .flatMap(fileStatus -> {
                        if (Boolean.TRUE.equals(fileStatus.get("success"))) {
                            log.info("第{}次查询成功，文件 {} 文档成功上线", attempts + 1, zhenzhiFileId);
                            return Mono.empty(); // 成功，停止重试
                        } else {
                            return Mono.just(attempts + 1); // 继续重试
                        }
                    })
                    .onErrorResume(e -> {
                        log.error("第{}次查询失败", attempts + 1, e);
                        return Mono.just(attempts + 1);
                    });
            })
            .last() // 获取最后一次的重试次数
            .flatMap(finalAttempts -> {
                if (finalAttempts < MAX_QUERY_ATTEMPTS) {
                    // 在重试过程中成功了
                    String today = LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    String verificationKey = getVerificationCountKey(systemName, today);
                    
                    return incrementRedisCounter(verificationKey, "权限校验通过（重试后成功）", systemName)
                        .then(processOnlineDocumentEnhanced(roleUserMessage, addRoleList, delRoleList, 
                            fileNotChange, repoId, departmentId, zhenzhiFileId, fileNumber, taskHeaders));
                } else {
                    // 达到最大查询次数，记录错误并同步数据库状态
                    log.error("达到最大查询次数，文档 {} 未成功上线", zhenzhiFileId);
                    errorHandler.logError(
                        ERROR_TYPE_FILE_NOT_IMPORTED,
                        fileNotChange.getFileId(),
                        "文档未入库",
                        String.format("文档 %s 未成功上线", zhenzhiFileId),
                        Map.of(
                            "task_id", fileNotChange.getMessageTaskId(),
                            "message_type", fileNotChange.getMessageType(),
                            "zhenzhi_file_id", zhenzhiFileId
                        )
                    );
                    
                    // 同步数据库状态为未上线
                    return syncUnstructuredDocumentStatus(zhenzhiFileId, systemName, fileNumber, 0)
                        .then(Mono.defer(() -> {
                            // 如果有权限变更请求，也要同步到数据库
                            if ((addRoleList != null && !addRoleList.isEmpty()) ||
                                (delRoleList != null && !delRoleList.isEmpty())) {
                                return syncUnstructuredDocumentRoles(zhenzhiFileId, systemName, fileNumber,
                                    addRoleList, delRoleList).then(Mono.just(false));
                            }
                            return Mono.just(false);
                        }));
                }
            });
    }
    
    /**
     * 处理已上线文档 - 反应式版本
     */
    private Mono<Boolean> processOnlineDocumentReactive(RoleUserMessage roleUserMessage, 
            List<String> addRoleList, List<String> delRoleList, 
            FileNotChange fileNotChange, String repoId, String departmentId) {
        
        // 处理用户绑定/解绑
        return roleUserHandler.handleMessage(roleUserMessage)
            .flatMap(userProcessingResult -> {
                if (!Boolean.TRUE.equals(userProcessingResult)) {
                    log.error("用户处理失败");
                    return Mono.just(false);
                }
                
                // 处理角色权限
                String systemName = fileNotChange.getFileMetadata().getSystemName();
                String adminOpenId = "system_" + systemName;
                
                // 生成本次任务专用的请求头
                return reactiveKnowledgeHandler.generateHeadersWithSignature(adminOpenId)
                    .flatMap(taskHeaders -> {
                        Mono<Boolean> addRolesMono = Mono.just(true);
                        Mono<Boolean> delRolesMono = Mono.just(true);
                        
                        // 添加角色权限
                        if (addRoleList != null && !addRoleList.isEmpty()) {
                            log.info("需要添加角色权限: {}", addRoleList);
                            addRolesMono = reactiveKnowledgeHandler.setDocAdmin(
                                systemName,
                                roleUserMessage.getRoleId().replace("virtual_", ""),
                                repoId,
                                addRoleList,
                                Collections.emptyList(),
                                "add",
                                departmentId,
                                adminOpenId,
                                2,
                                taskHeaders
                            )
                            .doOnNext(addResult -> log.info("设置文档权限结果: {}", addResult))
                            .then(Mono.just(true));
                        }

                        // 删除角色权限
                        if (delRoleList != null && !delRoleList.isEmpty()) {
                            log.info("需要删除角色权限: {}", delRoleList);
                            delRolesMono = reactiveKnowledgeHandler.setDocAdmin(
                                systemName,
                                roleUserMessage.getRoleId().replace("virtual_", ""),
                                repoId,
                                delRoleList,
                                Collections.emptyList(),
                                "del",
                                departmentId,
                                adminOpenId,
                                2,
                                taskHeaders
                            )
                            .doOnNext(delResult -> log.info("删除文档权限结果: {}", delResult))
                            .then(Mono.just(true));
                        }
                        
                        return Mono.zip(addRolesMono, delRolesMono)
                            .map(tuple -> tuple.getT1() && tuple.getT2());
                    });
            })
            .onErrorReturn(false);
    }
    
    /**
     * 重试文档处理 - 反应式版本
     */
    private Mono<Boolean> retryDocumentProcessingReactive(String zhenzhiFileId, RoleUserMessage roleUserMessage,
            List<String> addRoleList, List<String> delRoleList, FileNotChange fileNotChange,
            String repoId, String departmentId) {
        
        return Mono.fromCallable(() -> 0) // 初始重试次数
            .expand(attempts -> {
                if (attempts >= MAX_QUERY_ATTEMPTS) {
                    return Mono.empty(); // 停止重试
                }
                
                return Mono.delay(Duration.ofSeconds(1))
                    .then(reactiveKnowledgeHandler.queryFileStatus(Arrays.asList(zhenzhiFileId), 1))
                    .flatMap(fileStatus -> {
                        if (Boolean.TRUE.equals(fileStatus.get("success"))) {
                            log.info("第{}次查询成功，文件 {} 文档成功上线", attempts + 1, zhenzhiFileId);
                            return Mono.empty(); // 成功，停止重试
                        } else {
                            return Mono.just(attempts + 1); // 继续重试
                        }
                    })
                    .onErrorResume(e -> {
                        log.error("第{}次查询失败", attempts + 1, e);
                        return Mono.just(attempts + 1);
                    });
            })
            .last() // 获取最后一次的重试次数
            .flatMap(finalAttempts -> {
                if (finalAttempts < MAX_QUERY_ATTEMPTS) {
                    // 在重试过程中成功了
                    return processOnlineDocumentReactive(roleUserMessage, addRoleList, delRoleList, 
                        fileNotChange, repoId, departmentId);
                } else {
                    // 达到最大查询次数
                    log.error("达到最大查询次数，文档 {} 未成功上线", zhenzhiFileId);
                    errorHandler.logError(
                        ERROR_TYPE_FILE_NOT_IMPORTED,
                        fileNotChange.getFileId(),
                        "文档未入库",
                        String.format("文档 %s 未成功上线", zhenzhiFileId),
                        Map.of(
                            "task_id", fileNotChange.getMessageTaskId(),
                            "message_type", fileNotChange.getMessageType(),
                            "zhenzhi_file_id", zhenzhiFileId
                        )
                    );
                    return Mono.just(false);
                }
            });
    }
    
    /**
     * 记录同步日志 - 反应式版本
     */
    private Mono<Void> recordSyncLogReactive(FileNotChange fileNotChange, String zhenzhiFileId, String fileNumber, String repoId,
            List<String> addRoleList, List<String> addUserList, List<String> delRoleList,
            List<String> delUserList) {
        
        String id = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();
        
        String insertSql = "INSERT INTO knowledge_role_sync_logs " +
                          "(id, message_task_id, message_type, message_datetime, file_id, file_name, " +
                          "system_name, repo_id, zhenzhi_file_id, file_number, add_role_list, add_user_list, " +
                          "del_role_list, del_user_list, created_at, updated_at) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return databaseClient.sql(insertSql)
            .bind(0, id)
            .bind(1, fileNotChange.getMessageTaskId())
            .bind(2, fileNotChange.getMessageType())
            .bind(3, fileNotChange.getMessageDateTime())
            .bind(4, fileNotChange.getFileId())
            .bind(5, fileNotChange.getFileMetadata().getFileName())
            .bind(6, fileNotChange.getFileMetadata().getSystemName())
            .bind(7, repoId)
            .bind(8, zhenzhiFileId)
            .bind(9, fileNumber)
            .bind(10, convertListToJsonString(addRoleList))
            .bind(11, convertListToJsonString(addUserList))
            .bind(12, convertListToJsonString(delRoleList))
            .bind(13, convertListToJsonString(delUserList))
            .bind(14, now)
            .bind(15, now)
            .fetch()
            .rowsUpdated()
            .doOnSuccess(rows -> log.info("成功记录文件权限同步日志，ID: {}", id))
            .doOnError(e -> log.error("记录同步日志时发生错误", e))
            .then();
    }
    

    
    /**
     * 处理已上线文档
     */
    private Mono<Boolean> processOnlineDocument(RoleUserMessage roleUserMessage, 
            List<String> addRoleList, List<String> delRoleList, 
            FileNotChange fileNotChange, String repoId, String departmentId) {
        
        // 处理用户绑定/解绑
        return roleUserHandler.handleMessage(roleUserMessage)
            .flatMap(userProcessingResult -> {
                if (!Boolean.TRUE.equals(userProcessingResult)) {
                    log.error("用户处理失败");
                    return Mono.just(false);
                }
        
                // 处理角色权限
                String systemName = fileNotChange.getFileMetadata().getSystemName();
                String adminOpenId = "system_" + systemName;
                
                // 生成本次任务专用的请求头
                return reactiveKnowledgeHandler.generateHeadersWithSignature(adminOpenId)
                    .flatMap(taskHeaders -> {
        
                        Mono<Boolean> addRolesMono = Mono.just(true);
                        Mono<Boolean> delRolesMono = Mono.just(true);
                        
                        // 添加角色权限
                        if (addRoleList != null && !addRoleList.isEmpty()) {
                            log.info("需要添加角色权限: {}", addRoleList);
                            addRolesMono = reactiveKnowledgeHandler.setDocAdmin(
                                systemName,
                                roleUserMessage.getRoleId().replace("virtual_", ""),
                                repoId,
                                addRoleList,
                                Collections.emptyList(),
                                "add",
                                departmentId,
                                adminOpenId,
                                2,
                                taskHeaders
                            )
                            .doOnNext(addResult -> log.info("设置文档权限结果: {}", addResult))
                            .then(Mono.just(true));
                        }

                        // 删除角色权限
                        if (delRoleList != null && !delRoleList.isEmpty()) {
                            log.info("需要删除角色权限: {}", delRoleList);
                            delRolesMono = reactiveKnowledgeHandler.setDocAdmin(
                                systemName,
                                roleUserMessage.getRoleId().replace("virtual_", ""),
                                repoId,
                                delRoleList,
                                Collections.emptyList(),
                                "del",
                                departmentId,
                                adminOpenId,
                                2,
                                taskHeaders
                            )
                            .doOnNext(delResult -> log.info("删除文档权限结果: {}", delResult))
                            .then(Mono.just(true));
                        }
                        
                        return Mono.zip(addRolesMono, delRolesMono)
                            .map(tuple -> tuple.getT1() && tuple.getT2());
                    });
            })
            .onErrorReturn(false);
    }
    

    

    
    /**
     * 创建成功响应
     */
    private Map<String, Object> createSuccessResponse() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "处理成功");
        return response;
    }
    
    /**
     * 创建错误响应
     */
    private Map<String, Object> createErrorResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", message);
        return response;
    }
    
    
    /**
     * 生成Redis验证计数键
     */
    private String getVerificationCountKey(String systemName, String date) {
        return String.format(VERIFICATION_COUNT_KEY_FORMAT, systemName, date);
    }
    
    /**
     * 生成Redis权限设置成功计数键
     */
    private String getPermissionSuccessCountKey(String systemName, String date) {
        return String.format(PERMISSION_SUCCESS_KEY_FORMAT, systemName, date);
    }
    
    /**
     * 递增Redis计数器并记录日志
     */
    private Mono<Void> incrementRedisCounter(String key, String operation, String systemName) {
        return reactiveRedisManager.get(key, String.class)
            .defaultIfEmpty("0")
            .map(Integer::valueOf)
            .map(current -> current + 1)
            .flatMap(newValue -> reactiveRedisManager.set(key, newValue.toString(), Duration.ofDays(30)))
            .doOnNext(result -> {
                if (Boolean.TRUE.equals(result)) {
                    log.info("{} - 系统: {}", operation, systemName);
                }
            })
            .doOnError(e -> log.warn("记录{}统计失败，但不影响主流程: {}", operation, e.getMessage()))
            .onErrorResume(e -> Mono.empty())
            .then();
    }
    
    /**
     * 同步非结构化文档角色列表
     */
    private Mono<List<String>> syncUnstructuredDocumentRoles(String zhenzhiFileId, String systemName, 
            String fileNumber, List<String> addRoles, List<String> delRoles) {
        
        return unstructuredDocumentRepository.findByZhenzhiFileId(zhenzhiFileId)
            .flatMap(document -> {
                // 解析现有角色列表
                Set<String> currentRoles = parseRoleList(document.getRoleList());
                log.info("解析现有角色列表: {}", currentRoles);
                
                boolean isOfflineDocument = document.getStatus() == 0;
                List<String> existingRoles = new ArrayList<>(currentRoles);
                
                // 计算更新后的角色列表
                if (addRoles != null) {
                    currentRoles.addAll(addRoles);
                }
                if (delRoles != null) {
                    currentRoles.removeAll(delRoles);
                }
                
                // 更新文档记录
                try {
                    JsonNode updatedRoleListJson = objectMapper.valueToTree(new ArrayList<>(currentRoles));
                    document.setRoleList(updatedRoleListJson);
                    document.setUpdatedAt(LocalDateTime.now());
                } catch (Exception e) {
                    log.error("转换角色列表为JsonNode失败: {}", e.getMessage(), e);
                    throw new RuntimeException("角色列表转换失败", e);
                }
                
                if (isOfflineDocument) {
                    document.setStatus(1); // 设置为已上线
                    log.info("文档状态为0，需要完整设置权限。现有角色: {}, 最终角色: {}", existingRoles, currentRoles);
                }
                
                return unstructuredDocumentRepository.save(document)
                    .then(Mono.just(isOfflineDocument ? existingRoles : new ArrayList<String>()));
            })
            .switchIfEmpty(Mono.defer(() -> {
                log.warn("未找到文档记录，无法同步角色列表: {}", zhenzhiFileId);
                return Mono.just(Collections.emptyList());
            }))
            .onErrorResume(e -> {
                log.error("同步非结构化文档角色时发生错误: {}", e.getMessage(), e);
                return Mono.just(Collections.emptyList());
            });
    }
    
    /**
     * 同步非结构化文档状态
     */
    private Mono<Void> syncUnstructuredDocumentStatus(String zhenzhiFileId, String systemName, 
            String fileNumber, int status) {
        
        return unstructuredDocumentRepository.findByZhenzhiFileId(zhenzhiFileId)
            .flatMap(document -> {
                document.setStatus(status);
                document.setUpdatedAt(LocalDateTime.now());
                log.info("更新非结构化文档记录状态为: {}", status);
                return unstructuredDocumentRepository.save(document);
            })
            .switchIfEmpty(Mono.defer(() -> {
                log.warn("未找到文档记录，无法同步状态: {}", zhenzhiFileId);
                return Mono.empty();
            }))
            .onErrorResume(e -> {
                log.error("同步非结构化文档状态时发生错误: {}", e.getMessage(), e);
                return Mono.empty();
            })
            .then();
    }
    
    /**
     * 解析角色列表，处理可能的逗号分隔字符串
     */
    private Set<String> parseRoleList(JsonNode roleListJson) {
        if (roleListJson == null || roleListJson.isNull()) {
            return new HashSet<>();
        }
        
        Set<String> roles = new HashSet<>();
        
        if (roleListJson.isArray()) {
            // 处理JSON数组
            for (JsonNode roleNode : roleListJson) {
                if (!roleNode.isNull()) {
                    String role = roleNode.asText();
                    if (role.contains(",")) {
                        // 处理逗号分隔的字符串
                        Arrays.stream(role.split(","))
                            .map(String::trim)
                            .filter(r -> !r.isEmpty())
                            .forEach(roles::add);
                    } else if (!role.trim().isEmpty()) {
                        roles.add(role.trim());
                    }
                }
            }
        } else if (roleListJson.isTextual()) {
            // 处理单个字符串
            String roleText = roleListJson.asText();
            if (roleText.contains(",")) {
                Arrays.stream(roleText.split(","))
                    .map(String::trim)
                    .filter(r -> !r.isEmpty())
                    .forEach(roles::add);
            } else if (!roleText.trim().isEmpty()) {
                roles.add(roleText.trim());
            }
        }
        
        return roles;
    }
    
    /**
     * 增强的文档权限设置包装器
     */
    private Mono<Boolean> setDocAdminWrapper(String zhenzhiFileId, String systemName, String fileNumber,
            String repoId, String departmentId, String adminOpenId, List<String> roleList, 
            String action, HttpHeaders taskHeaders) {
        
        if (roleList == null || roleList.isEmpty() || repoId == null) {
            return Mono.just(true);
        }
        
        return syncUnstructuredDocumentRoles(zhenzhiFileId, systemName, fileNumber, 
                "add".equals(action) ? roleList : null, 
                "del".equals(action) ? roleList : null)
            .flatMap(existingRoles -> {
                List<String> finalRoleList = roleList;
                String finalAction = action;
                
                // 如果是状态为0的文档，需要合并角色
                if (!existingRoles.isEmpty()) {
                    if ("add".equals(action)) {
                        Set<String> allRoles = new HashSet<>(existingRoles);
                        allRoles.addAll(roleList);
                        finalRoleList = new ArrayList<>(allRoles);
                        finalAction = "add"; // 统一使用add操作
                        log.info("文档状态为0，合并现有角色进行完整权限设置。现有: {}, 新增: {}, 最终: {}", 
                            existingRoles, roleList, finalRoleList);
                    } else if ("del".equals(action)) {
                        Set<String> remainingRoles = new HashSet<>(existingRoles);
                        remainingRoles.removeAll(roleList);
                        finalRoleList = new ArrayList<>(remainingRoles);
                        finalAction = "add"; // 统一使用add操作设置剩余角色
                        log.info("文档状态为0，删除指定角色后设置剩余权限。现有: {}, 删除: {}, 最终: {}", 
                            existingRoles, roleList, finalRoleList);
                        
                        if (finalRoleList.isEmpty()) {
                            log.warn("删除操作后没有剩余角色，跳过权限设置");
                            return Mono.just(true);
                        }
                    }
                }
                
                log.info("开始执行修改文档权限 - {}", finalAction);
                return reactiveKnowledgeHandler.setDocAdmin(
                    systemName, zhenzhiFileId, repoId, finalRoleList, Collections.emptyList(),
                    finalAction, departmentId, adminOpenId, 2, taskHeaders)
                    .doOnNext(result -> log.info("设置文档权限结果: {}", result))
                    .flatMap(docAdminResult -> {
                        if (docAdminResult != null && "success".equals(docAdminResult.get("status"))) {
                            // 权限设置成功，记录Redis统计
                            String today = LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                            String permissionKey = getPermissionSuccessCountKey(systemName, today);
                            return incrementRedisCounter(permissionKey, "权限设置成功", systemName)
                                .then(Mono.just(true));
                        }
                        return Mono.just(false);
                    });
            })
            .onErrorResume(e -> {
                log.warn("设置文档权限失败，但继续流程: {}", e.getMessage(), e);
                return Mono.just(false);
            });
    }

    /**
     * 将字符串列表转换为JSON字符串，用于DatabaseClient
     */
    private String convertListToJsonString(List<String> list) {
        try {
            if (list == null || list.isEmpty()) {
                return "[]";
            }
            return objectMapper.writeValueAsString(list);
        } catch (Exception e) {
            log.error("转换列表为JSON失败", e);
            return "[]";
        }
    }
} 
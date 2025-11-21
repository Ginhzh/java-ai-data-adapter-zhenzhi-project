package com.weichai.knowledge.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weichai.knowledge.entity.*;
import com.weichai.knowledge.service.ReactiveKnowledgeHandler;
import com.weichai.knowledge.utils.ErrorHandler;
import com.weichai.knowledge.utils.LoggingUtils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 响应式知识库添加服务类，处理文件添加消息，实现文档入库、权限设置和入库
 * 
 * 主要特性：
 * - 全响应式架构，基于Mono/Flux
 * - 非阻塞I/O操作
 * - 响应式链式处理
 * - 链式错误处理
 * - 背压支持
 */
@Slf4j
@Service
public class ReactiveKnowledgeAddService {
    
    @Autowired
    private ReactiveKnowledgeHandler reactiveKnowledgeHandler;
    
    @Autowired
    private ErrorHandler errorHandler;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // 初始化标记
    private volatile boolean initialized = false;
    
    /**
     * Spring初始化方法 - 响应式版本
     */
    @PostConstruct
    public void init() {
        try {
            if (!initialized) {
                initialized = true;
                log.info("ReactiveKnowledgeAddService初始化完成");
            }
        } catch (Exception e) {
            log.error("ReactiveKnowledgeAddService初始化失败", e);
            throw new RuntimeException("Failed to initialize ReactiveKnowledgeAddService", e);
        }
    }
    
    /**
     * 处理文件添加消息 - 完全响应式实现
     * 
     * 工作流程:
     * 1. 根据系统名查询系统部门ID
     * 2. 查询文档路径-知识库映射表
     * 3. 如未查询到映射，创建知识库并分配超管权限
     * 4. 更新文档路径-知识库映射表
     * 5. 推送数据到甄知
     * 6. 创建文档级别的虚拟用户组
     * 7. 绑定用户到虚拟组
     * 8. 维护非结构化数据记录表，状态设为0
     * 
     * @param message 消息数据，包含文件元数据和权限信息
     * @return 处理结果的响应式流
     */
    public Mono<Map<String, Object>> processFileAddMessage(Map<String, Object> message) {
        log.info("📋 开始响应式文件添加消息处理流程...");
        
        // 步骤1: 验证和提取消息数据 
        return validateAndExtractMessageData(message)
            .doOnNext(context -> log.info("步骤1完成，准备查询系统信息"))
            .doOnError(error -> log.error("步骤1失败: {}", error.getMessage(), error))
            // 步骤2: 查询系统信息（自动创建缺失的部门与管理员）
            .flatMap(this::querySystemInfoWithAutoCreate)
            // 步骤3: 生成任务请求头
            .flatMap(this::generateTaskHeaders)
            // 步骤4: 处理知识库映射和创建
            .flatMap(this::processRepositoryMapping)
            // 步骤5: 推送数据到甄知
            .flatMap(this::pushDataToZhenzhi)
            // 步骤6: 处理虚拟用户组（如果有用户）
            .flatMap(this::processVirtualGroups)
            // 步骤7: 维护非结构化数据记录表
            .flatMap(this::maintainUnstructuredDocument)
            // 步骤8: 构建最终结果
            .map(this::buildFinalResult)
            // 错误处理
            .onErrorResume(this::handleProcessingError)
            .timeout(Duration.ofMinutes(5)) // 5分钟超时
            .retryWhen(Retry.backoff(2, Duration.ofSeconds(3))
                .filter(throwable -> !(throwable instanceof IllegalArgumentException))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    log.error("文件添加处理重试耗尽，最后错误: {}", retrySignal.failure().getMessage());
                    return new RuntimeException("文件添加处理失败，已重试" + retrySignal.totalRetries() + "次", 
                        retrySignal.failure());
                }));
    }
    
    /**
     * 步骤1: 验证和提取消息数据
     */
    private Mono<ProcessingContext> validateAndExtractMessageData(Map<String, Object> message) {
        return Mono.fromCallable(() -> {
            log.info("步骤1: 开始验证和提取消息数据");
            
            // 验证消息数据完整性
            if (message == null || message.isEmpty()) {
                throw new IllegalArgumentException("消息数据为空");
            }
            
            // 提取消息数据
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) message.get("fileMetadata");
            if (metadata == null) {
                throw new IllegalArgumentException("fileMetadata字段不存在或为空");
            }
            
            @SuppressWarnings("unchecked")
            List<Object> roleList = (List<Object>) message.get("fileAddRoleList");
            @SuppressWarnings("unchecked")
            List<Object> userListRaw = (List<Object>) message.get("fileAddUserList");
            String messageTaskId = (String) message.get("messageTaskId");
            
            // 优先从metadata中获取fileId
            String fileId = (String) metadata.get("fileId");
            if (fileId == null || fileId.isEmpty()) {
                fileId = (String) message.get("fileId");
                log.info("metadata中没有fileId，使用顶层fileId: {}", fileId);
            } else {
                log.info("使用metadata中的fileId: {}", fileId);
            }
            
            // 验证必要字段
            if (fileId == null || fileId.isEmpty()) {
                throw new IllegalArgumentException("fileId不能为空");
            }
            
            // 从metadata中提取必要字段
            String systemName = (String) metadata.get("systemName");
            String fileNumber = (String) metadata.get("fileNumber");
            String fileName = (String) metadata.get("fileName");
            String filePath = (String) metadata.get("filePath");
            String version = (String) metadata.get("version");
            String bucketName = (String) metadata.get("bucketName");
            String objectKey = (String) metadata.get("objectKey");
            Object desc = metadata.get("description");
            
            if (systemName == null || fileNumber == null || fileName == null || filePath == null) {
                throw new IllegalArgumentException("文件元数据缺少必要字段");
            }
            
            if (bucketName == null || bucketName.isEmpty() || objectKey == null || objectKey.isEmpty()) {
                throw new IllegalArgumentException("bucketName和objectKey不能为空");
            }
            
            // SMS系统专属处理逻辑：
            // 1) 去除 fileId 空格；2) 将处理过的 fileId 与原始 fileName 拼接
            if ("SMS".equals(systemName)) {
                String cleanedFileId = fileId != null ? fileId.replace(" ", "") : "";
                String newFileName = cleanedFileId + (fileName != null ? fileName : "");
                log.info("SMS系统处理: 原file_id[{}] -> 清理后[{}] -> 新file_name[{}]", fileId, cleanedFileId, newFileName);
                fileName = newFileName;
            }
            
            log.info("开始处理系统[{}]的文件[{}]，文件ID[{}], 任务ID[{}]", 
                systemName, fileName, fileId, messageTaskId);
            
            // 创建处理上下文对象
            ProcessingContext context = new ProcessingContext();
            context.systemName = systemName;
            context.fileId = fileId;
            context.fileNumber = fileNumber;
            context.fileName = fileName;
            context.filePath = filePath;
            context.version = version;
            context.bucketName = bucketName;
            context.objectKey = objectKey;
            context.desc = desc;
            context.roleList = roleList;
            context.userListRaw = userListRaw;
            context.messageTaskId = messageTaskId;
            context.systemId = "system_" + systemName;
            
            log.info("步骤1: 消息数据提取完成，准备进入步骤2");
            return context;
        });
    }
    
    /**
     * 步骤2: 查询系统信息
     */
    private Mono<ProcessingContext> querySystemInfo(ProcessingContext context) {
        log.info("步骤2: 开始查询系统信息，系统名称: {}", context.systemName);
        
        return reactiveKnowledgeHandler.querySystemInfo(context.systemName)
            .flatMap(systemInfo -> {
                context.departmentId = (String) systemInfo.get("department_guid");
                context.adminOpenId = (String) systemInfo.get("admin_open_id");
                
                if (context.departmentId == null || context.departmentId.isEmpty() || 
                    context.adminOpenId == null || context.adminOpenId.isEmpty()) {
                    return Mono.error(new RuntimeException(String.format("无法获取系统[%s]的完整部门或管理员信息", context.systemName)));
                }
                
                log.info("查询系统信息成功，部门ID: {}, 管理员ID: {}", context.departmentId, context.adminOpenId);
                return Mono.just(context);
            })
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                .filter(throwable -> !(throwable instanceof IllegalArgumentException))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    log.error("查询系统信息失败，已重试{}次", retrySignal.totalRetries());
                    return new RuntimeException(
                        String.format("查询系统信息失败，已重试%d次", retrySignal.totalRetries()),
                        retrySignal.failure());
                }));
    }
    
    /**
     * 步骤3: 生成任务请求头
     */
    private Mono<ProcessingContext> generateTaskHeaders(ProcessingContext context) {
        log.info("步骤3: 开始生成任务请求头，管理员ID: {}", context.adminOpenId);
        
        return reactiveKnowledgeHandler.generateHeadersWithSignature(context.adminOpenId)
            .map(taskHeaders -> {
                context.taskHeaders = taskHeaders;
                log.info("步骤3完成，成功生成任务请求头");
                return context;
            })
            .onErrorMap(e -> {
                log.error("生成任务请求头失败: {}", e.getMessage(), e);
                return new RuntimeException("生成任务签名失败: " + e.getMessage(), e);
            });
    }
    
    /**
     * 步骤4: 处理知识库映射和创建
     */
    private Mono<ProcessingContext> processRepositoryMapping(ProcessingContext context) {
        log.info("步骤4: 开始处理知识库映射和创建");
        
        // 处理特殊系统名
        String processedFilePath = context.filePath;
        if ("WPROS_STRUCT".equals(context.systemName)) {
            processedFilePath = "WPROS_STRUCT_" + context.filePath;
        }

        String repoName = formatRepoName(processedFilePath, context.systemName);
        
        return reactiveKnowledgeHandler.queryRepoMapping(context.systemName, repoName)
            .flatMap(repoMapping -> {
                // 命中映射
                context.repoId = (String) repoMapping.get("repo_id");
                context.isNewRepo = false;
                log.info("已找到知识库映射，知识库ID: {}", context.repoId);
                return Mono.just(context);
            })
            .switchIfEmpty(Mono.defer(() -> {
                log.info("未找到知识库映射，触发创建流程: system={}, repoName={}", context.systemName, repoName);
                return createNewRepository(context, repoName);
            }));
    }
    
    /**
     * 创建新知识库
     */
    private Mono<ProcessingContext> createNewRepository(ProcessingContext context, String repoName) {
        context.isNewRepo = true;
        log.info("未找到知识库映射，准备创建新知识库");

        String repoIntro = String.format("系统 %s 路径 %s 的文档知识库", context.systemName, context.filePath);

        return reactiveKnowledgeHandler.createRepository(
                context.systemName, context.filePath, context.departmentId,
                repoName, repoIntro, 20, context.taskHeaders)
            .flatMap(createResult -> {
                Integer returnCode = (Integer) createResult.get("returnCode");
                if (!Integer.valueOf(200).equals(returnCode)) {
                    String errorMsg = (String) createResult.get("returnMessage");
                    log.error("创建知识库失败: {}", errorMsg);
                    return Mono.error(new RuntimeException(String.format("创建知识库失败: %s", errorMsg)));
                }

                context.repoId = (String) createResult.get("result");
                if (context.repoId == null || context.repoId.isEmpty()) {
                    return Mono.error(new RuntimeException("创建知识库成功但未返回知识库ID"));
                }

                log.info("成功创建知识库: {}", context.repoId);
                return Mono.just(context);
            });
    }
    
    /**
     * 步骤5: 推送数据到甄知
     */
    private Mono<ProcessingContext> pushDataToZhenzhi(ProcessingContext context) {
        log.info("步骤5: 开始推送数据到甄知");
        
        return Mono.fromCallable(() -> {
                // 构建额外字段数据
                Map<String, Object> extraFieldData = buildExtraFieldData(
                    context.systemName, context.filePath, context.fileId, 
                    context.fileNumber, context.version, context.desc);
                
                return objectMapper.writeValueAsString(extraFieldData);
            })
            .flatMap(extraFieldJson -> 
                reactiveKnowledgeHandler.importToRepo(
                    context.repoId, context.departmentId, context.bucketName, 
                    context.objectKey, context.fileId, context.fileName, 
                    extraFieldJson, null, context.taskHeaders))
            .flatMap(pushResponse -> validatePushResponse(pushResponse, context))
            .onErrorMap(e -> {
                log.error("推送数据时发生异常: {}", e.getMessage(), e);
                return new RuntimeException(String.format("推送数据时发生异常: %s", e.getMessage()), e);
            });
    }
    
    /**
     * 验证推送响应
     */
    private Mono<ProcessingContext> validatePushResponse(Map<String, Object> pushResponse, ProcessingContext context) {
        return Mono.fromCallable(() -> {
            // 检查推送结果
            Boolean success = (Boolean) pushResponse.get("success");
            if (Boolean.FALSE.equals(success)) {
                String errorMessage = extractErrorMessage(pushResponse);
                log.error("推送数据到甄知失败: {}", pushResponse);
                throw new RuntimeException(String.format("推送数据到甄知失败: %s", errorMessage));
            }
            
            Integer returnCode = (Integer) pushResponse.get("returnCode");
            if (returnCode != null && !Integer.valueOf(200).equals(returnCode)) {
                String errorMessage = extractErrorMessage(pushResponse);
                log.error("推送数据到甄知返回非成功状态码: {}", pushResponse);
                throw new RuntimeException(String.format("推送数据到甄知失败: %s", errorMessage));
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> resultObj = (Map<String, Object>) Optional
                .ofNullable(pushResponse.get("result"))
                .orElseGet(() -> Optional.ofNullable(pushResponse.get("data"))
                    .orElse(pushResponse.get("resultData")));
            
            if (resultObj == null) {
                log.error("推送成功但未返回文档数据，完整响应: {}", pushResponse);
                throw new RuntimeException("推送成功但未返回文档数据");
            }
            
            Object docGuidObj = resultObj.get("docGuid");
            context.docGuid = docGuidObj != null ? docGuidObj.toString() : null;
            
            if (context.docGuid == null || context.docGuid.isEmpty()) {
                log.error("推送成功但未返回文档GUID，完整响应: {}", pushResponse);
                throw new RuntimeException("推送成功但未返回文档GUID");
            }
            
            log.info("成功推送数据到甄知，文档GUID: {}", context.docGuid);
            return context;
        });
    }
    
    private String extractErrorMessage(Map<String, Object> pushResponse) {
        if (pushResponse == null) {
            return "未知错误";
        }
        List<String> keys = Arrays.asList("returnMessage", "message", "msg", "errorMessage", "error");
        for (String key : keys) {
            Object value = pushResponse.get(key);
            if (value != null) {
                return value.toString();
            }
        }
        return "未知错误";
    }
    
    /**
     * 步骤6: 处理虚拟用户组
     */
    private Mono<ProcessingContext> processVirtualGroups(ProcessingContext context) {
        if (context.userListRaw == null || context.userListRaw.isEmpty()) {
            log.info("步骤6: 没有用户列表，跳过虚拟用户组处理");
            return Mono.just(context);
        }
        
        log.info("步骤6: 开始处理虚拟用户组");
        String virtualGroupId = "virtual_" + context.docGuid;
        log.info("创建基于文档的虚拟群组 - ID和名称: {}", virtualGroupId);
        
        return reactiveKnowledgeHandler.syncVirtualGroup(
                context.systemName, virtualGroupId, virtualGroupId, 0, context.taskHeaders)
            .flatMap(syncResponse -> {
                Integer syncCode = (Integer) syncResponse.get("code");
                if (!Integer.valueOf(200).equals(syncCode)) {
                    log.warn("同步虚拟群组失败，但继续流程: {}", syncResponse);
                }
                
                // 绑定用户到虚拟组
                List<String> processedUserList = extractUserIds(context.userListRaw);
                if (!processedUserList.isEmpty()) {
                    return reactiveKnowledgeHandler.manageVirtualGroupRelation(
                            "CREATE", context.docGuid, processedUserList, context.taskHeaders)
                        .map(relationResponse -> {
                            Boolean relationSuccess = (Boolean) relationResponse.get("success");
                            if (!Boolean.TRUE.equals(relationSuccess)) {
                                log.warn("创建虚拟用户组关系失败，但继续流程: {}", relationResponse);
                            } else {
                                log.info("成功创建虚拟用户组关系，用户数: {}", processedUserList.size());
                            }
                            return context;
                        });
                } else {
                    log.info("没有有效的用户ID，跳过创建虚拟用户组关系");
                    return Mono.just(context);
                }
            })
            .onErrorResume(e -> {
                log.warn("处理虚拟用户组时出错，但继续流程: {}", e.getMessage(), e);
                return Mono.just(context);
            });
    }
    
    /**
     * 步骤7: 维护非结构化数据记录表
     */
    private Mono<ProcessingContext> maintainUnstructuredDocument(ProcessingContext context) {
        log.info("步骤7: 开始记录非结构化文档，状态强制设为0，文件ID：{}", context.fileId);
        
        // 构建角色列表字符串
        String roleListStr = extractRoleIds(context.roleList);
        
        return reactiveKnowledgeHandler.upsertUnstructuredDocument(
                context.systemName, context.fileId, context.fileNumber, context.fileName, 
                context.docGuid, context.version, 0, 0, roleListStr, context.repoId)
            .map(recordResult -> {
                Boolean recordSuccess = (Boolean) recordResult.get("success");
                if (Boolean.FALSE.equals(recordSuccess)) {
                    String errorMsg = (String) recordResult.getOrDefault("message", "未知错误");
                    log.warn("维护非结构化数据记录失败: {}", errorMsg);
                } else {
                    log.info("成功维护非结构化数据记录，状态已强制设为0，等待定时任务处理");
                }
                
                return context;
            });
    }
    
    /**
     * 步骤8: 构建最终结果
     */
    private Map<String, Object> buildFinalResult(ProcessingContext context) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "success");
        result.put("repo_id", context.repoId);
        result.put("department_id", context.departmentId);
        result.put("doc_guid", context.docGuid);
        result.put("file_id", context.fileId);
        result.put("system_name", context.systemName);
        result.put("file_path", context.filePath);
        result.put("bucket_name", context.bucketName);
        result.put("object_key", context.objectKey);
        result.put("file_name", context.fileName);
        result.put("is_new_repo", context.isNewRepo);
        result.put("message_task_id", context.messageTaskId);
        result.put("message", "文件处理成功，已记录到非结构化数据表，状态待定时任务处理");
        result.put("timestamp", LocalDateTime.now().toString());

        log.info("🎉 文件添加处理全部完成！系统: {}, 文件: {}, 文档GUID: {}",
            context.systemName, context.fileName, context.docGuid);

        return result;
    }
    
    /**
     * 错误处理
     */
    private Mono<Map<String, Object>> handleProcessingError(Throwable e) {
        log.error("处理文件添加消息时出错: {}", e.getMessage(), e);
        
        // 记录响应式错误日志
        return logErrorReactive(1, "unknown", "process_file_add_message", 
                "处理文件添加消息失败: " + e.getMessage(), 
                Map.of("error_type", e.getClass().getSimpleName()))
            .then(Mono.fromCallable(() -> {
                if (e instanceof IllegalArgumentException) {
                    return createErrorResponse(e.getMessage());
                }
                return createErrorResponse(String.format("处理失败: %s", e.getMessage()));
            }));
    }
    
    /**
     * 处理上下文类 - 在响应式链中传递数据
     */
    private static class ProcessingContext {
        String systemName;
        String fileId;
        String fileNumber;
        String fileName;
        String filePath;
        String version;
        String bucketName;
        String objectKey;
        Object desc;
        List<Object> roleList;
        List<Object> userListRaw;
        String messageTaskId;
        String systemId;
        String departmentId;
        String adminOpenId;
        HttpHeaders taskHeaders;
        String repoId;
        boolean isNewRepo;
        String docGuid;
    }
    
    /**
     * 格式化知识库名称
     * 
     * 规则:
     * 1. 将file_path中的斜杠/替换为短横线-
     * 2. 只取file_path的最后三层目录（如果系统为WPROS或WPROS_TEST，则取前三层目录）
     * 3. 确保最后一个片段是路径而不是文件名
     */
    private String formatRepoName(String filePath, String systemName) {
        // 处理空路径的情况
        if (filePath == null || filePath.isEmpty() || "/".equals(filePath)) {
            return "默认路径";
        }
        
        // 如果最后一个部分包含扩展名，则移除它
        String pathOnly;
        String baseName = filePath.substring(filePath.lastIndexOf('/') + 1);
        if (baseName.contains(".") && baseName.split("\\.").length > 1) {
            // 只有当最后部分是文件名（包含扩展名）时才移除
            int lastSlashIndex = filePath.lastIndexOf('/');
            pathOnly = lastSlashIndex > 0 ? filePath.substring(0, lastSlashIndex) : "";
        } else {
            // 否则保留完整路径
            pathOnly = filePath;
        }
        
        // 按斜杠分割路径
        String[] pathParts = pathOnly.replaceFirst("^/", "").split("/");
        
        // 过滤掉空字符串
        List<String> validParts = Arrays.stream(pathParts)
            .filter(part -> part != null && !part.isEmpty())
            .collect(Collectors.toList());
        
        // 处理没有有效路径片段的情况
        if (validParts.isEmpty()) {
            return "默认路径";
        }
        
        // 根据系统名称选择目录
        List<String> selectedParts;
        if ("WPROS".equals(systemName) || "WPROS_STRUCT".equals(systemName)) {
            // 取前两层目录，如果不足两层则全部保留
            int takeCount = Math.min(2, validParts.size());
            selectedParts = validParts.subList(0, takeCount);
        } else {
            // 取后三层目录，如果不足三层则全部保留
            int takeCount = Math.min(3, validParts.size());
            int startIndex = Math.max(0, validParts.size() - takeCount);
            selectedParts = validParts.subList(startIndex, validParts.size());
        }
        
        // 将路径片段用短横线连接
        return String.join("-", selectedParts);
    }
    
    /**
     * 构建额外字段数据
     */
    private Map<String, Object> buildExtraFieldData(String systemName, String filePath, 
            String fileId, String fileNumber, String version, Object desc) {
        Map<String, Object> extraFieldData = new HashMap<>();
        
        if ("SIS".equals(systemName) || "EPC".equals(systemName)) {
            try {
                if (desc != null) {
                    Map<String, Object> descData = new HashMap<>();

                    // 1. 判断 desc 是否已经是 Map 类型
                    if (desc instanceof Map) {
                        // 如果已经是Map，直接强制转换并使用
                        // 使用 @SuppressWarnings 来抑制未经检查的转换警告
                        @SuppressWarnings("unchecked")
                        Map<String, Object> tempMap = (Map<String, Object>) desc;
                        descData.putAll(tempMap);

                    // 2. 如果不是Map，再判断是否是字符串并尝试解析
                    } else if (desc instanceof String) {
                        String descStr = (String) desc;
                        if (!descStr.isEmpty() && descStr.startsWith("{")) {
                            // 尝试解析JSON字符串
                            descData = objectMapper.readValue(descStr, new TypeReference<Map<String, Object>>() {});
                        } else if (!descStr.isEmpty()) {
                            log.warn("SIS系统的desc字段不是有效的JSON格式: {}", descStr);
                        }
                    } else {
                        log.warn("SIS系统的desc字段类型不是Map或String，无法处理: {}", desc.getClass().getName());
                    }

                    // 将解析后的数据添加到extraFieldData
                    if (!descData.isEmpty()) {
                        extraFieldData.putAll(descData);
                        // 扩展字段：保留原始描述数据
                        extraFieldData.put("sis_ext_field", descData);
                        // EPC 需要汇总所有文本用于检索
                        if ("EPC".equals(systemName)) {
                            List<String> searchValues = new ArrayList<>();
                            // 简单递归收集文本
                            collectDescValues(descData, searchValues);
                            if (!searchValues.isEmpty()) {
                                extraFieldData.put("weichai_need_search", String.join(" ", searchValues));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("解析SIS系统的desc字段失败: {}", e.getMessage(), e);
                log.error("原始desc内容: {}", desc);
            }
        } else if (!"WPROS_STRUCT".equals(systemName)) {
            List<String> systemNameList = generatePathList(systemName, filePath);
            extraFieldData.put("weichai_system", systemNameList);
            extraFieldData.put("is_system", 1);
            extraFieldData.put("weichai_fileid", fileId);
            extraFieldData.put("weichai_file_number", fileNumber);
            extraFieldData.put("weichai_version", version);
        } else {
            extraFieldData.put("weichai_system", Arrays.asList(systemName));
            extraFieldData.put("weichai_skip_url", 
                String.format("https://wpros.weichai.com/viewer/processes?id=%s&mod=chart", fileId));
            extraFieldData.put("is_system", 1);
            extraFieldData.put("weichai_fileid", fileId);
            extraFieldData.put("weichai_file_number", fileNumber);
            extraFieldData.put("weichai_version", version);
        }
        
        return extraFieldData;
    }
    
    /**
     * 生成路径列表
     */
    private List<String> generatePathList(String systemName, String filePath) {
        String[] pathParts = filePath.split("/");
        List<String> result = new ArrayList<>();
        result.add(systemName);
        
        String currentPath = systemName;
        for (String part : pathParts) {
            if (part != null && !part.isEmpty()) {
                currentPath += "_" + part;
                result.add(currentPath);
            }
        }
        
        return result;
    }
    
    // 递归收集 desc 中的所有文本值（用于 EPC 搜索扩展）
    private void collectDescValues(Object value, List<String> out) {
        if (value == null) return;
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) value;
            for (Object v : m.values()) {
                collectDescValues(v, out);
            }
        } else if (value instanceof Iterable) {
            for (Object v : (Iterable<?>) value) {
                collectDescValues(v, out);
            }
        } else {
            String text = String.valueOf(value).trim();
            if (!text.isEmpty()) out.add(text);
        }
    }
    
    /**
     * 从用户列表中提取用户ID，支持两种格式：
     * 1. List<String> - 直接是用户ID字符串列表
     * 2. List<Map<String, String>> - 包含id字段的Map列表
     */
    private List<String> extractUserIds(List<Object> userList) {
        if (userList == null || userList.isEmpty()) {
            return new ArrayList<>();
        }
        
        return userList.stream()
            .map(user -> {
                if (user instanceof String) {
                    // 如果用户是字符串类型，直接返回
                    return (String) user;
                } else if (user instanceof Map) {
                    // 如果用户是Map类型，提取id字段
                    @SuppressWarnings("unchecked")
                    Map<String, Object> userMap = (Map<String, Object>) user;
                    Object id = userMap.get("id");
                    return id != null ? id.toString() : null;
                } else {
                    // 其他类型，尝试转换为字符串
                    return user != null ? user.toString() : null;
                }
            })
            .filter(Objects::nonNull)
            .filter(id -> !id.isEmpty())
            .collect(Collectors.toList());
    }
    
    /**
     * 从角色列表中提取角色ID，支持两种格式：
     * 1. List<Map<String, String>> - 每个元素是包含id字段的Map
     * 2. List<String> - 每个元素直接是角色ID字符串
     */
    private String extractRoleIds(List<Object> roleList) {
        if (roleList == null || roleList.isEmpty()) {
            return null;
        }
        
        return roleList.stream()
            .map(role -> {
                if (role instanceof String) {
                    // 如果角色是字符串类型，直接返回
                    return (String) role;
                } else if (role instanceof Map) {
                    // 如果角色是Map类型，提取id字段
                    @SuppressWarnings("unchecked")
                    Map<String, Object> roleMap = (Map<String, Object>) role;
                    Object id = roleMap.get("id");
                    return id != null ? id.toString() : null;
                } else {
                    // 其他类型，尝试转换为字符串
                    return role != null ? role.toString() : null;
                }
            })
            .filter(Objects::nonNull)
            .filter(id -> !id.isEmpty())
            .collect(Collectors.joining(","));
    }
    
    /**
     * 创建错误响应
     */
    private Map<String, Object> createErrorResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", message);
        response.put("timestamp", LocalDateTime.now().toString());
        return response;
    }
    
    /**
     * 响应式错误日志记录
     */
    private Mono<Void> logErrorReactive(int errorType, String fileId, String step, 
            String errorMsg, Map<String, Object> params) {
        return Mono.fromRunnable(() -> {
            try {
                errorHandler.logError(errorType, fileId, step, errorMsg, params);
                
                // 使用新的日志工具记录消息处理错误
                String operation = this.getClass().getSimpleName() + ".processFileAddMessage";
                LoggingUtils.logMessageError(operation, fileId, step, 
                    new RuntimeException(errorMsg), params);
                    
                log.error("错误类型: {}, 文件: {}, 步骤: {}, 消息: {}", 
                    errorType, fileId, step, errorMsg);
            } catch (Exception e) {
                log.error("记录错误日志时发生异常", e);
            }
        });
    }
    
    /**
     * 记录异常错误 - 响应式版本
     */
    private Mono<Void> logExceptionReactive(int errorType, String fileId, String step, 
            Throwable exception, Map<String, Object> params) {
        return Mono.fromRunnable(() -> {
            try {
                // 使用增强的错误处理器记录异常
                errorHandler.logException(errorType, fileId, step, exception, params);
                
                // 使用新的日志工具记录消息处理错误
                String operation = this.getClass().getSimpleName() + ".processFileAddMessage";
                LoggingUtils.logMessageError(operation, fileId, step, exception, params);
                    
            } catch (Exception e) {
                log.error("记录异常日志时发生异常", e);
            }
        });
    }

    /**
     * 步骤2扩展: 查询系统信息（如缺失则自动创建部门与管理员后重试）
     */
    private Mono<ProcessingContext> querySystemInfoWithAutoCreate(ProcessingContext context) {
        log.info("步骤2: 开始查询系统信息（自动创建兜底），系统名称: {}", context.systemName);
        return reactiveKnowledgeHandler.querySystemInfo(context.systemName)
            .onErrorResume(err -> {
                log.warn("系统[{}]初次查询失败，尝试自动创建部门与用户: {}", context.systemName, err.getMessage());
                return createDepartmentAndUser(context)
                    .then(reactiveKnowledgeHandler.querySystemInfo(context.systemName));
            })
            .flatMap(systemInfo -> {
                String departmentId = (String) systemInfo.get("department_guid");
                String adminOpenId = (String) systemInfo.get("admin_open_id");
                if (departmentId == null || departmentId.isEmpty() || adminOpenId == null || adminOpenId.isEmpty()) {
                    log.warn("系统[{}]缺少关键信息(department/admin)，尝试自动创建并重查", context.systemName);
                    return createDepartmentAndUser(context)
                        .then(reactiveKnowledgeHandler.querySystemInfo(context.systemName))
                        .map(si -> {
                            context.departmentId = (String) si.get("department_guid");
                            context.adminOpenId = (String) si.get("admin_open_id");
                            return context;
                        });
                } else {
                    context.departmentId = departmentId;
                    context.adminOpenId = adminOpenId;
                    log.info("查询系统信息成功，部门ID: {}, 管理员ID: {}", context.departmentId, context.adminOpenId);
                    return Mono.just(context);
                }
            })
            .retryWhen(Retry.backoff(5, Duration.ofSeconds(1))
                .filter(throwable -> !(throwable instanceof IllegalArgumentException))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    log.error("自动创建后查询系统信息仍失败，已重试{}次", retrySignal.totalRetries());
                    return new RuntimeException(
                        String.format("查询系统信息失败（已重试%d次）", retrySignal.totalRetries()),
                        retrySignal.failure());
                }));
    }

    /**
     * 为系统自动创建部门与默认管理员用户（幂等）
     */
    private Mono<Void> createDepartmentAndUser(ProcessingContext context) {
        String systemId = "system_" + context.systemName;
        log.info("准备为系统[{}]创建部门与默认用户，system_id={}", context.systemName, systemId);
        return reactiveKnowledgeHandler.manageSystemDepartment("CREATE", systemId)
            .flatMap(deptResult -> {
                Integer code = (Integer) deptResult.get("returnCode");
                if (code == null || code != 200) {
                    log.error("创建系统部门失败: {}", deptResult);
                    return Mono.error(new RuntimeException("创建系统部门失败: " + deptResult.get("returnMessage")));
                }
                log.info("部门创建成功，继续创建默认用户");
                return reactiveKnowledgeHandler.manageUser(
                    "CREATE",
                    systemId, // userOpenId
                    systemId, // userName
                    systemId, // deptOpenId
                    systemId, // deptName
                    null,     // tenantId -> 默认
                    null      // deleted  -> 默认
                );
            })
            .map(userResult -> {
                Integer code = (Integer) userResult.get("returnCode");
                if (code == null || code != 200) {
                    log.error("创建默认用户失败: {}", userResult);
                } else {
                    log.info("默认用户创建成功");
                }
                return userResult;
            })
            .then();
    }
}

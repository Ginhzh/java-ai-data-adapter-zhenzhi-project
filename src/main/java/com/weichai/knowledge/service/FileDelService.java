package com.weichai.knowledge.service;

import com.weichai.knowledge.entity.FileDel;
import com.weichai.knowledge.redis.ReactiveRedisManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.*;

/**
 * FILE_DEL消息处理服务（响应式版本）
 * 对应Python中的knowledge_delete_service.py
 */
@Slf4j
@Service
public class FileDelService {
    
    @Autowired
    private ReactiveKnowledgeHandler reactiveKnowledgeHandler;

    @Autowired
    private ReactiveRedisManager reactiveRedisManager;

    private static final String DELETE_VERIFICATION_KEY_FMT = "%s:knowledge:delete:verification:passed:%s";
    private static final String DELETE_SUCCESS_KEY_FMT = "%s:knowledge:delete:operation:success:%s";
    private static final String HASH_MESSAGE_FIELD = "message_count";
    private static final String HASH_DOCUMENT_FIELD = "document_count";
    
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
        
        // 如果最后一个部分包含扩展名，则移除它
        String pathOnly = filePath;
        String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
        if (fileName.contains(".") && fileName.split("\\.").length > 1) {
            // 只有当最后部分是文件名（包含扩展名）时才移除
            int lastSlashIndex = filePath.lastIndexOf('/');
            pathOnly = lastSlashIndex > 0 ? filePath.substring(0, lastSlashIndex) : filePath;
        }
        
        // 按斜杠分割路径
        String[] pathParts = pathOnly.replaceAll("^/|/$", "").split("/");
        
        // 过滤掉空字符串
        List<String> validParts = new ArrayList<>();
        for (String part : pathParts) {
            if (!part.isEmpty()) {
                validParts.add(part);
            }
        }
        
        // 处理没有有效路径片段的情况
        if (validParts.isEmpty()) {
            return "默认路径";
        }
        
        // 根据系统名称选择目录
        List<String> selectedParts;
        if ("WPROS".equals(systemName) || "WPROS_TEST".equals(systemName)) {
            // 取前三层目录，如果不足三层则全部保留
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
    
    /**
     * 处理结果记录类
     */
    private static class ProcessingResult {
        boolean success = false;
        String errorMessage = "";
        String zhenzhiFileId = "";
        String repoId = "";
        int deleteStatus = 0; // 0: 失败, 1: 成功
        
        ProcessingResult(boolean success, String errorMessage) {
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }
    
    /**
     * 处理FILE_DEL消息 - 响应式版本
     * 重构版本：每个步骤失败后继续执行，最终统一写入状态表
     * 
     * @param fileDel 文件删除消息对象
     * @return 处理结果Map的Mono
     */
    public Mono<Map<String, Object>> processFileDelMessage(FileDel fileDel) {
        log.info("开始处理文件删除消息: {}", fileDel);
        
        ProcessingResult result = new ProcessingResult(false, "");
        
        return extractBasicInfo(fileDel)
            .flatMap(metadata -> {
                String fileId = fileDel.getFileId();
                String systemName = metadata.getSystemName();
                String fileNumber = metadata.getFileNumber();
                String fileName = metadata.getFileName();
                String filePath = metadata.getFilePath();
                String version = metadata.getVersion();
                
                log.info("开始处理系统[{}]的文件[{}]，文件ID[{}]", systemName, fileName, fileId);
                
                // 步骤1：查询甄知文档ID
                return queryZhenzhiId(fileNumber, systemName, result)
                    // 步骤2：查询知识库映射（并行执行，不依赖甄知ID）
                    .zipWith(queryRepoMapping(filePath, systemName, result))
                    // 步骤3：执行文档删除（只有前面步骤都成功才执行）
                    .flatMap(tuple -> {
                        boolean step1Success = tuple.getT1();
                        boolean step2Success = tuple.getT2();

                        if (step1Success && step2Success) {
                            return incrHashCounters(getVerificationKey(systemName), systemName, "delete verification")
                                .then(executeDocumentDeletion(result.repoId, result.zhenzhiFileId, 
                                    fileName, systemName, result));
                        } else {
                            log.warn("跳过文档删除：前置条件未满足");
                            result.errorMessage = "前置步骤失败，跳过文档删除";
                            return Mono.just(false);
                        }
                    })
                    .doOnNext(step3Success -> {
                        result.deleteStatus = step3Success ? 1 : 0;
                        result.success = step3Success;
                        if (step3Success) {
                            incrHashCounters(getSuccessKey(systemName), systemName, "delete success").subscribe();
                        }
                    })
                    // 步骤4：无论前面成功与否，都要写入状态表
                    .then(recordDocumentStatus(systemName, fileId, fileNumber, result.deleteStatus, 
                        result.repoId, result.zhenzhiFileId))
                    .doOnNext(statusRecordSuccess -> {
                        if (!statusRecordSuccess) {
                            log.error("写入文档状态表失败，系统[{}]，文件ID[{}]", systemName, fileId);
                        }
                    })
                    // 返回最终结果
                    .then(Mono.fromCallable(() -> createFinalResult(
                        result.success, systemName, fileId, fileNumber, fileName,
                        result.zhenzhiFileId, version, result.deleteStatus, result.errorMessage)));
            })
            .onErrorResume(error -> {
                log.error("处理文件删除时出现异常: {}", error.getMessage(), error);
                result.errorMessage = "处理异常: " + error.getMessage();
                result.success = false;
                
                return Mono.just(createFinalErrorResult("处理异常", error.getMessage()));
            });
    }
    
    /**
     * 提取和验证基础信息 - 响应式版本
     */
    private Mono<FileDel.FileMetadata> extractBasicInfo(FileDel fileDel) {
        return Mono.fromCallable(() -> {
            if (fileDel == null) {
                throw new IllegalArgumentException("fileDel对象不能为空");
            }
            
            String fileId = fileDel.getFileId();
            if (fileId == null || fileId.isEmpty()) {
                throw new IllegalArgumentException("fileId不能为空");
            }
            
            FileDel.FileMetadata metadata = fileDel.getFileMetadata();
            if (metadata == null) {
                throw new IllegalArgumentException("fileMetadata不能为空");
            }
            
            String systemName = metadata.getSystemName();
            String fileNumber = metadata.getFileNumber();
            String fileName = metadata.getFileName();
            String filePath = metadata.getFilePath();
            
            if (systemName == null || fileNumber == null || fileName == null || filePath == null) {
                throw new IllegalArgumentException("文件元数据缺少必要字段");
            }
            
            return metadata;
        });
    }
    
    /**
     * 查询甄知文档ID - 响应式版本
     */
    private Mono<Boolean> queryZhenzhiId(String fileNumber, String systemName, ProcessingResult result) {
        return reactiveKnowledgeHandler.queryZhenzhiFileId(fileNumber, systemName)
            .map(queryResult -> {
                log.info("查询甄知ID映射结果: {}", queryResult);
                
                if (queryResult == null) {
                    result.errorMessage = String.format("查询[%s]的甄知ID失败", fileNumber);
                    return false;
                }
                
                String zhenzhiFileId = (String) queryResult.get("zhenzhi_file_id");
                if (zhenzhiFileId == null || zhenzhiFileId.isEmpty()) {
                    result.errorMessage = String.format("无法获取[%s]的甄知ID", fileNumber);
                    return false;
                }
                
                result.zhenzhiFileId = zhenzhiFileId;
                return true;
            })
            .onErrorResume(error -> {
                result.errorMessage = "查询甄知ID异常: " + error.getMessage();
                log.warn("步骤1失败：{}", result.errorMessage);
                return Mono.just(false);
            });
    }
    
    /**
     * 查询知识库映射 - 响应式版本
     */
    private Mono<Boolean> queryRepoMapping(String filePath, String systemName, ProcessingResult result) {
        String repoName = formatRepoName(filePath, systemName);
        
        return reactiveKnowledgeHandler.queryRepoMapping(systemName, repoName)
            .map(repoMapping -> {
                log.info("查询知识库映射结果: {}", repoMapping);
                
                if (repoMapping == null) {
                    result.errorMessage = "未找到知识库映射";
                    return false;
                }
                
                String repoId = (String) repoMapping.get("repo_id");
                if (repoId == null || repoId.isEmpty()) {
                    result.errorMessage = "知识库映射中未找到repo_id";
                    return false;
                }
                
                result.repoId = repoId;
                log.info("已找到知识库映射，知识库ID: {}", repoId);
                return true;
            })
            .onErrorResume(error -> {
                result.errorMessage = "查询知识库映射异常: " + error.getMessage();
                log.warn("步骤2失败：{}", result.errorMessage);
                return Mono.just(false);
            });
    }
    
    /**
     * 执行文档删除操作 - 使用最新的deleteDocument函数
     */
    private Mono<Boolean> executeDocumentDeletion(String repoId, String zhenzhiFileId, 
                                                String fileName, String systemName, 
                                                ProcessingResult result) {
        return reactiveKnowledgeHandler.querySystemInfo(systemName)
            .flatMap(systemInfo -> {
                log.info("查询系统信息成功: {}", systemInfo);
                
                String adminOpenId = (String) systemInfo.get("admin_open_id");
                if (adminOpenId == null || adminOpenId.isEmpty()) {
                    result.errorMessage = "未找到系统管理员OpenID";
                    return Mono.just(false);
                }
                
                // 使用最新的deleteDocument函数
                return reactiveKnowledgeHandler.deleteDocument(repoId, zhenzhiFileId, true, adminOpenId)
                    .map(deleteResult -> {
                        // 判断文档删除是否成功
                        Boolean success = (Boolean) deleteResult.get("success");
                        Integer returnCode = (Integer) deleteResult.get("returnCode");
                        String returnMessage = (String) deleteResult.get("returnMessage");
                        
                        if (Boolean.TRUE.equals(success) && returnCode != null && returnCode == 200) {
                            log.info("文档删除成功");
                            return true;
                        } else {
                            result.errorMessage = String.format("文档删除失败: %s", 
                                deleteResult.getOrDefault("returnMessage", "未知错误"));
                            return false;
                        }
                    });
            })
            .onErrorResume(error -> {
                result.errorMessage = "执行文档删除异常: " + error.getMessage();
                log.warn("步骤3失败：{}", result.errorMessage);
                return Mono.just(false);
            });
    }
    
    /**
     * 记录文档状态 - 响应式版本
     */
    private Mono<Boolean> recordDocumentStatus(String systemName, String fileId, String fileNumber, 
                                             int deleteStatus, String repoId, String zhenzhiFileId) {
        return reactiveKnowledgeHandler.upsertDocumentStatus(
                systemName, fileId, fileNumber, deleteStatus, repoId, zhenzhiFileId)
            .map(recordResult -> {
                if (Boolean.TRUE.equals(recordResult.get("success"))) {
                    log.info("更新文档{}状态记录成功，删除状态：{}", fileId, deleteStatus);
                    return true;
                } else {
                    log.error("更新文档{}状态记录失败", fileId);
                    return false;
                }
            })
            .onErrorResume(error -> {
                log.error("记录文档状态异常: {}", error.getMessage(), error);
                return Mono.just(false);
            });
    }
    
    /**
     * 创建最终结果
     */
    private Map<String, Object> createFinalResult(boolean success, String systemName, 
                                                 String fileId, String fileNumber, String fileName,
                                                 String zhenzhiFileId, String version, 
                                                 int deleteStatus, String errorMessage) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", success ? "success" : "error");
        result.put("system_name", systemName);
        result.put("file_id", fileId);
        result.put("file_number", fileNumber);
        result.put("file_name", fileName);
        result.put("file_zhenzhi_id", zhenzhiFileId);
        result.put("version", version);
        result.put("is_delete", deleteStatus);
        result.put("message", success ? "文件删除处理成功" : errorMessage);
        result.put("timestamp", LocalDateTime.now().toString());
        
        return result;
    }
    
    /**
     * 创建最终错误结果（用于基础验证失败的情况）
     */
    private Map<String, Object> createFinalErrorResult(String status, String message) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "error");
        result.put("message", status + ": " + message);
        result.put("timestamp", LocalDateTime.now().toString());
        return result;
    }

    private String today() {
        return LocalDate.now().toString();
    }

    private String getVerificationKey(String systemName) {
        return String.format(DELETE_VERIFICATION_KEY_FMT, systemName, today());
    }

    private String getSuccessKey(String systemName) {
        return String.format(DELETE_SUCCESS_KEY_FMT, systemName, today());
    }

    private Mono<Void> incrHashCounters(String key, String systemName, String stage) {
        return reactiveRedisManager.hincrBy(key, HASH_MESSAGE_FIELD, 1)
            .then(reactiveRedisManager.hincrBy(key, HASH_DOCUMENT_FIELD, 1))
            .doOnSuccess(v -> log.info("Redis计数成功 [{}] 系统={} key={}", stage, systemName, key))
            .doOnError(e -> log.warn("Redis计数失败 [{}] 系统={} key={} err={}", stage, systemName, key, e.getMessage()))
            .onErrorResume(e -> Mono.empty())
            .then();
    }
}

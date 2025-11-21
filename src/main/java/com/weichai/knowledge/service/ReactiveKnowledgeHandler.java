package com.weichai.knowledge.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weichai.knowledge.config.ApplicationProperties;
import com.weichai.knowledge.dto.*;
import com.weichai.knowledge.entity.*;
import com.weichai.knowledge.exception.*;
import com.weichai.knowledge.redis.ReactiveRedisManager;
import com.weichai.knowledge.redis.ReactiveSyncManager;
import com.weichai.knowledge.repository.*;
import com.weichai.knowledge.utils.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 响应式知识库服务类，提供知识库各类操作的完全非阻塞实现
 * 
 * 主要特性：
 * - 全响应式架构，基于Mono/Flux
 * - 非阻塞I/O操作
 * - 响应式数据库访问（R2DBC）
 * - 响应式Redis缓存
 * - 链式错误处理
 * - 背压支持
 */
@Slf4j
@Service
public class ReactiveKnowledgeHandler {
    
    // AES加密密钥
    private static final String MATCH_API_AES_KEY = "psGG1kULp/8Lko022q836w==";
    
    // 缓存超时时间（秒）
    private static final Duration CACHE_TIMEOUT = Duration.ofHours(1);
    private static final Duration SYSTEM_INFO_CACHE_TIMEOUT = Duration.ofHours(2);
    
    // 服务依赖 - 响应式版本
    @Autowired
    private ReactiveRedisManager reactiveRedisManager;
    
    @Autowired
    private ReactiveSyncManager reactiveSyncManager;
    
    @Autowired
    private ErrorHandler errorHandler;
    
    @Autowired
    private ApplicationProperties applicationProperties;
    
    @Autowired
    private DatabaseClient databaseClient;
    
    // 响应式Repository
    @Autowired
    private ReactiveRepoMappingRepository repoMappingRepository;
    
    @Autowired
    private ReactiveUnstructuredDocumentRepository unstructuredDocumentRepository;
    
    @Autowired
    private ReactiveDocumentStatusRepository documentStatusRepository;
    
    // HTTP客户端
    @Autowired
    private WebClient webClient;
    
    // 请求头缓存
    private final Map<String, String> baseHeaders = new ConcurrentHashMap<>();
    
    // 初始化标记
    private volatile boolean initialized = false;
    
    // 对象映射器
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Spring初始化方法 - 修复为同步初始化
     */
    @PostConstruct
    public void init() {
        try {
            if (!initialized) {
                initHeaders();
                initialized = true;
                log.info("ReactiveKnowledgeHandler基础初始化完成");
            }

            // 异步启动同步任务，不阻塞初始化过程
            registerSyncTasksAsync();

        } catch (Exception error) {
            log.error("ReactiveKnowledgeHandler初始化失败", error);
            throw new RuntimeException("Failed to initialize ReactiveKnowledgeHandler", error);
        }
    }

    /**
     * 异步注册同步任务
     */
    private void registerSyncTasksAsync() {
        registerSyncTasks()
            .doOnSuccess(v -> log.info("响应式同步管理器启动成功"))
            .doOnError(error -> log.error("响应式同步管理器启动失败", error))
            .subscribe(); // 异步执行，不阻塞初始化
    }
    
    /**
     * 初始化请求头
     */
    private void initHeaders() {
        baseHeaders.put("Content-Type", "application/json");
        baseHeaders.put("superhelper-login-state", "matchApi");
        baseHeaders.put("x-match-api-signature", applicationProperties.getApi().getMatchSignature());
        log.info("成功设置API基础请求头");
    }
    
    /**
     * 注册同步任务 - 响应式版本
     */
    private Mono<Void> registerSyncTasks() {
        return Mono.fromRunnable(() -> {
            reactiveSyncManager.registerSyncTask("repo_mapping_sync", RepoMapping.class, "repo_mapping");
            reactiveSyncManager.registerSyncTask("unstructured_document_sync", UnstructuredDocument.class, "unstructured_document");
            log.info("响应式同步任务注册完成");
        })
        .then(reactiveSyncManager.start(Duration.ofMinutes(5)))
        .doOnSuccess(v -> log.info("响应式同步管理器启动成功"))
        .then();
    }
    
    /**
     * 生成包含签名的HttpHeaders - 响应式安全版本
     */
    public Mono<HttpHeaders> generateHeadersWithSignature(String adminOpenId) {
        return generateSignature(adminOpenId)
            .map(signature -> {
                HttpHeaders headers = new HttpHeaders();
                baseHeaders.forEach(headers::add);
                headers.set("x-match-api-signature", signature);
                log.debug("成功为admin_open_id={}生成专用请求头", adminOpenId);
                return headers;
            })
            .onErrorMap(error -> {
                log.error("为admin_open_id={}生成API签名失败", adminOpenId, error);
                return new IllegalStateException("生成API签名失败", error);
            });
    }
    
    /**
     * 查询系统信息 - 完全响应式实现
     */
    public Mono<Map<String, Object>> querySystemInfo(String systemName) {
        String normalizedSystemName = Optional.ofNullable(systemName)
            .map(String::toLowerCase)
            .orElse("");
            
        String cacheKey = generateCacheKey("system_info", normalizedSystemName);
        
        return reactiveRedisManager.getMap(cacheKey)
            .doOnNext(cached -> log.debug("从Redis缓存获取系统信息: {}", normalizedSystemName))
            .switchIfEmpty(
                callSystemInfoApi(normalizedSystemName, cacheKey)
                    .flatMap(apiResult -> cacheSystemInfo(cacheKey, apiResult))
            )
            .onErrorResume(error -> {
                log.error("查询系统信息失败: system={}, error={}", normalizedSystemName, error.getMessage());
                return Mono.error(new RuntimeException("查询系统信息失败: " + error.getMessage(), error));
            });
    }
    
    /**
     * 调用系统信息API - 响应式实现，增强错误处理
     */
    private Mono<Map<String, Object>> callSystemInfoApi(String normalizedSystemName, String cacheKey) {
        log.info("调用查询系统信息API，system={}", normalizedSystemName);

        Map<String, Object> requestLog = Map.of(
                "system", "system_" + normalizedSystemName
        );

        Mono<Map<String, Object>> pipeline = webClient.get()
            .uri(uriBuilder -> uriBuilder
                .path("/super-help/openapi/v1/querySystemInfo")
                .queryParam("system", "system_" + normalizedSystemName)
                .build())
            .headers(httpHeaders -> baseHeaders.forEach(httpHeaders::add))
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .doOnSuccess(result -> {
                    if (result == null) {
                        // 这个日志触发，说明我的上一个回答是正确的：流成功完成但为空
                        log.info("[诊断日志] bodyToMono 成功，但结果为 null (空响应体)。");
                    }
                })
                .doOnNext(result -> {
                    // 这个日志触发，说明JSON解析成功，我们可以看到内容
                    log.info("[诊断日志] bodyToMono 成功，成功解析出数据: {}", result);
                })
                .doOnError(error -> {
                    // 这个日志触发，说明在 bodyToMono 阶段就出错了，例如JSON解码失败
                    log.error("[诊断日志] bodyToMono 失败，错误类型: {}, 错误信息: {}",
                            error.getClass().getName(), error.getMessage());
                })
            .timeout(Duration.ofSeconds(10)) // 添加超时控制
            .flatMap(this::validateSystemInfoResponse)
            .flatMap(this::extractSystemInfoData)
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                .filter(throwable -> {
                    // 不重试验证异常，但重试网络异常
                    if (throwable instanceof ValidationException) {
                        return false;
                    }
                    log.warn("API调用失败，准备重试: {}", throwable.getMessage());
                    return true;
                })
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    Throwable lastFailure = retrySignal.failure();
                    String errorMsg = String.format("查询系统信息API重试耗尽，最后错误: %s", lastFailure.getMessage());
                    log.error("API重试耗尽: system={}, 重试次数: {}, 最后错误: {}",
                        normalizedSystemName, retrySignal.totalRetries(), lastFailure.getMessage());
                    return new ExternalApiException("查询系统信息", errorMsg, 500);
                }))
            .doOnNext(result -> log.info("查询系统信息API成功: system={}", normalizedSystemName))
            .onErrorMap(error -> {
                if (error instanceof ApiException) {
                    return error;
                }
                String errorMsg = String.format("调用系统信息API失败: %s", error.getMessage());
                log.error("系统信息API调用异常: system={}, error={}", normalizedSystemName, errorMsg, error);
                return new ExternalApiException("查询系统信息", errorMsg, 500);
            });

        return logExternalCall("/super-help/openapi/v1/querySystemInfo", requestLog, pipeline);
    }
    
    /**
     * 验证系统信息API响应
     */
    private Mono<Map<String, Object>> validateSystemInfoResponse(Map<String, Object> apiResult) {
        if (apiResult == null || !Integer.valueOf(200).equals(apiResult.get("code"))) {
            String errorMsg = "查询系统信息失败: " + 
                (apiResult != null ? apiResult.getOrDefault("msg", "未知错误") : "响应为空");
            return Mono.error(new ExternalApiException("查询系统信息", errorMsg, 500));
        }
        return Mono.just(apiResult);
    }
    
    /**
     * 提取系统信息数据
     */
    @SuppressWarnings("unchecked")
    private Mono<Map<String, Object>> extractSystemInfoData(Map<String, Object> apiResult) {
        return Mono.fromCallable(() -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) apiResult.get("data");
            String adminOpenId = (String) data.get("thirdUserId");
            String departmentGuid = (String) data.get("departmentGuid");
            
            if (adminOpenId == null || departmentGuid == null) {
                throw new ValidationException("系统信息数据不完整: " + data);
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("department_guid", departmentGuid);
            result.put("admin_open_id", adminOpenId);
            result.put("message", apiResult.getOrDefault("msg", "ok"));
            
            return result;
        });
    }
    
    /**
     * 缓存系统信息
     */
    private Mono<Map<String, Object>> cacheSystemInfo(String cacheKey, Map<String, Object> result) {
        return reactiveRedisManager.set(cacheKey, result, SYSTEM_INFO_CACHE_TIMEOUT)
            .doOnSuccess(v -> log.debug("成功缓存系统信息: key={}", cacheKey))
            .onErrorResume(error -> {
                log.warn("缓存系统信息失败，但不影响返回结果: key={}, error={}", cacheKey, error.getMessage());
                return Mono.empty();
            })
            .thenReturn(result);
    }
    
    /**
     * 查询知识库映射关系 - 响应式实现
     */
    public Mono<Map<String, Object>> queryRepoMapping(String systemName, String filePath) {
        String cacheKey = generateCacheKey("repo_mapping", systemName, filePath);
        
        return reactiveRedisManager.getMap(cacheKey)
            .doOnNext(cached -> log.debug("从Redis获取知识库映射: {}/{}", systemName, filePath))
            .switchIfEmpty(
                repoMappingRepository.findBySystemNameAndFilePath(systemName, filePath)
                    .flatMap(this::convertRepoMappingToMap)
                    .flatMap(mapping -> cacheRepoMapping(cacheKey, mapping))
            )
            .onErrorResume(error -> {
                log.error("查询知识库映射失败: system={}, path={}, error={}", 
                    systemName, filePath, error.getMessage());
                return logErrorReactive(2, "unknown", "query_repo_mapping", 
                    "查询知识库映射失败: " + error.getMessage(),
                    Map.of("system_name", systemName, "file_path", filePath))
                    .then(Mono.error(new RuntimeException("查询知识库映射失败", error)));
            });
    }
    
    /**
     * 转换RepoMapping实体为Map
     */
    private Mono<Map<String, Object>> convertRepoMappingToMap(RepoMapping mapping) {
        return Mono.fromCallable(() -> {
            Map<String, Object> result = new HashMap<>();
            result.put("id", mapping.getId());
            result.put("system_name", mapping.getSystemName());
            result.put("file_path", mapping.getFilePath());
            result.put("repo_id", mapping.getRepoId());
            result.put("department_id", mapping.getDepartmentId());
            result.put("created_at", mapping.getCreatedAt().toString());
            result.put("updated_at", mapping.getUpdatedAt().toString());
            return result;
        });
    }
    
    /**
     * 缓存知识库映射
     */
    private Mono<Map<String, Object>> cacheRepoMapping(String cacheKey, Map<String, Object> mapping) {
        return reactiveRedisManager.set(cacheKey, mapping, CACHE_TIMEOUT)
            .doOnSuccess(v -> log.debug("成功缓存知识库映射: key={}", cacheKey))
            .onErrorResume(error -> {
                log.warn("缓存知识库映射失败: key={}, error={}", cacheKey, error.getMessage());
                return Mono.empty();
            })
            .thenReturn(mapping);
    }
    
    /**
     * 创建知识库 - 响应式事务处理
     */
    @Transactional
    public Mono<Map<String, Object>> createRepository(String systemName, String filePath, 
            String departmentId, String repoName, String intro, Integer scope, HttpHeaders headers) {
        
        return validateCreateRepositoryParams(systemName, filePath, departmentId, repoName)
            .then(checkRepositoryExists(systemName, repoName))
            .then(callCreateRepositoryApi(departmentId, intro, repoName, scope, headers))
            .flatMap(apiResponse -> extractRepositoryId(apiResponse, systemName, repoName))
            .flatMap(repoId -> createRepoMappingByInsert(systemName, repoName, repoId, departmentId)
                .thenReturn(buildCreateRepositoryResult(repoId, "知识库创建成功并保存映射记录")))
            .onErrorMap(error -> {
                if (error instanceof ApiException) {
                    return error;
                }
                String errorMsg = "创建知识库过程中发生错误: " + error.getMessage();
                return ApiException.internalError(errorMsg, error);
            });
    }
    
    /**
     * 验证创建知识库参数
     */
    private Mono<Void> validateCreateRepositoryParams(String systemName, String filePath, 
            String departmentId, String repoName) {
        return Mono.fromRunnable(() -> {
            if (systemName == null || filePath == null || departmentId == null || repoName == null) {
                throw new ValidationException("系统名称、文件路径、部门ID和知识库名称不能为空");
            }
        });
    }
    
    /**
     * 检查知识库是否已存在
     */
    private Mono<Void> checkRepositoryExists(String systemName, String repoName) {
        return repoMappingRepository.findBySystemNameAndFilePath(systemName, repoName)
            .hasElement()
            .flatMap(exists -> {
                if (exists) {
                    String errorMsg = String.format("知识库映射已存在: 系统=%s, 路径=%s", systemName, repoName);
                    return logErrorReactive(3, systemName, "create_repository", errorMsg,
                        Map.of("system_name", systemName, "file_path", repoName))
                        .then(Mono.error(BusinessException.resourceExists("知识库映射", systemName + "/" + repoName)));
                }
                return Mono.empty();
            });
    }
    
    /**
     * 调用创建知识库API
     */
    private Mono<ApiResponse<String>> callCreateRepositoryApi(String departmentId, String intro, 
            String repoName, Integer scope, HttpHeaders headers) {
        
        CreateRepoRequest requestBody = new CreateRepoRequest(departmentId, intro, repoName, scope);

        Mono<ApiResponse<String>> pipeline = webClient.post()
                .uri("/path_wiki/wiki/ku/openapi/v3/repo/createRepo")
                .headers(httpHeaders -> httpHeaders.addAll(headers))
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<ApiResponse<String>>() {})
                .doOnNext(response -> log.info("创建知识库API响应: {}", response));

        return logExternalCall("/path_wiki/wiki/ku/openapi/v3/repo/createRepo", requestBody, pipeline);
    }
    
    /**
     * 提取知识库ID并验证
     */
    private Mono<String> extractRepositoryId(ApiResponse<String> response, String systemName, String repoName) {
        return Mono.fromCallable(() -> {
            if (response == null || !response.isSuccess()) {
                String errorMsg = "创建知识库API调用失败: " + 
                    (response != null ? response.getErrorMessage() : "响应为空");
                
                // 检查是否为知识库名称重复错误
                String returnMessage = response != null ? response.getErrorMessage() : "";
                if (returnMessage != null && returnMessage.contains("repository name dup")) {
                    throw BusinessException.resourceExists("知识库", repoName);
                }
                
                int errorCode = response != null ? 
                    (response.returnCode() != null ? response.returnCode() : 500) : 500;
                throw new ExternalApiException("创建知识库", errorMsg, errorCode);
            }
            
            String repoId = response.getResultData();
            if (repoId == null || repoId.isEmpty()) {
                throw new ExternalApiException("创建知识库", "成功但未返回知识库ID", 500);
            }
            
            return repoId;
        });
    }
    
    /**
     * 创建知识库映射记录
     */
    private Mono<RepoMapping> createRepoMapping(String systemName, String filePath, String repoId, String departmentId) {
        return Mono.fromCallable(() -> {
            RepoMapping newMapping = new RepoMapping();
            // 让R2DBC识别为INSERT：不预先设置ID，由数据库生成或使用默认策略
            newMapping.setSystemName(systemName);
            newMapping.setFilePath(filePath);
            newMapping.setRepoId(repoId);
            newMapping.setDepartmentId(departmentId);
            newMapping.setCreatedAt(LocalDateTime.now());
            newMapping.setUpdatedAt(LocalDateTime.now());
            return newMapping;
        })
        .flatMap(repoMappingRepository::save)
        .doOnSuccess(mapping -> log.info("成功创建知识库映射记录: repoId={}", repoId));
    }
    
    /**
     * 构建创建知识库结果
     */
    private Map<String, Object> buildCreateRepositoryResult(String repoId, String message) {
        Map<String, Object> result = new HashMap<>();
        result.put("returnCode", 200);
        result.put("returnMessage", message);
        result.put("result", repoId);
        result.put("traceId", UUID.randomUUID().toString());
        return result;
    }
    
    /**
     * 显式插入知识库映射记录，避免因@ID为空或非空导致的R2DBC行为差异
     */
    private Mono<RepoMapping> createRepoMappingByInsert(String systemName, String filePath, String repoId, String departmentId) {
        String id = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();
        
        String insertSql = "INSERT INTO repo_mappings " +
                "(id, system_name, file_path, repo_id, department_id, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        return databaseClient.sql(insertSql)
                .bind(0, id)
                .bind(1, systemName)
                .bind(2, filePath)
                .bind(3, repoId)
                .bind(4, departmentId)
                .bind(5, now)
                .bind(6, now)
                .fetch()
                .rowsUpdated()
                .flatMap(rows -> {
                    if (rows == null || rows == 0) {
                        return Mono.error(new RuntimeException("插入repo_mappings失败，未影响任何行"));
                    }
                    RepoMapping mapping = new RepoMapping();
                    mapping.setId(id);
                    mapping.setSystemName(systemName);
                    mapping.setFilePath(filePath);
                    mapping.setRepoId(repoId);
                    mapping.setDepartmentId(departmentId);
                    mapping.setCreatedAt(now);
                    mapping.setUpdatedAt(now);
                    return Mono.just(mapping);
                })
                .doOnSuccess(m -> log.info("成功创建知识库映射记录: system={}, filePath={}, repoId={}", systemName, filePath, repoId))
                .onErrorMap(e -> new RuntimeException("创建知识库映射记录失败: " + e.getMessage(), e));
    }
    
    /**
     * 导入文件到知识库 - 响应式实现
     */
    public Mono<Map<String, Object>> importToRepo(String repositoryGuid, String groupGuid,
            String bucketName, String objectKey, String fileId, String fileName,
            String extraField, String spaceGuid, HttpHeaders headers) {
        
        return validateImportParams(bucketName, objectKey)
            .then(buildImportRequest(repositoryGuid, groupGuid, bucketName, objectKey, 
                fileName, extraField, spaceGuid))
            .flatMap(requestBody -> callImportApi(requestBody, headers))
            .map(this::convertImportResponse)
            .onErrorMap(error -> {
                if (error instanceof IllegalArgumentException) {
                    return error;
                }
                log.error("导入文件到知识库过程中发生异常", error);
                return new RuntimeException("导入文件到知识库失败: " + error.getMessage(), error);
            });
    }
    
    /**
     * 验证导入参数
     */
    private Mono<Void> validateImportParams(String bucketName, String objectKey) {
        return Mono.fromRunnable(() -> {
            if (bucketName == null || bucketName.isEmpty() || 
                objectKey == null || objectKey.isEmpty()) {
                log.error("错误参数: bucketName={}, objectKey={}不能为空", bucketName, objectKey);
                throw new IllegalArgumentException("bucketName和objectKey不能为空");
            }
        });
    }
    
    /**
     * 构建导入请求
     */
    private Mono<ImportToRepoRequest> buildImportRequest(String repositoryGuid, String groupGuid,
            String bucketName, String objectKey, String fileName, String extraField, String spaceGuid) {
        
        return Mono.fromCallable(() -> {
            String finalSpaceGuid = spaceGuid != null ? spaceGuid : applicationProperties.getSpace().getGuid();
            
            return new ImportToRepoRequest(
                finalSpaceGuid, groupGuid, repositoryGuid, 
                bucketName, objectKey, fileName, extraField
            );
        });
    }
    
    /**
     * 调用导入API
     */
    private Mono<ApiResponse<Object>> callImportApi(ImportToRepoRequest requestBody, HttpHeaders headers) {
        Mono<ApiResponse<Object>> pipeline = webClient.post()
                .uri("/path_wiki/wiki/ku/openapi/client/files/import")
                .headers(httpHeaders -> httpHeaders.addAll(headers))
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<ApiResponse<Object>>() {});

        return logExternalCall("/path_wiki/wiki/ku/openapi/client/files/import", requestBody, pipeline);
    }
    
    /**
     * 转换导入响应
     */
    private Map<String, Object> convertImportResponse(ApiResponse<Object> response) {
        Map<String, Object> data = new HashMap<>();
        if (response != null) {
            data.put("success", response.isSuccess());
            data.put("returnCode", response.returnCode());
            data.put("returnMessage", response.returnMessage());
            data.put("code", response.code());
            data.put("message", response.message());
            data.put("msg", response.msg());
            data.put("errorMessage", response.getErrorMessage());
            data.put("traceId", response.traceId());
            data.put("result", response.result());
            data.put("data", response.data());
            data.put("resultData", response.getResultData());
        }
        
        try {
            log.info("导入文件到知识库API响应: {}", objectMapper.writeValueAsString(data));
        } catch (Exception e) {
            log.warn("序列化响应数据时出错: {}", e.getMessage());
        }
        
        return data;
    }
    
    /**
     * 查询文档状态 - 响应式批处理
     */
    public Mono<Map<String, Object>> queryFileStatus(List<String> docGuids, int judgeMode) {
        return validateDocGuids(docGuids)
            .then(logDocGuids(docGuids))
            .then(callFileStatusApi(docGuids))
            .flatMap(response -> processFileStatusResponse(response, docGuids, judgeMode))
            .onErrorMap(error -> {
                if (error instanceof IllegalArgumentException) {
                    return error;
                }
                log.error("查询文档状态过程中发生错误", error);
                String errorMsg = "查询文档状态过程中发生错误: " + error.getMessage();
                return new RuntimeException(errorMsg, error);
            });
    }
    
    /**
     * 验证文档GUID列表
     */
    private Mono<Void> validateDocGuids(List<String> docGuids) {
        return Mono.fromRunnable(() -> {
            if (docGuids == null || docGuids.isEmpty()) {
                String errorMsg = "文档GUID列表不能为空";
                throw new IllegalArgumentException(errorMsg);
            }
        });
    }
    
    /**
     * 记录文档GUID列表
     */
    private Mono<Void> logDocGuids(List<String> docGuids) {
        return Mono.fromRunnable(() -> {
            try {
                log.info("查询文档状态的GUID列表: {}", objectMapper.writeValueAsString(docGuids));
            } catch (Exception e) {
                log.warn("序列化文档GUID列表时出错: {}", e.getMessage());
            }
        });
    }
    
    /**
     * 调用文档状态API
     */
    private Mono<Map<String, Object>> callFileStatusApi(List<String> docGuids) {
        Mono<Map<String, Object>> pipeline = webClient.post()
            .uri("/kmss/openapi/data/file/info/list")
            .headers(httpHeaders -> baseHeaders.forEach(httpHeaders::add))
            .bodyValue(docGuids)
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {});

        return logExternalCall("/kmss/openapi/data/file/info/list", docGuids, pipeline);
    }
    
    /**
     * 处理文档状态响应 - 响应式流处理
     */
    @SuppressWarnings("unchecked")
    private Mono<Map<String, Object>> processFileStatusResponse(Map<String, Object> data, 
            List<String> requestedGuids, int judgeMode) {
        
        return Mono.fromCallable(() -> {
            if (data == null) {
                return Collections.<Map<String, Object>>emptyList();
            }
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> files = (List<Map<String, Object>>) data.get("data");
            
            if (files == null || files.isEmpty()) {
                log.warn("未找到任何文档信息，原始数据: {}", data);
                return Collections.<Map<String, Object>>emptyList();
            }
            
            log.info("查询文档数量对比: 请求了 {} 个文档，返回了 {} 个文档", 
                requestedGuids.size(), files.size());
            
            return files;
        })
        .flatMapMany(files -> Flux.fromIterable(files))
        .flatMap(file -> processFileStatus(file, judgeMode))
        .filter(Objects::nonNull)
        .collectList()
        .map(processedFiles -> buildFileStatusResult(processedFiles, data));
    }
    
    /**
     * 处理单个文档状态
     */
    private Mono<Map<String, Object>> processFileStatus(Map<String, Object> file, int judgeMode) {
        return Mono.fromCallable(() -> {
            String docGuid = (String) file.get("@id");
            Integer status = (Integer) file.get("status");
            String title = file.getOrDefault("fileName", "未知标题").toString();
            
            if (docGuid == null) {
                log.warn("跳过文档，因为返回的guid为null: 标题={}, 状态={}", title, status);
                return null;
            }
            
            log.info("文档[{}]状态: {}", docGuid, status);
            
            Map<String, Object> fileResult = new HashMap<>();
            fileResult.put("guid", docGuid);
            fileResult.put("title", title);
            fileResult.put("status", status);
            fileResult.put("online", false);
            fileResult.put("message", "");
            
            boolean isOnline = judgeMode == 1 ? 
                judgeSimpleStatus(status, fileResult) : 
                judgeStandardStatus(file, fileResult);
            
            return isOnline ? fileResult : null;
        });
    }
    
    /**
     * 简化状态判断
     */
    private boolean judgeSimpleStatus(Integer status, Map<String, Object> fileResult) {
        boolean isSuccess = (status != null && status >= 20);
        
        if (isSuccess) {
            fileResult.put("online", true);
            fileResult.put("message", String.format("文档状态为%d，大于等于20，表示文档已上线", status));
        } else {
            fileResult.put("message", String.format("文档状态为%d，小于20，文档未上线", status));
        }
        
        return isSuccess;
    }
    
    /**
     * 标准状态判断
     */
    @SuppressWarnings("unchecked")
    private boolean judgeStandardStatus(Map<String, Object> file, Map<String, Object> fileResult) {
        Integer status = (Integer) file.get("status");
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> distributeStatus = 
            (List<Map<String, Object>>) file.get("distributeStatus");
        
        boolean textSuccess = false;
        if (distributeStatus != null) {
            textSuccess = distributeStatus.stream()
                .filter(item -> "text".equals(item.get("distributeType")))
                .anyMatch(item -> Integer.valueOf(0).equals(item.get("code")));
        }
        
        boolean isSuccess = (status != null && status == 50 && 
                           distributeStatus != null && !distributeStatus.isEmpty() && 
                           textSuccess);
        
        if (isSuccess) {
            fileResult.put("online", true);
            fileResult.put("message", "文档状态为50且text分发成功，表示文档达到下线标准");
        } else {
            List<String> errorReasons = new ArrayList<>();
            if (status == null || status != 50) {
                errorReasons.add(String.format("文档状态=%d，未达到下线标准(50)", status));
            }
            if (distributeStatus == null || distributeStatus.isEmpty()) {
                errorReasons.add("分发状态列表为空");
            } else if (!textSuccess) {
                errorReasons.add("text类型分发失败");
            }
            fileResult.put("message", "文档未达到下线标准: " + String.join(", ", errorReasons));
        }
        
        return isSuccess;
    }
    
    /**
     * 构建文档状态结果
     */
    private Map<String, Object> buildFileStatusResult(List<Map<String, Object>> processedFiles, 
            Map<String, Object> rawData) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", !processedFiles.isEmpty());
        result.put("message", !processedFiles.isEmpty() ? 
            String.format("共查询了%d个文档的状态", processedFiles.size()) : 
            "没有文档满足条件");
        result.put("data", processedFiles);
        result.put("raw_data", rawData);
        
        return result;
    }
    
    /**
     * 同步虚拟用户组 - 响应式实现
     */
    public Mono<Map<String, Object>> syncVirtualGroup(String systemName, 
            String virtualGroupId, String virtualGroupName, int deleted, HttpHeaders headers) {
        
        return validateSystemName(systemName, "sync_virtual_group")
            .then(buildVirtualGroupRequest(systemName, virtualGroupId, virtualGroupName, deleted))
            .flatMap(requestBody -> callSyncVirtualGroupApi(requestBody, headers))
            .map(this::convertVirtualGroupResponse)
            .onErrorMap(error -> {
                if (error instanceof ApiException) {
                    return error;
                }
                String errorMsg = "同步虚拟群组过程中发生错误: " + error.getMessage();
                return ApiException.internalError(errorMsg, error);
            });
    }
    
    /**
     * 验证系统名称
     */
    private Mono<Void> validateSystemName(String systemName, String operation) {
        return Mono.fromRunnable(() -> {
            if (systemName == null || systemName.isEmpty()) {
                String errorMsg = "系统名称不能为空";
                throw new ValidationException(errorMsg);
            }
        });
    }
    
    /**
     * 构建虚拟用户组请求
     */
    private Mono<Map<String, Object>> buildVirtualGroupRequest(String systemName, 
            String virtualGroupId, String virtualGroupName, int deleted) {
        
        return Mono.fromCallable(() -> {
            Map<String, Object> virtualGroupDto = new HashMap<>();
            virtualGroupDto.put("virtualGroupId", virtualGroupName);
            virtualGroupDto.put("tenantId", "85d5572986784a53a649726085691974");
            virtualGroupDto.put("virtualGroupName", virtualGroupName);
            virtualGroupDto.put("deleted", deleted);
            virtualGroupDto.put("system", systemName);
            
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("openVirtualGroupDTOs", Arrays.asList(virtualGroupDto));
            
            try {
                log.info("调用同步虚拟群组API，请求参数: {}", objectMapper.writeValueAsString(requestBody));
            } catch (Exception e) {
                log.warn("序列化请求参数时出错: {}", e.getMessage());
            }
            
            return requestBody;
        });
    }
    
    /**
     * 调用同步虚拟用户组API
     */
    private Mono<Map<String, Object>> callSyncVirtualGroupApi(Map<String, Object> requestBody, HttpHeaders headers) {
        Mono<Map<String, Object>> pipeline = webClient.post()
                .uri("/custom/openapi/v1/virtualGroup/sync")
                .headers(httpHeaders -> httpHeaders.addAll(headers))
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {});

        return logExternalCall("/custom/openapi/v1/virtualGroup/sync", requestBody, pipeline);
    }
    
    /**
     * 转换虚拟用户组响应
     */
    private Map<String, Object> convertVirtualGroupResponse(Map<String, Object> data) {
        try {
            log.info("同步虚拟群组API响应: {}", objectMapper.writeValueAsString(data));
        } catch (Exception e) {
            log.warn("序列化响应数据时出错: {}", e.getMessage());
        }
        
        Map<String, Object> result = new HashMap<>();
        if (data != null) {
            result.put("success", data.getOrDefault("success", true));
            result.put("data", data.getOrDefault("data", null));
            result.put("message", data.getOrDefault("msg", "操作成功"));
            result.put("code", data.getOrDefault("code", 200));
        } else {
            result.put("success", true);
            result.put("data", null);
            result.put("message", "操作成功");
            result.put("code", 200);
        }
        
        return result;
    }
    
    /**
     * 管理虚拟群组关系 - 响应式实现
     */
    public Mono<Map<String, Object>> manageVirtualGroupRelation(
            String method, String docGuid, List<String> userList, HttpHeaders headers) {
        
        return validateVirtualGroupMethod(method)
            .then(buildVirtualGroupRelationRequest(method, docGuid, userList))
            .flatMap(requestBody -> {
                if (requestBody == null) {
                    return Mono.just(createErrorResult(400, "没有有效的用户ID"));
                }
                return callVirtualGroupRelationApi(requestBody, headers);
            })
            .map(this::convertVirtualGroupRelationResponse)
            .onErrorMap(error -> {
                String errorMsg = "管理虚拟群组关系过程中发生错误: " + error.getMessage();
                return new RuntimeException(errorMsg, error);
            });
    }
    
    /**
     * 验证虚拟群组方法
     */
    private Mono<Void> validateVirtualGroupMethod(String method) {
        return Mono.fromRunnable(() -> {
            if (!"CREATE".equals(method) && !"DELETE".equals(method)) {
                String errorMsg = "方法必须是 CREATE 或 DELETE";
                throw new IllegalArgumentException(errorMsg);
            }
        });
    }
    
    /**
     * 构建虚拟群组关系请求
     */
    private Mono<Map<String, Object>> buildVirtualGroupRelationRequest(String method, 
            String docGuid, List<String> userList) {
        
        return Mono.fromCallable(() -> {
            String virtualGroupId = "virtual_" + docGuid;
            
            List<Map<String, Object>> relationDtos = new ArrayList<>();
            for (String userId : userList) {
                if (userId != null && !userId.isEmpty()) {
                    Map<String, Object> dto = new HashMap<>();
                    dto.put("virtualGroupId", virtualGroupId);
                    dto.put("tenantId", "85d5572986784a53a649726085691974");
                    dto.put("userOpenId", userId);
                    relationDtos.add(dto);
                }
            }
            
            if (relationDtos.isEmpty()) {
                String errorMsg = "没有有效的用户ID";
                log.warn("警告: {}, 原始用户列表: {}", errorMsg, userList);
                return null;
            }
            
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("method", method);
            requestBody.put("openVirtualGroupRelationDTOs", relationDtos);
            
            return requestBody;
        });
    }
    
    /**
     * 调用虚拟群组关系API
     */
    private Mono<Map<String, Object>> callVirtualGroupRelationApi(Map<String, Object> requestBody, HttpHeaders headers) {
        Mono<Map<String, Object>> pipeline = webClient.post()
                .uri("/custom/openapi/v1/virtualGroup/relation")
                .headers(httpHeaders -> httpHeaders.addAll(headers))
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {});

        return logExternalCall("/custom/openapi/v1/virtualGroup/relation", requestBody, pipeline);
    }
    
    /**
     * 转换虚拟群组关系响应
     */
    private Map<String, Object> convertVirtualGroupRelationResponse(Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", data != null ? data.getOrDefault("success", false) : false);
        result.put("code", data != null ? data.getOrDefault("code", 500) : 500);
        result.put("msg", data != null ? data.getOrDefault("msg", "成功") : "响应为空");
        
        return result;
    }
    
    /**
     * 更新或创建非结构化文档记录 - 响应式事务处理
     */
    @Transactional
    public Mono<Map<String, Object>> upsertUnstructuredDocument(
            String systemName, String fileId, String fileNumber, String fileName,
            String zhenzhiFileId, String version, int shouldDelete, int status,
            String roleList, String repoId) {
        
        final String cacheKey = generateCacheKey("unstructured_document", systemName, fileNumber);
        final String finalFileId = fileId;
        final String finalZhenzhiFileId = zhenzhiFileId;
        final int finalShouldDelete = shouldDelete;
        
        return unstructuredDocumentRepository.findBySystemNameAndFileNumber(systemName, fileNumber)
            .cast(UnstructuredDocument.class)
            .flatMap(existingDoc -> {
                if (finalShouldDelete == 1) {
                    return deleteUnstructuredDocument(existingDoc, cacheKey);
                } else {
                    return updateUnstructuredDocument(existingDoc, fileName, fileNumber, status, 
                        roleList, repoId, finalZhenzhiFileId, version, cacheKey);
                }
            })
            .switchIfEmpty(Mono.<Map<String, Object>>defer(() -> {
                if (finalShouldDelete == 1) {
                    return Mono.just(createSuccessResult("文档不存在，无需删除"));
                }
                
                if (finalZhenzhiFileId == null) {
                    log.warn("尝试更新不存在的非结构化文档且未提供甄知文档ID: {}", finalFileId);
                    return Mono.just(createErrorResult(400, "文档不存在且没有甄知文档ID，无法创建"));
                }
                
                return createUnstructuredDocument(systemName, finalFileId, fileNumber, fileName, 
                    finalZhenzhiFileId, version, status, roleList, repoId, cacheKey);
            }))
            .onErrorResume(error -> {
                String errorMsg = (finalShouldDelete == 1 ? "删除" : "更新/创建") + 
                    "非结构化文档失败: " + error.getMessage();
                log.error(errorMsg, error);
                
                return logErrorReactive(7, finalFileId, "upsert_unstructured_document", errorMsg,
                    Map.of("system_name", systemName, "file_id", finalFileId))
                    .then(Mono.just(createErrorResultWithParams(errorMsg, systemName, finalFileId, finalZhenzhiFileId, status)));
            });
    }
    
    /**
     * 删除非结构化文档
     */
    private Mono<Map<String, Object>> deleteUnstructuredDocument(UnstructuredDocument document, String cacheKey) {
        return unstructuredDocumentRepository.delete(document)
            .then(reactiveRedisManager.delete(cacheKey))
            .then(Mono.fromCallable(() -> {
                log.info("已从数据库中物理删除非结构化文档: {}", document.getFileId());
                return createSuccessResult("文档已成功从数据库中删除");
            }));
    }
    
    /**
     * 更新非结构化文档
     */
    private Mono<Map<String, Object>> updateUnstructuredDocument(UnstructuredDocument document, 
            String fileName, String fileNumber, int status, String roleList, String repoId, 
            String zhenzhiFileId, String version, String cacheKey) {
        
        return Mono.fromCallable(() -> {
            document.setFileName(fileName);
            document.setFileNumber(fileNumber);
            document.setUpdatedAt(LocalDateTime.now());
            document.setStatus(status);
            
            if (roleList != null) {
                document.setRoleList(convertStringListToJsonNode(Arrays.asList(roleList.split(","))));
            }
            if (repoId != null) {
                document.setRepoId(repoId);
            }
            if (zhenzhiFileId != null) {
                document.setZhenzhiFileId(zhenzhiFileId);
            }
            if (version != null) {
                document.setVersion(version);
            }
            
            return document;
        })
        .flatMap(unstructuredDocumentRepository::save)
        .flatMap(savedDoc -> {
            Map<String, Object> updatedDoc = convertUnstructuredDocumentToMap(savedDoc);
            return reactiveRedisManager.set(cacheKey, updatedDoc, CACHE_TIMEOUT)
                .thenReturn(updatedDoc);
        })
        .doOnSuccess(doc -> log.info("更新非结构化文档: {}", document.getFileId()));
    }
    
    /**
     * 创建非结构化文档
     */
    private Mono<Map<String, Object>> createUnstructuredDocument(String systemName, String fileId,
            String fileNumber, String fileName, String zhenzhiFileId, String version, int status,
            String roleList, String repoId, String cacheKey) {
        
        String id = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();
        
        String insertSql = "INSERT INTO unstructured_documents " +
                          "(id, system_name, file_id, file_number, file_name, zhenzhi_file_id, version, status, role_list, repo_id, created_at, updated_at) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return databaseClient.sql(insertSql)
                .bind(0, id)
                .bind(1, systemName)
                .bind(2, fileId)
                .bind(3, fileNumber)
                .bind(4, fileName)
                .bind(5, zhenzhiFileId)
                .bind(6, version != null ? version : "")
                .bind(7, status)
                .bind(8, convertRoleListToJsonString(roleList))
                .bind(9, repoId)
                .bind(10, now)
                .bind(11, now)
                .fetch()
                .rowsUpdated()
                .flatMap(rowsUpdated -> {
                    Map<String, Object> createdDoc = new HashMap<>();
                    createdDoc.put("id", id);
                    createdDoc.put("system_name", systemName);
                    createdDoc.put("file_id", fileId);
                    createdDoc.put("file_number", fileNumber);
                    createdDoc.put("file_name", fileName);
                    createdDoc.put("zhenzhi_file_id", zhenzhiFileId);
                    createdDoc.put("version", version != null ? version : "");
                    createdDoc.put("status", status);
                    createdDoc.put("role_list", roleList);
                    createdDoc.put("repo_id", repoId);
                    createdDoc.put("created_at", now);
                    createdDoc.put("updated_at", now);
                    
                    return reactiveRedisManager.set(cacheKey, createdDoc, CACHE_TIMEOUT)
                        .thenReturn(createdDoc);
                })
        .doOnSuccess(doc -> log.info("创建非结构化文档: {}, 甄知文档ID: {}", fileId, zhenzhiFileId));
    }
    
    /**
     * 查询甄知文件ID - 响应式实现
     */
    public Mono<Map<String, Object>> queryZhenzhiFileId(String fileNumber, String systemName) {
        String cacheKey = generateCacheKey("unstructured_document", systemName + ":" + fileNumber);
        
        return reactiveRedisManager.getMap(cacheKey)
            .doOnNext(cached -> log.info("从Redis获取知识库映射: system={}, file_number={}", systemName, fileNumber))
            .switchIfEmpty(
                unstructuredDocumentRepository.findByFileNumberAndSystemName(fileNumber, systemName)
                    .map(this::convertUnstructuredDocumentToMap)
                    .flatMap(mapping -> reactiveRedisManager.set(cacheKey, mapping, CACHE_TIMEOUT)
                        .thenReturn(mapping))
                    .doOnSuccess(mapping -> log.info("从数据库获取并缓存甄知文件映射"))
                    .switchIfEmpty(Mono.fromCallable(() -> {
                        log.warn("未找到记录: system={}, file_number={}", systemName, fileNumber);
                        return null;
                    }))
            )
            .onErrorResume(error -> {
                String errorMsg = "查询甄知id映射失败: " + error.getMessage();
                return logErrorReactive(2, systemName, "query_zhenzhi_file_id", errorMsg,
                    Map.of("file_number", fileNumber, "system_name", systemName))
                    .then(Mono.error(new RuntimeException(errorMsg, error)));
            });
    }
    
    /**
     * 操作文档上下线状态 - 响应式实现
     */
    public Mono<Map<String, Object>> manageDocumentOnlineStatus(
            String repoid, String docGuid, int cmsOnline, String fileName,
            String adminOpenId, String contentType, HttpHeaders headers) {
        
        return validateDocumentParams(repoid, docGuid)
            .then(buildDocumentOnlineRequest(docGuid, cmsOnline, fileName, contentType))
            .flatMap(requestData -> callDocumentOnlineApi(repoid, requestData, headers))
            .doOnSuccess(result -> {
                try {
                    log.info("文档{}API响应: {}", cmsOnline == 1 ? "上线" : "下线",
                        objectMapper.writeValueAsString(result));
                } catch (Exception e) {
                    log.warn("序列化响应数据时出错: {}", e.getMessage());
                }
            })
            .onErrorMap(error -> {
                log.error("文档{}过程中发生异常", cmsOnline == 1 ? "上线" : "下线", error);
                String errorMsg = "文档" + (cmsOnline == 1 ? "上线" : "下线") + "失败: " + error.getMessage();
                return new RuntimeException(errorMsg, error);
            });
    }
    
    /**
     * 验证文档参数
     */
    private Mono<Void> validateDocumentParams(String repoid, String docGuid) {
        return Mono.fromRunnable(() -> {
            if (repoid == null || repoid.isEmpty() || docGuid == null || docGuid.isEmpty()) {
                log.error("错误参数: repoid={}, doc_guid={}不能为空", repoid, docGuid);
                throw new IllegalArgumentException("知识库ID和文档ID不能为空");
            }
        });
    }
    
    /**
     * 构建文档上下线请求
     */
    private Mono<List<Map<String, Object>>> buildDocumentOnlineRequest(String docGuid, 
            int cmsOnline, String fileName, String contentType) {
        
        return Mono.fromCallable(() -> {
            List<Map<String, Object>> requestData = new ArrayList<>();
            Map<String, Object> docData = new HashMap<>();
            docData.put("@id", docGuid);
            docData.put("cms_online", cmsOnline);
            docData.put("title", fileName);
            docData.put("contentType", contentType != null ? contentType : "file");
            requestData.add(docData);
            
            try {
                log.info("调用文档上下线API，请求参数: {}", objectMapper.writeValueAsString(requestData));
            } catch (Exception e) {
                log.warn("序列化请求参数时出错: {}", e.getMessage());
            }
            
            return requestData;
        });
    }
    
    /**
     * 调用文档上下线API
     */
    private Mono<Map<String, Object>> callDocumentOnlineApi(String repoid, 
            List<Map<String, Object>> requestData, HttpHeaders headers) {
        
        Mono<Map<String, Object>> pipeline = webClient.post()
            .uri(uriBuilder -> uriBuilder
                .path("/kmss/openapi/search-platform/datamanage/fulltext/online")
                .queryParam("repoGuid", repoid)
                .build())
            .headers(httpHeaders -> httpHeaders.addAll(headers))
            .bodyValue(requestData)
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {});

        Map<String, Object> logPayload = new HashMap<>();
        logPayload.put("repoGuid", repoid);
        logPayload.put("body", requestData);

        return logExternalCall("/kmss/openapi/search-platform/datamanage/fulltext/online", logPayload, pipeline);
    }
    
    /**
     * 更新或创建文档状态记录 - 响应式事务处理
     */
    @Transactional
    public Mono<Map<String, Object>> upsertDocumentStatus(
            String systemName, String fileId, String fileNumber,
            int status, String repoId, String zhenzhiFileId) {
        
        String cacheKey = generateCacheKey("document_status", systemName, fileId);
        
        return documentStatusRepository.findBySystemNameAndFileId(systemName, fileId)
            .cast(DocumentStatus.class)
            .flatMap(existingStatus -> updateDocumentStatus(existingStatus, fileNumber, status, repoId, zhenzhiFileId, cacheKey))
            .switchIfEmpty(createDocumentStatus(systemName, fileId, fileNumber, status, repoId, zhenzhiFileId, cacheKey))
            .map(docStatus -> {
                Map<String, Object> result = new HashMap<>();
                result.put("success", true);
                result.put("data", docStatus);
                return result;
            })
            .onErrorResume(error -> {
                String errorMsg = "维护文档状态记录时出错: " + error.getMessage();
                log.error(errorMsg, error);
                Map<String, Object> result = new HashMap<>();
                result.put("success", false);
                result.put("message", errorMsg);
                return Mono.just(result);
            });
    }
    
    /**
     * 更新文档状态
     */
    private Mono<Map<String, Object>> updateDocumentStatus(DocumentStatus docStatus, 
            String fileNumber, int status, String repoId, String zhenzhiFileId, String cacheKey) {
        
        return Mono.fromCallable(() -> {
            docStatus.setFileNumber(fileNumber);
            docStatus.setUpdatedAt(LocalDateTime.now());
            docStatus.setStatus(status);
            
            if (repoId != null) {
                docStatus.setRepoId(repoId);
            }
            if (zhenzhiFileId != null) {
                docStatus.setZhenzhiFileId(zhenzhiFileId);
            }
            
            return docStatus;
        })
        .flatMap(documentStatusRepository::save)
        .flatMap(savedStatus -> {
            Map<String, Object> updatedStatus = convertDocumentStatusToMap(savedStatus);
            return reactiveRedisManager.set(cacheKey, updatedStatus, CACHE_TIMEOUT)
                .thenReturn(updatedStatus);
        })
        .doOnSuccess(updatedDoc -> log.info("更新文档状态记录: {}", docStatus.getFileId()));
    }
    
    /**
     * 创建文档状态
     */
    private Mono<Map<String, Object>> createDocumentStatus(String systemName, String fileId, 
            String fileNumber, int status, String repoId, String zhenzhiFileId, String cacheKey) {
        
        String id = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();
        
        String insertSql = "INSERT INTO document_status " +
                          "(id, system_name, file_id, file_number, status, repo_id, zhenzhi_file_id, created_at, updated_at) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        return databaseClient.sql(insertSql)
                .bind(0, id)
                .bind(1, systemName)
                .bind(2, fileId)
                .bind(3, fileNumber)
                .bind(4, status)
                .bind(5, repoId)
                .bind(6, zhenzhiFileId)
                .bind(7, now)
                .bind(8, now)
                .fetch()
                .rowsUpdated()
                .flatMap(rowsUpdated -> {
                    Map<String, Object> createdStatus = new HashMap<>();
                    createdStatus.put("id", id);
                    createdStatus.put("system_name", systemName);
                    createdStatus.put("file_id", fileId);
                    createdStatus.put("file_number", fileNumber);
                    createdStatus.put("status", status);
                    createdStatus.put("repo_id", repoId);
                    createdStatus.put("zhenzhi_file_id", zhenzhiFileId);
                    createdStatus.put("created_at", now);
                    createdStatus.put("updated_at", now);
                    
                    return reactiveRedisManager.set(cacheKey, createdStatus, CACHE_TIMEOUT)
                        .thenReturn(createdStatus);
                })
        .doOnSuccess(createdDoc -> log.info("创建文档状态记录: {}, 状态: {}", fileId, status));
    }
    
    // ==================== 辅助方法 ====================

    /**
     * 健康检查方法 - 测试外部API连接
     */
    public Mono<Map<String, Object>> healthCheck() {
        return Mono.fromCallable(() -> {
            Map<String, Object> health = new HashMap<>();
            health.put("service", "ReactiveKnowledgeHandler");
            health.put("initialized", initialized);
            health.put("timestamp", System.currentTimeMillis());
            return health;
        })
        .flatMap(health -> {
            // 测试外部API连接
            return testExternalApiConnection()
                .map(apiHealth -> {
                    health.put("external_api", apiHealth);
                    health.put("status", apiHealth.get("status"));
                    return health;
                })
                .onErrorResume(error -> {
                    Map<String, Object> apiHealth = new HashMap<>();
                    apiHealth.put("status", "error");
                    apiHealth.put("message", error.getMessage());
                    health.put("external_api", apiHealth);
                    health.put("status", "degraded");
                    return Mono.just(health);
                });
        });
    }

    /**
     * 测试外部API连接
     */
    private Mono<Map<String, Object>> testExternalApiConnection() {
        Mono<Map<String, Object>> pipeline = webClient.get()
            .uri("/super-help/openapi/v1/querySystemInfo?system=health_check")
            .headers(httpHeaders -> baseHeaders.forEach(httpHeaders::add))
            .retrieve()
            .bodyToMono(String.class)
            .timeout(Duration.ofSeconds(5))
            .map(response -> {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "healthy");
                result.put("message", "API连接正常");
                result.put("base_url", applicationProperties.getApi().getBaseUrl());
                return result;
            })
            .onErrorMap(error -> {
                log.warn("外部API连接测试失败: {}", error.getMessage());
                Map<String, Object> result = new HashMap<>();
                result.put("status", "unhealthy");
                result.put("message", "API连接失败: " + error.getMessage());
                result.put("base_url", applicationProperties.getApi().getBaseUrl());
                return new RuntimeException("API连接测试失败", error);
            });

        return logExternalCall("/super-help/openapi/v1/querySystemInfo", Map.of("system", "health_check"), pipeline);
    }

    /**
     * 生成API签名 - 响应式版本
     */
    private Mono<String> generateSignature(String adminOpenId) {
        return Mono.fromCallable(() -> {
            try {
                // 构建签名参数
                Map<String, String> params = new TreeMap<>();
                params.put("userId", adminOpenId);
                params.put("userName", adminOpenId);
                params.put("tenantId", "85d5572986784a53a649726085691974");
                
                // 将参数转换为JSON字符串
                String dataToEncrypt = objectMapper.writeValueAsString(params);
                log.debug("排序并JSON序列化后的数据: {}", dataToEncrypt);
                
                // 准备AES密钥
                byte[] keyBytes = MATCH_API_AES_KEY.getBytes(StandardCharsets.UTF_8);
                
                // AES加密
                SecretKeySpec secretKey = new SecretKeySpec(keyBytes, "AES");
                Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
                cipher.init(Cipher.ENCRYPT_MODE, secretKey);
                
                byte[] encryptedBytes = cipher.doFinal(dataToEncrypt.getBytes(StandardCharsets.UTF_8));
                String sign = Base64.getEncoder().encodeToString(encryptedBytes);
                
                log.debug("签名生成成功");
                return sign;
                
            } catch (Exception e) {
                log.error("生成签名过程中发生错误", e);
                throw new RuntimeException("签名生成失败", e);
            }
        });
    }
    
    /**
     * 生成缓存键
     */
    private String generateCacheKey(String prefix, String... params) {
        return prefix + ":" + String.join(":", params);
    }
    
    /**
     * 响应式错误日志记录
     */
    private Mono<Void> logErrorReactive(int errorType, String fileId, String step, 
            String errorMsg, Map<String, Object> params) {
        return Mono.fromRunnable(() -> {
            try {
                errorHandler.logError(errorType, fileId, step, errorMsg, params);
                log.error("错误类型: {}, 文件: {}, 步骤: {}, 消息: {}", 
                    errorType, fileId, step, errorMsg);
            } catch (Exception e) {
                log.error("记录错误日志时发生异常", e);
            }
        });
    }
    
    /**
     * 转换UnstructuredDocument为Map
     */
    private Map<String, Object> convertUnstructuredDocumentToMap(UnstructuredDocument document) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", document.getId());
        map.put("system_name", document.getSystemName());
        map.put("file_id", document.getFileId());
        map.put("file_number", document.getFileNumber());
        map.put("file_name", document.getFileName());
        map.put("zhenzhi_file_id", document.getZhenzhiFileId());
        map.put("version", document.getVersion());
        map.put("status", document.getStatus());
        map.put("role_list", document.getRoleList());
        map.put("repo_id", document.getRepoId());
        map.put("created_at", document.getCreatedAt().toString());
        map.put("updated_at", document.getUpdatedAt().toString());
        return map;
    }
    
    /**
     * 转换DocumentStatus为Map
     */
    private Map<String, Object> convertDocumentStatusToMap(DocumentStatus status) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", status.getId());
        map.put("system_name", status.getSystemName());
        map.put("file_id", status.getFileId());
        map.put("file_number", status.getFileNumber());
        map.put("status", status.getStatus());
        map.put("repo_id", status.getRepoId());
        map.put("zhenzhi_file_id", status.getZhenzhiFileId());
        map.put("created_at", status.getCreatedAt().toString());
        map.put("updated_at", status.getUpdatedAt().toString());
        return map;
    }
    
    /**
     * 创建成功结果
     */
    private Map<String, Object> createSuccessResult(String message) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("message", message);
        return result;
    }
    
    /**
     * 创建错误结果
     */
    private Map<String, Object> createErrorResult(int code, String message) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", false);
        result.put("code", code);
        result.put("message", message);
        return result;
    }
    
    /**
     * 创建带参数的错误结果
     */
    private Map<String, Object> createErrorResultWithParams(String message, String systemName, 
            String fileId, String zhenzhiFileId, int status) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", false);
        result.put("error", message);
        result.put("params", Map.of(
            "system_name", systemName,
            "file_id", fileId,
            "zhenzhi_file_id", zhenzhiFileId != null ? zhenzhiFileId : "",
            "status", status
        ));
        return result;
    }
    
    /**
     * 关闭资源 - 响应式版本
     */
    @PreDestroy
    public Mono<Void> shutdown() {
        return Mono.fromRunnable(() -> log.info("正在关闭ReactiveKnowledgeHandler..."))
            .then(reactiveSyncManager.stop())
            .doOnSuccess(v -> log.info("响应式同步管理器已停止"))
            .then(reactiveRedisManager.close())
            .doOnSuccess(v -> log.info("响应式缓存服务已关闭"))
            .doOnSuccess(v -> log.info("ReactiveKnowledgeHandler资源已释放"))
            .onErrorResume(error -> {
                log.error("关闭ReactiveKnowledgeHandler时出错", error);
                return Mono.empty();
            })
            .then();
    }
    
    /**
     * 设置文档权限（响应式版本）
     *
     * @param systemName 系统名称
     * @param docGuid 文档GUID
     * @param repoId 知识库ID
     * @param roleList 角色列表
     * @param userList 用户列表
     * @param action 操作类型（add/del）
     * @param groupGuid 部门ID
     * @param adminOpenId 管理员ID
     * @param opType 操作类型
     * @param headers 包含签名的请求头
     * @return 操作结果
     */
    public Mono<Map<String, Object>> setDocAdmin(String systemName, String docGuid, String repoId,
            List<String> roleList, List<String> userList, 
            String action, String groupGuid, String adminOpenId, int opType, HttpHeaders headers) {
        
        return Mono.fromCallable(() -> {
                // 参数验证
                if (systemName == null || docGuid == null) {
                    String errorMsg = "系统名称和文档GUID不能为空";
                    throw new IllegalArgumentException(errorMsg);
                }
                
                // 构造成员列表
                List<Map<String, Object>> members = new ArrayList<>();
                
                // 处理角色列表
                if (roleList != null) {
                    for (String roleId : roleList) {
                        Map<String, Object> member = new HashMap<>();
                        member.put("nickname", roleId);
                        member.put("memberType", 2);
                        member.put("memberId", roleId);
                        members.add(member);
                    }
                }
                
                // 创建虚拟群组
                String virtualGroupId = "virtual_" + docGuid;
                if (opType == 1) {
                    Map<String, Object> member = new HashMap<>();
                    member.put("nickname", virtualGroupId);
                    member.put("memberType", 2);
                    member.put("memberId", virtualGroupId);
                    members.add(member);
                }
                
                if (members.isEmpty()) {
                    String errorMsg = "角色列表和用户列表不能同时为空";
                    throw new IllegalArgumentException(errorMsg);
                }
                
                // 构建请求参数
                Map<String, Object> requestBody = new HashMap<>();
                requestBody.put("docId", docGuid);
                requestBody.put("action", action);
                requestBody.put("members", members);
                requestBody.put("message", ("add".equals(action) ? "添加" : "删除") + "文档权限");
                
                if (repoId != null) {
                    requestBody.put("repoId", repoId);
                }
                if (groupGuid != null) {
                    requestBody.put("groupGuid", groupGuid);
                }
                
                try {
                    log.info("调用设置文档权限API，请求参数: {}", objectMapper.writeValueAsString(requestBody));
                } catch (Exception e) {
                    log.warn("序列化请求参数时出错: {}", e.getMessage());
                }
                
                return requestBody;
            })
            .flatMap(requestBody -> {
                Mono<Map<String, Object>> apiCall = webClient.post()
                        .uri("/path_wiki/wiki/ku/openapi/doc/v1/members")
                        .headers(httpHeaders -> httpHeaders.addAll(headers))
                        .bodyValue(requestBody)
                        .retrieve()
                        .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {});

                return logExternalCall("/path_wiki/wiki/ku/openapi/doc/v1/members", requestBody, apiCall)
                    .doOnNext(data -> log.info("设置文档权限API原始响应: {}", data))
                    .flatMap(data -> {
                        // 检查响应数据是否有效
                        if (data == null) {
                            String errorMsg = "设置文档权限失败: API响应为空";
                            return logErrorReactive(9, "unknown", "set_doc_admin", errorMsg,
                                Map.of("system_name", systemName, "doc_guid", docGuid))
                                .then(Mono.error(new RuntimeException(errorMsg)));
                        }
                        
                        // 检查多种可能的错误状态
                        Object returnCode = data.get("returnCode");
                        Object code = data.get("code");
                        Object status = data.get("status");
                        Object message = data.get("message");
                        Object returnMessage = data.get("returnMessage");
                        
                        // 判断是否为错误响应
                        boolean isError = false;
                        String errorMsg = null;
                        
                        if (returnCode != null && returnCode instanceof Number && ((Number) returnCode).intValue() >= 400) {
                            isError = true;
                            errorMsg = "设置文档权限失败 (returnCode=" + returnCode + "): " + 
                                (returnMessage != null ? returnMessage : "未知错误");
                        } else if (code != null && code instanceof Number && ((Number) code).intValue() >= 400) {
                            isError = true;
                            errorMsg = "设置文档权限失败 (code=" + code + "): " + 
                                (message != null ? message : "未知错误");
                        } else if (status != null && "error".equals(status.toString().toLowerCase())) {
                            isError = true;
                            errorMsg = "设置文档权限失败 (status=error): " + 
                                (message != null ? message : "未知错误");
                        }
                        
                        if (isError) {
                            return logErrorReactive(9, "unknown", "set_doc_admin", errorMsg,
                                Map.of("system_name", systemName, "doc_guid", docGuid, "response", data))
                                .then(Mono.error(new RuntimeException(errorMsg)));
                        }
                        
                        Map<String, Object> result = new HashMap<>();
                        result.put("status", "success");
                        result.put("message", "文档权限" + ("add".equals(action) ? "添加" : "删除") + "成功");
                        result.put("doc_guid", docGuid);
                        result.put("repo_id", repoId);
                        result.put("groupGuid", groupGuid);
                        // 将List转换为JsonNode以避免类型转换问题
                        result.put("role_list", roleList != null ? objectMapper.valueToTree(roleList) : null);
                        result.put("user_list", userList != null ? objectMapper.valueToTree(userList) : null);
                        result.put("action", action);
                        
                        return Mono.just(result);
                    });
            })
            .onErrorResume(e -> {
                if (e instanceof RuntimeException) {
                    return Mono.error(e);
                }
                String errorMsg = "设置文档权限过程中发生错误: " + e.getMessage();
                return logErrorReactive(9, "unknown", "set_doc_admin", errorMsg,
                    Map.of("system_name", systemName, "doc_guid", docGuid))
                    .then(Mono.error(new RuntimeException(errorMsg, e)));
            });
    }

    /**
     * 删除文档（响应式）
     * 
     * @param repositoryGuid 知识库GUID
     * @param docGuid 文档GUID  
     * @param withDescendants 是否删除子文档，默认为true
     * @param adminOpenId 管理员ID（可选）
     * @return 删除结果的Mono，包含returnCode、returnMessage、traceId等
     */
    public Mono<Map<String, Object>> deleteDocument(String repositoryGuid, String docGuid, Boolean withDescendants, String adminOpenId) {
        log.info("开始删除文档: repositoryGuid={}, docGuid={}, withDescendants={}", repositoryGuid, docGuid, withDescendants);
        
        return validateDeleteDocumentParams(repositoryGuid, docGuid)
            .then(generateDeleteDocumentHeaders(adminOpenId))
            .flatMap(headers -> callDeleteDocumentApi(repositoryGuid, docGuid, withDescendants != null ? withDescendants : true, headers))
            .flatMap(this::processDeleteDocumentResponse)
            .onErrorResume(error -> handleDeleteDocumentError(error, repositoryGuid, docGuid, withDescendants));
    }
    
    /**
     * 验证删除文档参数
     */
    private Mono<Void> validateDeleteDocumentParams(String repositoryGuid, String docGuid) {
        if (repositoryGuid == null || repositoryGuid.trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("知识库GUID不能为空"));
        }
        if (docGuid == null || docGuid.trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("文档GUID不能为空"));
        }
        return Mono.empty();
    }
    
    /**
     * 生成删除文档请求头
     */
    private Mono<HttpHeaders> generateDeleteDocumentHeaders(String adminOpenId) {
        if (adminOpenId != null && !adminOpenId.trim().isEmpty()) {
            return generateHeadersWithSignature(adminOpenId);
        } else {
            HttpHeaders headers = new HttpHeaders();
            // 正确地从baseHeaders复制到HttpHeaders
            baseHeaders.forEach(headers::set);
            return Mono.just(headers);
        }
    }
    
    /**
     * 调用删除文档API
     */
    private Mono<Map<String, Object>> callDeleteDocumentApi(String repositoryGuid, String docGuid, boolean withDescendants, HttpHeaders headers) {
        // 构建请求参数
        Map<String, Object> requestBody = Map.of(
            "repositoryGuid", repositoryGuid,
            "docGuid", docGuid,
            "withDescendants", withDescendants
        );
        
        log.debug("请求头: {}", headers);

        Mono<Map<String, Object>> pipeline = webClient.post()
            .uri("/path_wiki/wiki/ku/openapi/docs/delete")
            .headers(httpHeaders -> httpHeaders.addAll(headers))
            .bodyValue(requestBody)
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
            .retry(3)
            .timeout(Duration.ofSeconds(60))
            .doOnNext(response -> log.info("删除文档API响应: {}", response))
            .onErrorMap(error -> new RuntimeException("删除文档API调用失败: " + error.getMessage(), error));

        return logExternalCall("/path_wiki/wiki/ku/openapi/docs/delete", requestBody, pipeline);
    }
    
    /**
     * 处理删除文档响应
     */
    private Mono<Map<String, Object>> processDeleteDocumentResponse(Map<String, Object> response) {
        return Mono.fromCallable(() -> {
            if (response == null) {
                throw new RuntimeException("删除文档API返回空响应");
            }
            
            Integer returnCode = (Integer) response.get("returnCode");
            String returnMessage = (String) response.get("returnMessage");
            String traceId = (String) response.get("traceId");
            
            Map<String, Object> result = new HashMap<>();
            
            if (returnCode != null && returnCode == 200) {
                log.info("文档删除成功: returnCode={}, returnMessage={}", returnCode, returnMessage);
                result.put("success", true);
                result.put("returnCode", returnCode);
                result.put("returnMessage", returnMessage != null ? returnMessage : "删除成功");
                result.put("traceId", traceId);
            } else {
                String errorMsg = String.format("删除文档失败: %s", returnMessage != null ? returnMessage : "未知错误");
                log.error("删除文档失败: returnCode={}, returnMessage={}", returnCode, returnMessage);
                result.put("success", false);
                result.put("returnCode", returnCode);
                result.put("returnMessage", returnMessage != null ? returnMessage : "删除失败");
                result.put("traceId", traceId);
            }
            
            return result;
        });
    }
    
    /**
     * 处理删除文档错误
     */
    private Mono<Map<String, Object>> handleDeleteDocumentError(Throwable error, String repositoryGuid, String docGuid, Boolean withDescendants) {
        String errorMsg = String.format("删除文档时发生错误: %s", error.getMessage());
        log.error(errorMsg, error);
        
        // 记录错误日志
        Map<String, Object> errorParams = Map.of(
            "repositoryGuid", repositoryGuid != null ? repositoryGuid : "",
            "docGuid", docGuid != null ? docGuid : "",
            "withDescendants", withDescendants != null ? withDescendants : true
        );
        
        return logErrorReactive(4, docGuid != null ? docGuid : "", "delete_document", errorMsg, errorParams)
            .then(Mono.fromCallable(() -> {
                Map<String, Object> result = new HashMap<>();
                result.put("success", false);
                result.put("error", errorMsg);
                result.put("returnMessage", "处理异常");
                return result;
            }));
    }

    /**
     * 管理系统部门（创建、更新、删除）- 响应式实现
     * 对应 Python: manage_system_department
     */
    public Mono<Map<String, Object>> manageSystemDepartment(String method, String systemName) {
        return Mono.defer(() -> {
            // 1. 参数校验
            if (!"CREATE".equals(method) && !"UPDATE".equals(method) && !"DELETE".equals(method)) {
                String errorMsg = "方法必须是 CREATE 或 UPDATE 或 DELETE";
                Map<String, Object> params = new HashMap<>();
                params.put("method", method);
                return logErrorReactive(15, "unknown", "manage_system_department", errorMsg, params)
                    .thenReturn(buildReturnMap(400, errorMsg, "", ""));
            }
            if (isBlank(systemName)) {
                String errorMsg = "系统名称不能为空";
                Map<String, Object> params = new HashMap<>();
                params.put("system_name", systemName);
                return logErrorReactive(15, systemName != null ? systemName : "unknown",
                        "manage_system_department", errorMsg, params)
                    .thenReturn(buildReturnMap(400, errorMsg, "", ""));
            }

            // 2. 构建请求并调用接口
            return buildDepartmentSyncRequest(method, systemName)
                .flatMap(this::callDepartmentSyncApi)
                // 3. 处理响应（HTTP 200）
                .flatMap(data -> {
                    int code = extractReturnCode(data);
                    String traceId = extractTraceId(data);
                    if (code != 200) {
                        String msg = extractReturnMessage(data, "创建系统部门API调用失败");
                        Map<String, Object> params = new HashMap<>();
                        params.put("method", method);
                        params.put("system_name", systemName);
                        return logErrorReactive(15, systemName, "manage_system_department", msg, params)
                            .thenReturn(buildReturnMap(code, msg, "", traceId));
                    }
                    return Mono.just(buildReturnMap(200, "系统部门创建成功", data, traceId));
                })
                // 非200 HTTP错误或解析异常
                .onErrorResume(error -> {
                    String errorMsg;
                    int status = 500;
                    if (error instanceof WebClientResponseException) {
                        WebClientResponseException ex = (WebClientResponseException) error;
                        status = ex.getRawStatusCode();
                        errorMsg = "创建系统部门API调用失败: " + status + ", " + ex.getResponseBodyAsString();
                    } else {
                        errorMsg = "创建系统部门过程中发生错误: " + error.getMessage();
                    }
                    Map<String, Object> params = new HashMap<>();
                    params.put("method", method);
                    params.put("system_name", systemName);
                    return logErrorReactive(15, systemName, "manage_system_department", errorMsg, params)
                        .thenReturn(buildReturnMap(status, errorMsg, "", ""));
                });
        });
    }

    private Mono<Map<String, Object>> buildDepartmentSyncRequest(String method, String systemName) {
        return Mono.fromCallable(() -> {
            Map<String, Object> dept = new HashMap<>();
            dept.put("deptOpenId", systemName);
            dept.put("tenantId", "85d5572986784a53a649726085691974");
            dept.put("deptName", systemName);
            dept.put("deptComment", systemName);
            dept.put("deleted", 0);

            Map<String, Object> payload = new HashMap<>();
            payload.put("method", method);
            payload.put("openDepartments", Collections.singletonList(dept));

            try {
                log.info("调用创建系统部门API，请求参数: {}", objectMapper.writeValueAsString(payload));
                log.info("请求头: {}", baseHeaders);
            } catch (Exception e) {
                log.warn("序列化请求参数时出错: {}", e.getMessage());
            }
            return payload;
        });
    }

    private Mono<Map<String, Object>> callDepartmentSyncApi(Map<String, Object> payload) {
        Mono<Map<String, Object>> pipeline = webClient.post()
            .uri("/super-help/openapi/v1/department/sync")
            .headers(h -> baseHeaders.forEach(h::add))
            .bodyValue(payload)
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
            .timeout(Duration.ofSeconds(60))
            .doOnNext(data -> {
                try {
                    log.info("创建系统部门API响应: {}", objectMapper.writeValueAsString(data));
                } catch (Exception e) {
                    log.info("创建系统部门API响应(序列化失败，原始对象输出): {}", data);
                }
            });

        return logExternalCall("/super-help/openapi/v1/department/sync", payload, pipeline);
    }

    /**
     * 管理用户（创建、更新、删除）- 响应式实现
     * 对应 Python: manage_user
     */
    public Mono<Map<String, Object>> manageUser(
        String method,
        String userOpenId,
        String userName,
        String deptOpenId,
        String deptName,
        String tenantId,
        Integer deleted
    ) {
        return Mono.defer(() -> {
            // 1. 参数校验
            if (!"CREATE".equals(method) && !"UPDATE".equals(method) && !"DELETE".equals(method)) {
                String errorMsg = "方法必须是 CREATE 或 UPDATE 或 DELETE";
                Map<String, Object> params = new HashMap<>();
                params.put("method", method);
                return logErrorReactive(15, "unknown", "manage_user", errorMsg, params)
                    .thenReturn(buildReturnMap(400, errorMsg, "", ""));
            }

            Map<String, String> required = new LinkedHashMap<>();
            required.put("user_open_id", userOpenId);
            required.put("user_name", userName);
            required.put("dept_open_id", deptOpenId);
            required.put("dept_name", deptName);

            for (Map.Entry<String, String> entry : required.entrySet()) {
                if (isBlank(entry.getValue())) {
                    String errorMsg = entry.getKey() + " 不能为空";
                    Map<String, Object> params = new HashMap<>();
                    params.put(entry.getKey(), entry.getValue());
                    return logErrorReactive(15, userOpenId != null ? userOpenId : "unknown",
                            "manage_user", errorMsg, params)
                        .thenReturn(buildReturnMap(400, errorMsg, "", ""));
                }
            }

            final String finalTenantId = !isBlank(tenantId) ? tenantId : "85d5572986784a53a649726085691974";
            final int finalDeleted = deleted != null ? deleted : 0;

            // 2. 构建请求并调用接口
            return buildUserSyncRequest(method, userOpenId, userName, deptOpenId, deptName, finalTenantId, finalDeleted)
                .flatMap(this::callUserSyncApi)
                // 3. 处理响应（HTTP 200）
                .flatMap(data -> {
                    int code = extractReturnCode(data);
                    String traceId = extractTraceId(data);
                    if (code != 200) {
                        String msg = extractReturnMessage(data, "用户管理API调用失败");
                        Map<String, Object> params = new HashMap<>();
                        params.put("method", method);
                        params.put("user_open_id", userOpenId);
                        params.put("user_name", userName);
                        return logErrorReactive(15, userOpenId != null ? userOpenId : "unknown",
                                "manage_user", msg, params)
                            .thenReturn(buildReturnMap(code, msg, "", traceId));
                    }

                    String opText = "操作";
                    if ("CREATE".equals(method)) opText = "创建";
                    else if ("UPDATE".equals(method)) opText = "更新";
                    else if ("DELETE".equals(method)) opText = "删除";

                    return Mono.just(buildReturnMap(200, "用户" + opText + "成功", data, traceId));
                })
                // 非200 HTTP错误或解析异常
                .onErrorResume(error -> {
                    String errorMsg;
                    int status = 500;
                    if (error instanceof WebClientResponseException) {
                        WebClientResponseException ex = (WebClientResponseException) error;
                        status = ex.getRawStatusCode();
                        errorMsg = "用户管理API调用失败: " + status + ", " + ex.getResponseBodyAsString();
                    } else {
                        errorMsg = "用户管理过程中发生错误: " + error.getMessage();
                    }
                    Map<String, Object> params = new HashMap<>();
                    params.put("method", method);
                    params.put("user_open_id", userOpenId);
                    params.put("user_name", userName);
                    return logErrorReactive(15, userOpenId != null ? userOpenId : "unknown",
                            "manage_user", errorMsg, params)
                        .thenReturn(buildReturnMap(status, errorMsg, "", ""));
                });
        });
    }

    private Mono<Map<String, Object>> buildUserSyncRequest(
        String method,
        String userOpenId,
        String userName,
        String deptOpenId,
        String deptName,
        String tenantId,
        int deleted
    ) {
        return Mono.fromCallable(() -> {
            Map<String, Object> userDto = new HashMap<>();
            userDto.put("userOpenId", userOpenId);
            userDto.put("tenantId", tenantId);
            userDto.put("userName", userName);
            userDto.put("deptOpenId", deptOpenId);
            userDto.put("deptName", deptName);
            userDto.put("deleted", deleted);

            Map<String, Object> payload = new HashMap<>();
            payload.put("method", method);
            payload.put("users", Collections.singletonList(userDto));

            try {
                log.info("调用用户管理API，请求参数: {}", objectMapper.writeValueAsString(payload));
                log.info("请求头: {}", baseHeaders);
            } catch (Exception e) {
                log.warn("序列化请求参数时出错: {}", e.getMessage());
            }
            return payload;
        });
    }

    private Mono<Map<String, Object>> callUserSyncApi(Map<String, Object> payload) {
        Mono<Map<String, Object>> pipeline = webClient.post()
            .uri("/super-help/openapi/v1/user/sync")
            .headers(h -> baseHeaders.forEach(h::add))
            .bodyValue(payload)
            .retrieve()
            .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
            .timeout(Duration.ofSeconds(60))
            .doOnNext(data -> {
                try {
                    log.info("用户管理API响应: {}", objectMapper.writeValueAsString(data));
                } catch (Exception e) {
                    log.info("用户管理API响应(序列化失败，原始对象输出): {}", data);
                }
            });

        return logExternalCall("/super-help/openapi/v1/user/sync", payload, pipeline);
    }

    // --------- 本段新增方法的内部通用辅助 ----------
    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    /**
     * 统一构建 returnCode/returnMessage/result/traceId 结构
     */
    private Map<String, Object> buildReturnMap(int code, String message, Object result, String traceId) {
        Map<String, Object> map = new HashMap<>();
        map.put("returnCode", code);
        map.put("returnMessage", message != null ? message : "");
        map.put("result", result != null ? result : "");
        map.put("traceId", traceId != null ? traceId : "");
        return map;
    }

    private int extractReturnCode(Map<String, Object> data) {
        if (data == null) return 200;
        Object rc = data.get("returnCode");
        if (rc instanceof Number) return ((Number) rc).intValue();
        Object code = data.get("code");
        if (code instanceof Number) return ((Number) code).intValue();
        return 200;
    }

    private String extractTraceId(Map<String, Object> data) {
        if (data == null) return "";
        Object tid = data.get("traceId");
        return tid != null ? String.valueOf(tid) : "";
    }

    private String extractReturnMessage(Map<String, Object> data, String defaultMsg) {
        if (data == null) return defaultMsg;
        Object rm = data.get("returnMessage");
        if (rm == null) rm = data.get("msg");
        return rm != null ? String.valueOf(rm) : defaultMsg;
    }

    private <T> Mono<T> logExternalCall(String apiName, Object requestPayload, Mono<T> publisher) {
        return Mono.defer(() -> {
            log.info("调用外部接口 [{}] 请求参数: {}", apiName, toJsonSafe(requestPayload));
            return publisher
                .doOnNext(resp -> log.info("外部接口 [{}] 响应: {}", apiName, toJsonSafe(resp)))
                .doOnError(err -> log.error("外部接口 [{}] 异常: {}", apiName, err.getMessage()));
        });
    }

    private String toJsonSafe(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return String.valueOf(obj);
        }
    }

    /**
     * 将字符串列表转换为JsonNode
     */
    private JsonNode convertStringListToJsonNode(List<String> stringList) {
        if (stringList == null || stringList.isEmpty()) {
            return objectMapper.createArrayNode();
        }
        return objectMapper.valueToTree(stringList);
    }
    
    /**
     * 将角色列表字符串转换为JSON字符串，用于DatabaseClient
     */
    private String convertRoleListToJsonString(String roleList) {
        try {
            if (roleList == null || roleList.trim().isEmpty()) {
                return "[]";
            }
            List<String> roles = Arrays.asList(roleList.split(","));
            return objectMapper.writeValueAsString(roles);
        } catch (Exception e) {
            log.error("转换角色列表为JSON失败", e);
            return "[]";
        }
    }
}

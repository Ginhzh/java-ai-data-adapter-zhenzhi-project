package com.weichai.knowledge.config;

import com.weichai.knowledge.exception.ApiException;
import com.weichai.knowledge.exception.BusinessException;
import com.weichai.knowledge.exception.ExternalApiException;
import com.weichai.knowledge.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.http.converter.HttpMessageNotReadableException;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 全局异常处理器
 * 统一处理所有异常，返回一致的响应格式
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 处理API业务异常
     */
    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, Object>> handleApiException(ApiException ex) {
        log.error("API业务异常: errorCode={}, message={}, traceId={}", 
                 ex.getErrorCode(), ex.getMessage(), ex.getTraceId(), ex);
        
        Map<String, Object> body = createErrorResponse(
            ex.getErrorCode(), ex.getMessage(), ex.getTraceId()
        );
        
        HttpStatus httpStatus = getHttpStatus(ex.getErrorCode());
        return new ResponseEntity<>(body, httpStatus);
    }
    
    /**
     * 处理参数验证异常（自定义）
     */
    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Map<String, Object>> handleValidationException(ValidationException ex) {
        log.warn("参数验证异常: {}", ex.getMessage());
        
        Map<String, Object> body = createErrorResponse(
            400, "参数验证失败: " + ex.getMessage(), ex.getTraceId()
        );
        
        return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
    }
    
    /**
     * 处理业务逻辑异常
     */
    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<Map<String, Object>> handleBusinessException(BusinessException ex) {
        log.warn("业务逻辑异常: errorCode={}, message={}", ex.getErrorCode(), ex.getMessage());
        
        Map<String, Object> body = createErrorResponse(
            ex.getErrorCode(), ex.getMessage(), ex.getTraceId()
        );
        
        HttpStatus httpStatus = getHttpStatus(ex.getErrorCode());
        return new ResponseEntity<>(body, httpStatus);
    }
    
    /**
     * 处理外部API异常
     */
    @ExceptionHandler(ExternalApiException.class)
    public ResponseEntity<Map<String, Object>> handleExternalApiException(ExternalApiException ex) {
        log.error("外部API异常: api={}, errorCode={}, message={}", 
                 ex.getApiName(), ex.getErrorCode(), ex.getMessage(), ex);
        
        Map<String, Object> body = createErrorResponse(
            ex.getErrorCode(), ex.getMessage(), ex.getTraceId()
        );
        body.put("apiName", ex.getApiName());
        
        HttpStatus httpStatus = getHttpStatus(ex.getErrorCode());
        return new ResponseEntity<>(body, httpStatus);
    }

    /**
     * 处理Spring参数验证异常
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleMethodArgumentNotValidException(
            MethodArgumentNotValidException ex, WebRequest request) {
        
        String errors = ex.getBindingResult().getAllErrors().stream()
            .map(error -> {
                String fieldName = ((FieldError) error).getField();
                String errorMessage = error.getDefaultMessage();
                return fieldName + ": " + errorMessage;
            })
            .collect(Collectors.joining(", "));
        
        log.warn("Spring参数验证失败: {}, 请求路径: {}", errors, request.getDescription(false));
        
        Map<String, Object> body = createErrorResponse(
            400, "参数验证失败: " + errors, generateTraceId()
        );
        
        return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
    }

    /**
     * 处理JSON解析异常
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleHttpMessageNotReadableException(
            HttpMessageNotReadableException ex, WebRequest request) {
        
        log.warn("JSON解析失败: {}, 请求路径: {}", ex.getMessage(), request.getDescription(false));
        
        String message = "JSON格式错误，请检查请求体格式";
        if (ex.getMessage().contains("Cannot deserialize")) {
            message = "JSON字段类型不匹配，请检查数据类型";
        }
        
        Map<String, Object> body = createErrorResponse(400, message, generateTraceId());
        
        return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
    }

    /**
     * 处理非法参数异常
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(
            IllegalArgumentException ex, WebRequest request) {
        
        log.warn("非法参数异常: {}, 请求路径: {}", ex.getMessage(), request.getDescription(false));
        
        Map<String, Object> body = createErrorResponse(
            400, "参数错误: " + ex.getMessage(), generateTraceId()
        );
        
        return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
    }

    /**
     * 处理运行时异常
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> handleRuntimeException(
            RuntimeException ex, WebRequest request) {
        
        log.error("运行时异常: {}, 请求路径: {}", ex.getMessage(), request.getDescription(false), ex);
        
        Map<String, Object> body = createErrorResponse(
            500, "服务器内部错误: " + ex.getMessage(), generateTraceId()
        );
        
        return new ResponseEntity<>(body, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * 处理其他所有异常
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericException(
            Exception ex, WebRequest request) {
        
        log.error("未知异常: {}, 请求路径: {}", ex.getMessage(), request.getDescription(false), ex);
        
        Map<String, Object> body = createErrorResponse(
            500, "系统异常，请联系管理员", generateTraceId()
        );
        
        return new ResponseEntity<>(body, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    /**
     * 创建统一的错误响应体
     */
    private Map<String, Object> createErrorResponse(int errorCode, String message, String traceId) {
        Map<String, Object> body = new HashMap<>();
        body.put("returnCode", errorCode);
        body.put("returnMessage", message);
        body.put("result", "");
        body.put("traceId", traceId);
        body.put("success", false);
        return body;
    }
    
    /**
     * 生成追踪ID
     */
    private String generateTraceId() {
        return "trace-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
    }
    
    /**
     * 根据错误码获取HTTP状态码
     */
    private HttpStatus getHttpStatus(int errorCode) {
        return switch (errorCode) {
            case 400 -> HttpStatus.BAD_REQUEST;
            case 401 -> HttpStatus.UNAUTHORIZED;
            case 403 -> HttpStatus.FORBIDDEN;
            case 404 -> HttpStatus.NOT_FOUND;
            case 409 -> HttpStatus.CONFLICT;
            case 422 -> HttpStatus.UNPROCESSABLE_ENTITY;
            case 502 -> HttpStatus.BAD_GATEWAY;
            default -> errorCode >= 500 ? HttpStatus.INTERNAL_SERVER_ERROR : HttpStatus.BAD_REQUEST;
        };
    }
}
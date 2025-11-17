# 文件删除服务 API 使用指南

## 概述

本文档介绍如何使用文件删除服务的RESTful API接口。该服务提供文件删除功能，支持单个文件删除、批量删除和状态查询。

## 基本信息

- **基础URL**: `http://localhost:8081/api/v1/file-deletion`
- **Content-Type**: `application/json`
- **响应格式**: JSON

## API 接口列表

### 1. 删除单个文件

**接口地址**: `POST /api/v1/file-deletion/delete`

**功能描述**: 根据文件信息删除知识库中的文档，包括下线操作和状态更新

**请求体格式**:
```json
{
  "fileId": "文件ID",
  "messageType": "FILE_DEL",
  "fileMetadata": {
    "systemName": "系统名称",
    "fileNumber": "文件编号", 
    "fileName": "文件名称",
    "filePath": "文件路径",
    "version": "版本号（可选）",
    "format": "文件格式",
    "creator": "创建者",
    "updater": "更新者",
    "objectKey": "对象键",
    "bucketName": "存储桶名称",
    "createTime": 时间戳,
    "updateTime": 时间戳,
    "description": "描述"
  },
  "messageTaskId": "消息任务ID（可选）",
  "fileAddRoleList": ["角色列表"],
  "fileAddUserList": ["用户列表"],
  "messageDateTime": "消息时间（可选）"
}
```

**请求示例**:
```bash
curl -X POST http://localhost:8081/api/v1/file-deletion/delete \
  -H "Content-Type: application/json" \
  -d '{
    "file_id": "FILE_123456",
    "file_metadata": {
      "system_name": "WPROS",
      "file_number": "DOC001",
      "file_name": "测试文档.pdf",
      "file_path": "/documents/test",
      "version": "1.0"
    }
  }'
```

**成功响应示例**:
```json
{
  "status": "success",
  "system_name": "WPROS",
  "file_id": "FILE_123456",
  "file_number": "DOC001",
  "file_name": "测试文档.pdf",
  "file_zhenzhi_id": "zh_123456",
  "version": "1.0",
  "is_delete": 1,
  "message": "文件删除处理成功",
  "timestamp": "2025-08-14T18:30:00"
}
```

**失败响应示例**:
```json
{
  "status": "error",
  "system_name": "WPROS",
  "file_id": "FILE_123456",
  "file_number": "DOC001",
  "file_name": "测试文档.pdf",
  "is_delete": 0,
  "message": "查询甄知ID失败",
  "timestamp": "2025-08-14T18:30:00"
}
```

### 2. 批量删除文件

**接口地址**: `POST /api/v1/file-deletion/batch-delete`

**功能描述**: 批量删除多个文件，返回每个文件的处理结果

**请求体格式**:
```json
[
  {
    "file_id": "文件ID1",
    "file_metadata": {
      "system_name": "系统名称",
      "file_number": "文件编号1",
      "file_name": "文件名称1",
      "file_path": "文件路径1",
      "version": "版本号1"
    }
  },
  {
    "file_id": "文件ID2",
    "file_metadata": {
      "system_name": "系统名称",
      "file_number": "文件编号2",
      "file_name": "文件名称2",
      "file_path": "文件路径2",
      "version": "版本号2"
    }
  }
]
```

**请求示例**:
```bash
curl -X POST http://localhost:8081/api/v1/file-deletion/batch-delete \
  -H "Content-Type: application/json" \
  -d '[
    {
      "file_id": "FILE_001",
      "file_metadata": {
        "system_name": "WPROS",
        "file_number": "DOC001",
        "file_name": "文档1.pdf",
        "file_path": "/docs/file1",
        "version": "1.0"
      }
    },
    {
      "file_id": "FILE_002", 
      "file_metadata": {
        "system_name": "WPROS",
        "file_number": "DOC002",
        "file_name": "文档2.pdf",
        "file_path": "/docs/file2",
        "version": "1.0"
      }
    }
  ]'
```

**响应示例**:
```json
{
  "status": "completed",
  "total": 2,
  "success": 1,
  "failed": 1,
  "results": [
    {
      "fileId": "FILE_001",
      "result": {
        "status": "success",
        "system_name": "WPROS",
        "file_id": "FILE_001",
        "message": "文件删除处理成功"
      }
    },
    {
      "fileId": "FILE_002",
      "result": {
        "status": "error", 
        "message": "查询甄知ID失败"
      }
    }
  ]
}
```

### 3. 查询文件删除状态

**接口地址**: `GET /api/v1/file-deletion/status`

**功能描述**: 根据文件ID和系统名称查询文件的删除状态

**请求参数**:
- `fileId`: 文件ID（必填）
- `systemName`: 系统名称（必填）

**请求示例**:
```bash
curl "http://localhost:8081/api/v1/file-deletion/status?fileId=FILE_123456&systemName=WPROS"
```

**响应示例**:
```json
{
  "fileId": "FILE_123456",
  "systemName": "WPROS", 
  "status": "unknown",
  "message": "状态查询功能待实现",
  "timestamp": "2025-08-14T18:30:00"
}
```

### 4. 健康检查

**接口地址**: `GET /api/v1/file-deletion/health`

**功能描述**: 检查文件删除服务的健康状态

**请求示例**:
```bash
curl http://localhost:8081/api/v1/file-deletion/health
```

**响应示例**:
```json
{
  "status": "healthy",
  "service": "FileDelService",
  "timestamp": "2025-08-14T18:30:00",
  "version": "1.0.0"
}
```

## 错误码说明

| HTTP状态码 | 说明 |
|------------|------|
| 200 | 请求成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 500 | 服务器内部错误 |

## 参数验证规则

1. **file_id**: 不能为空
2. **system_name**: 不能为空
3. **file_number**: 不能为空
4. **file_name**: 不能为空
5. **file_path**: 不能为空
6. **version**: 可选参数
7. **批量删除限制**: 单次最多100个文件

## 使用注意事项

1. **响应式处理**: 所有接口都采用响应式设计，支持高并发
2. **错误处理**: 接口具备完善的错误处理机制
3. **参数验证**: 所有必填参数都会进行验证
4. **状态码**: 通过`is_delete`字段判断删除是否成功（1=成功，0=失败）
5. **日志记录**: 所有操作都会记录详细日志

## Java客户端调用示例

```java
// 使用WebClient调用
@Service
public class FileDelClient {
    
    private final WebClient webClient;
    
    public FileDelClient(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder
            .baseUrl("http://localhost:8081")
            .build();
    }
    
    public Mono<FileDelResponse> deleteFile(FileDelRequest request) {
        return webClient.post()
            .uri("/api/v1/file-deletion/delete")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .retrieve()
            .bodyToMono(FileDelResponse.class);
    }
}
```

## Python客户端调用示例

```python
import requests
import json

def delete_file(file_id, system_name, file_number, file_name, file_path, version=None):
    url = "http://localhost:8081/api/v1/file-deletion/delete"
    
    data = {
        "file_id": file_id,
        "file_metadata": {
            "system_name": system_name,
            "file_number": file_number,
            "file_name": file_name,
            "file_path": file_path,
            "version": version
        }
    }
    
    response = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json=data
    )
    
    return response.json()

# 使用示例
result = delete_file(
    file_id="FILE_123456",
    system_name="WPROS",
    file_number="DOC001", 
    file_name="测试文档.pdf",
    file_path="/documents/test",
    version="1.0"
)

print(f"删除结果: {result['status']}")
print(f"消息: {result['message']}")
```

## 故障排查

1. **连接失败**: 检查服务是否启动，端口是否正确
2. **参数验证错误**: 确保所有必填参数都已提供
3. **删除失败**: 查看返回的错误消息，通常是甄知ID查询或知识库映射问题
4. **超时**: 检查网络连接和服务响应时间

## 版本更新记录

- **v1.0.0** (2025-08-14): 初始版本，支持单文件删除、批量删除、状态查询和健康检查
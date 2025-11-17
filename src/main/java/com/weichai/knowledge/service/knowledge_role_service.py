import asyncio
import sys
import os
import uuid
from http.client import HTTPException
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional, Tuple
from contextlib import asynccontextmanager
from functools import wraps

from src.configs.log_config import get_logger
from src.db.database import get_async_db
from sqlalchemy import select
from src.models.base import FileNotChange, RoleUserMessage
from src.utils.error_log_handler import ErrorLogHandler
from src.utils.knowledge_handler import KnowledgeHandler
from src.utils.role_user_handler import RoleUserHandler
from src.utils.message_handler import MessageHandler
from src.utils.cache_handler import CacheHandler
from src.schemas.schemas_comment import KnowledgeRoleSyncLog, UnstructuredDocument

logger = get_logger(__name__)
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# 常量定义
class Constants:
    """业务常量定义"""
    VIRTUAL_GROUP_PREFIX = "virtual_"
    SYSTEM_ADMIN_PREFIX = "system_"
    TENANT_ID = "85d5572986784a53a649726085691974"
    DEFAULT_PATH_NAME = "默认路径"
    
    # 缓存键定义 - 更新为包含系统标识的格式
    # 新格式: {system_name}:{service}:{operation}:{type}:{date}
    @staticmethod
    def get_verification_count_key(system_name: str, date: str = None) -> str:
        """获取验证计数键"""
        if date:
            return f"{system_name}:knowledge:verification:passed:{date}"
        return f"{system_name}:knowledge:verification:completed:count"
    
    @staticmethod  
    def get_permission_set_count_key(system_name: str, date: str = None) -> str:
        """获取权限设置成功计数键"""
        if date:
            return f"{system_name}:knowledge:operation:success:{date}"
        return f"{system_name}:knowledge:permission:set:success:count"
    
    # 保留旧格式以向后兼容
    VERIFICATION_COUNT_KEY = "knowledge:verification:completed:count"
    PERMISSION_SET_COUNT_KEY = "knowledge:permission:set:success:count"
    
    class MessageType:
        FILE_NOT_CHANGE = "FILE_NOT_CHANGE"
        ADD_ROLE = "ADD_ROLE"
    
    class ErrorType:
        FILE_NOT_ONLINE = 5
    
    class JudgeMode:
        CHECK_FILE_STATUS = 1
    
    class OpType:
        DOCUMENT_PERMISSION = 2
    
    class MemberType:
        USER = 1
        ROLE = 2


class BusinessError(Exception):
    """业务异常类"""
    def __init__(self, message: str, error_type: str = "BUSINESS_ERROR"):
        self.message = message
        self.error_type = error_type
        super().__init__(self.message)


@asynccontextmanager
async def error_handling_context(session, message: FileNotChange):
    """异常处理上下文管理器"""
    error_logger = ErrorLogHandler()
    try:
        await error_logger.async_start(session)
        yield error_logger
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"处理消息时发生错误: {exc}")
        raise BusinessError(f"处理失败: {str(exc)}")
    finally:
        await error_logger.async_close()
        logger.info("关闭错误日志服务")


# 主处理函数
def format_repo_name(file_path: str, system_name: str = "") -> str:
    """
    格式化知识库名称
    
    规则:
    1. 对于WPROS_STRUCT系统，在文件路径前添加'WPROS_STRUCT_'前缀
    2. 将file_path中的斜杠/替换为短横线-
    3. 只取file_path的最后三层目录（如果系统为WPROS或WPROS_TEST，则取前三层目录）
    4. 确保最后一个片段是路径而不是文件名
    
    Args:
        file_path: 文件路径
        system_name: 系统名称，用于决定目录选择方式
        
    Returns:
        str: 格式化后的知识库名称
    """
    # 处理空路径的情况
    if not file_path or file_path == '/':
        return Constants.DEFAULT_PATH_NAME
    
    # 特殊处理WPROS_STRUCT系统
    if system_name == 'WPROS_STRUCT':
        file_path = 'WPROS_STRUCT_' + file_path
    
    # 如果最后一个部分包含扩展名，则移除它
    if '.' in os.path.basename(file_path) and len(os.path.basename(file_path).split('.')) > 1:
        # 只有当最后部分是文件名（包含扩展名）时才移除
        path_only = os.path.dirname(file_path)
    else:
        # 否则保留完整路径
        path_only = file_path
    
    # 按斜杠分割路径
    path_parts = path_only.strip('/').split('/')
    
    # 过滤掉空字符串
    path_parts = [part for part in path_parts if part]
    
    # 处理没有有效路径片段的情况
    if not path_parts:
        return Constants.DEFAULT_PATH_NAME
    
    # 根据系统名称选择目录
    if system_name in ["WPROS", "WPROS_STRUCT"]:
        # 取前三层目录，如果不足三层则全部保留
        path_parts = path_parts[:min(2, len(path_parts))] if path_parts else []
    else:
        # 取后三层目录，如果不足三层则全部保留
        path_parts = path_parts[-min(3, len(path_parts)):] if path_parts else []
    
    # 将路径片段用短横线连接
    path_str = "-".join(path_parts)
    
    return path_str


class FileProcessorContext:
    """文件处理上下文，用于存储处理过程中的共享数据"""
    
    def __init__(self, message: FileNotChange):
        self.message = message
        self.zhenzhi_file_id: Optional[str] = None
        self.repo_id: Optional[str] = None
        self.department_id: Optional[str] = None
        self.repo_name: Optional[str] = None
        self.admin_open_id: Optional[str] = None
        self.knowledge_service: Optional[KnowledgeHandler] = None
        self.roleuser_handler: Optional[RoleUserHandler] = None
        self.cache_handler: Optional[CacheHandler] = None


async def prepare_processing_context(context: FileProcessorContext, session, error_logger: ErrorLogHandler) -> None:
    """准备处理上下文 - 获取必要的基础数据"""
    message = context.message
    
    # 创建缓存处理器
    context.cache_handler = CacheHandler()
    await context.cache_handler.start()
    
    # 创建知识服务实例
    context.knowledge_service = await KnowledgeHandler.create()
    
    # 查询甄知文件ID
    zhenzhi_file_id_info = await context.knowledge_service.query_zhenzhi_file_id(
        file_number=message.fileMetadata.fileNumber,
        system_name=message.fileMetadata.systemName
    )
    logger.info(f"获取到甄知ID结果: {zhenzhi_file_id_info}")
    
    if not zhenzhi_file_id_info:
        raise BusinessError(f"查询[{message.fileMetadata.fileNumber}]的甄知ID失败")
    
    context.zhenzhi_file_id = zhenzhi_file_id_info.get("zhenzhi_file_id")
    logger.info(f"获取到甄知文档ID: {context.zhenzhi_file_id}")
    
    # 准备仓库相关信息
    context.repo_name = format_repo_name(message.fileMetadata.filePath, message.fileMetadata.systemName)
    context.admin_open_id = f"{Constants.SYSTEM_ADMIN_PREFIX}{message.fileMetadata.systemName}"
    
    # 查询仓库映射
    repo_mapping = await context.knowledge_service.query_repo_mapping(
        message.fileMetadata.systemName, context.repo_name
    )
    logger.info(f"repo_mapping为{repo_mapping}")
    
    if repo_mapping:
        context.repo_id = repo_mapping.get("repo_id")
        context.department_id = repo_mapping.get("department_id")
        logger.info(f"已找到知识库映射，知识库ID: {context.repo_id}, 部门ID: {context.department_id}")
    
    # 创建角色用户处理器
    message_user = RoleUserMessage(
        roleId=f"{Constants.VIRTUAL_GROUP_PREFIX}{context.zhenzhi_file_id}",
        messageTaskId=message.messageTaskId,
        messageType=Constants.MessageType.ADD_ROLE,
        roleName=message.fileMetadata.fileName,
        systemName=message.fileMetadata.systemName,
        messageDateTime=message.messageDateTime,
        addUserList=message.fileAddUserList,
        delUserList=message.fileDelUserList,
        tenantId=Constants.TENANT_ID
    )
    
    context.roleuser_handler = await RoleUserHandler.create(
        message=message_user, session=session, error_logger=error_logger
    )


async def handle_user_role_binding(context: FileProcessorContext) -> None:
    """处理用户和角色绑定/解绑"""
    message = context.message
    virtual_group_id = f"{Constants.VIRTUAL_GROUP_PREFIX}{context.zhenzhi_file_id}"
    
    # 增加用户
    if message.fileAddUserList:
        logger.info("开始增加用户")
        bind_success = await context.roleuser_handler.bind_user_with_virtual_group(virtualGroupId=virtual_group_id)
        
        # 用户绑定成功后记录权限设置成功统计
        if bind_success:
            try:
                # 获取当前日期
                today = datetime.now().strftime('%Y-%m-%d')
                
                # 使用新格式的Redis键，包含系统名称和日期
                permission_key = Constants.get_permission_set_count_key(context.message.fileMetadata.systemName, today)
                
                permission_count = await context.cache_handler.increment(permission_key)
                logger.info(f"用户绑定成功 - 系统: {context.message.fileMetadata.systemName}, 日期: {today}, 权限设置成功总数: {permission_count}")
            except Exception as e:
                logger.warning(f"记录用户绑定成功统计失败，但不影响主流程: {str(e)}")
    
    # 删除用户
    if message.fileDelUserList:
        logger.info("开始删除用户")
        unbind_success = await context.roleuser_handler.unbind_user_with_virtual_group(virtualGroupId=virtual_group_id)
        
        # 用户解绑成功后记录权限设置成功统计
        if unbind_success:
            try:
                # 获取当前日期
                today = datetime.now().strftime('%Y-%m-%d')
                
                # 使用新格式的Redis键，包含系统名称和日期
                permission_key = Constants.get_permission_set_count_key(context.message.fileMetadata.systemName, today)
                
                permission_count = await context.cache_handler.increment(permission_key)
                logger.info(f"用户解绑成功 - 系统: {context.message.fileMetadata.systemName}, 日期: {today}, 权限设置成功总数: {permission_count}")
            except Exception as e:
                logger.warning(f"记录用户解绑成功统计失败，但不影响主流程: {str(e)}")


async def set_doc_admin_wrapper(context: FileProcessorContext, session, action: str, role_list: List, headers: Optional[Dict]) -> None:
    """文档权限设置包装器"""
    if not role_list or not context.repo_id:
        return
    
    try:
        # 先检查并获取可能需要合并的现有角色
        existing_roles = None
        if action == "add":
            existing_roles = await sync_unstructured_document_roles(context, session, add_roles=role_list)
        elif action == "del":
            existing_roles = await sync_unstructured_document_roles(context, session, del_roles=role_list)
        
        # 如果返回了现有角色（说明文档状态为0），需要合并角色一起设置
        final_role_list = role_list
        if existing_roles and action == "add":
            # 状态为0时，需要把现有角色和新角色一起设置
            all_roles = set(existing_roles + role_list)
            final_role_list = list(all_roles)
            logger.info(f"文档状态为0，合并现有角色进行完整权限设置。现有: {existing_roles}, 新增: {role_list}, 最终: {final_role_list}")
        elif existing_roles and action == "del":
            # 状态为0时删除操作，设置剩余的角色
            remaining_roles = set(existing_roles) - set(role_list)
            final_role_list = list(remaining_roles) if remaining_roles else []
            logger.info(f"文档状态为0，删除指定角色后设置剩余权限。现有: {existing_roles}, 删除: {role_list}, 最终: {final_role_list}")
            # 对于删除操作，如果最终没有角色了，可能需要特殊处理
            if not final_role_list:
                logger.warning("删除操作后没有剩余角色，跳过权限设置")
                return
        
        logger.info(f"开始执行修改文档权限 - {action}")
        doc_admin_result = await context.knowledge_service.set_doc_admin(
            system_name=context.message.fileMetadata.systemName,
            doc_guid=context.zhenzhi_file_id,
            repo_id=context.repo_id,
            role_list=final_role_list,
            user_list=[],
            action="add" if existing_roles else action,  # 如果是状态0的文档，统一使用add操作
            groupGuid=context.department_id,
            # admin_open_id=context.admin_open_id, # 【删除】这行已失效的参数
            op_type=Constants.OpType.DOCUMENT_PERMISSION,
            headers=headers # 【新增】使用传入的headers
        )
        logger.info(f"设置文档权限结果: {doc_admin_result}")
        
        # ===== 新增：权限设置成功后记录统计（使用新Redis键格式） =====
        if doc_admin_result and doc_admin_result.get("status") == "success":
            try:
                # 获取当前日期
                today = datetime.now().strftime('%Y-%m-%d')
                
                # 使用新格式的Redis键，包含系统名称和日期
                permission_key = Constants.get_permission_set_count_key(context.message.fileMetadata.systemName, today)
                
                permission_count = await context.cache_handler.increment(permission_key)
                logger.info(f"权限设置成功 - 系统: {context.message.fileMetadata.systemName}, 日期: {today}, 权限设置成功总数: {permission_count}")
                
                # 注意：数据库同步已经在函数开头完成，这里不需要重复调用
                
            except Exception as e:
                logger.warning(f"记录权限设置成功统计失败，但不影响主流程: {str(e)}")
        
    except Exception as e:
        logger.warning(f"设置文档权限失败，但继续流程: {str(e)}")


async def handle_file_online_operations(context: FileProcessorContext, session, headers: Optional[Dict]) -> None:
    """处理文件上线后的一揽子操作"""
    message = context.message
    
    logger.info(f"文件 {context.zhenzhi_file_id} 文档成功上线")
    
    # 处理用户角色绑定
    await handle_user_role_binding(context)
    
    # 处理权限添加
    if message.fileAddRoleList:
        await set_doc_admin_wrapper(context, session, "add", message.fileAddRoleList, headers) # 【修改】传入session和headers
    
    # 处理权限删除
    if message.fileDelRoleList:
        await set_doc_admin_wrapper(context, session, "del", message.fileDelRoleList, headers) # 【修改】传入session和headers


async def handle_file_not_online(context: FileProcessorContext, session, error_logger: ErrorLogHandler) -> None:
    """处理文件未上线的情况"""
    logger.warning(f"文档 {context.zhenzhi_file_id} 未成功上线，跳过权限处理")
    await error_logger.async_log_simple(
        error_type=Constants.ErrorType.FILE_NOT_ONLINE,
        file_id=context.message.fileId,
        step="文档未入库",
        error_msg=f"文档 {context.zhenzhi_file_id} 未成功上线",
        params={
            "task_id": context.message.messageTaskId,
            "message_type": context.message.messageType,
            "zhenzhi_file_id": context.zhenzhi_file_id
        }
    )
    
    # ===== 新增：同步更新数据库状态为未上线 =====
    await sync_unstructured_document_status(context, session, status=0)
    
    # ===== 新增：如果有权限变更请求，也要同步到数据库 =====
    message = context.message
    if message.fileAddRoleList or message.fileDelRoleList:
        add_roles = message.fileAddRoleList if message.fileAddRoleList else None
        del_roles = message.fileDelRoleList if message.fileDelRoleList else None
        await sync_unstructured_document_roles(context, session, add_roles=add_roles, del_roles=del_roles)


async def sync_unstructured_document_roles(context: FileProcessorContext, session, add_roles: List[str] = None, del_roles: List[str] = None) -> Optional[List[str]]:
    """同步非结构化文档记录表中的role_list"""
    try:
        if not context.zhenzhi_file_id:
            logger.warning("缺少甄知文件ID，无法同步非结构化文档角色")
            return
        
        # 查询现有记录
        existing_doc = await session.execute(
            select(UnstructuredDocument).filter(
                UnstructuredDocument.system_name == context.message.fileMetadata.systemName,
                UnstructuredDocument.file_number == context.message.fileMetadata.fileNumber,
                UnstructuredDocument.zhenzhi_file_id == context.zhenzhi_file_id
            )
        )
        doc_record = existing_doc.scalar_one_or_none()
        
        if not doc_record:
            logger.warning(f"未找到文档记录，无法同步角色列表: {context.zhenzhi_file_id}")
            return None
        
        # 获取现有角色列表，需要处理可能的逗号分隔字符串
        raw_roles = doc_record.role_list or []
        current_roles = set()
        for role in raw_roles:
            if isinstance(role, str) and ',' in role:
                # 处理逗号分隔的字符串
                current_roles.update([r.strip() for r in role.split(',') if r.strip()])
            else:
                current_roles.add(role)
        
        logger.info(f"解析现有角色列表: 原始={raw_roles}, 解析后={list(current_roles)}")
        
        # 如果文档状态为0（未上线），说明之前的权限从未设置过
        # 需要返回现有的角色列表，以便与新角色一起设置权限
        if doc_record.status == 0:
            # 计算需要一起设置的完整角色列表
            roles_to_set = current_roles.copy()
            if add_roles:
                roles_to_set.update(add_roles)
            if del_roles:
                roles_to_set.difference_update(del_roles)
            
            # 更新数据库记录，确保每个角色都是独立的字符串
            doc_record.role_list = list(roles_to_set) if roles_to_set else None
            doc_record.updated_at = datetime.now()
            doc_record.status = 1  # 设置为已上线
            
            logger.info(f"文档状态为0，需要完整设置权限。现有角色: {list(current_roles)}, 最终角色: {list(roles_to_set)}")
            await session.commit()
            return list(current_roles)  # 返回现有角色列表，用于合并权限设置
        else:
            # 状态为1的文档，只需要增量处理
            if add_roles:
                current_roles.update(add_roles)
                logger.info(f"添加角色到文档 {context.zhenzhi_file_id}: {add_roles}")
            
            if del_roles:
                current_roles.difference_update(del_roles)
                logger.info(f"从文档 {context.zhenzhi_file_id} 删除角色: {del_roles}")
            
            # 更新记录，确保每个角色都是独立的字符串
            doc_record.role_list = list(current_roles) if current_roles else None
            doc_record.updated_at = datetime.now()
            
            logger.info(f"更新非结构化文档记录，最终角色列表: {doc_record.role_list}")
            await session.commit()
            return None  # 状态为1的文档不需要返回现有角色
        
    except Exception as e:
        logger.error(f"同步非结构化文档角色时发生错误: {e}")
        await session.rollback()
        # 不抛出异常，避免影响主流程


async def sync_unstructured_document_status(context: FileProcessorContext, session, status: int = 0) -> None:
    """同步非结构化文档记录表中的状态"""
    try:
        if not context.zhenzhi_file_id:
            logger.warning("缺少甄知文件ID，无法同步非结构化文档状态")
            return
        
        # 查询现有记录
        existing_doc = await session.execute(
            select(UnstructuredDocument).filter(
                UnstructuredDocument.system_name == context.message.fileMetadata.systemName,
                UnstructuredDocument.file_number == context.message.fileMetadata.fileNumber,
                UnstructuredDocument.zhenzhi_file_id == context.zhenzhi_file_id
            )
        )
        doc_record = existing_doc.scalar_one_or_none()
        
        if not doc_record:
            logger.warning(f"未找到文档记录，无法同步状态: {context.zhenzhi_file_id}")
            return
        
        # 更新现有记录的状态
        doc_record.status = status
        doc_record.updated_at = datetime.now()
        logger.info(f"更新非结构化文档记录状态为: {status}")
        
        await session.commit()
        
    except Exception as e:
        logger.error(f"同步非结构化文档状态时发生错误: {e}")
        await session.rollback()
        # 不抛出异常，避免影响主流程


async def record_sync_log(context: FileProcessorContext, session) -> None:
    """记录权限同步日志"""
    try:
        if not context.zhenzhi_file_id:
            logger.warning("处理成功但缺少必要字段，无法记录到同步日志表")
            return
        
        message = context.message
        # 兼容性提取 file_number，避免因字段命名问题导致空值
        file_number_value = None
        try:
            if hasattr(message.fileMetadata, "fileNumber"):
                file_number_value = getattr(message.fileMetadata, "fileNumber")
            # 备用：处理可能出现的下划线命名
        except Exception as _extract_err:
            logger.warning(f"提取 file_number 失败: {_extract_err}")

        logger.info(f"准备写入同步日志，file_number={file_number_value}")
        sync_log = KnowledgeRoleSyncLog(
            id=str(uuid.uuid4()),
            message_task_id=message.messageTaskId,
            message_type=message.messageType,
            message_datetime=message.messageDateTime,
            file_id=message.fileId,
            file_name=message.fileMetadata.fileName,
            system_name=message.fileMetadata.systemName,
            repo_id=context.repo_id,
            file_number=file_number_value,
            zhenzhi_file_id=context.zhenzhi_file_id,
            add_role_list=message.fileAddRoleList if message.fileAddRoleList else None,
            add_user_list=message.fileAddUserList if message.fileAddUserList else None,
            del_role_list=message.fileDelRoleList if message.fileDelRoleList else None,
            del_user_list=message.fileDelUserList if message.fileDelUserList else None,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        session.add(sync_log)
        await session.commit()
        logger.info(f"成功记录文件权限同步日志，ID: {sync_log.id}")
        
    except Exception as log_exc:
        logger.error(f"记录同步日志时发生错误: {log_exc}")
        await session.rollback()


async def process_file_not_change_message(message: FileNotChange):
    """处理文件未改变消息 - 重构后的主函数"""
    async with get_async_db() as session:
        async with error_handling_context(session, message) as error_logger:
            logger.info("开始执行操作-------------------------》")
            
            if message.messageType != Constants.MessageType.FILE_NOT_CHANGE:
                raise BusinessError(f"不支持的消息类型: {message.messageType}")
            
            # 创建处理上下文
            context = FileProcessorContext(message)
            
            # 准备阶段：获取基础数据
            await prepare_processing_context(context, session, error_logger)
            
            # === 主要修改区域 开始 ===

            # 1. 在这里生成一次性的、任务专用的 headers
            task_headers = None
            if context.admin_open_id:
                try:
                    task_headers = context.knowledge_service.generate_headers_with_signature(context.admin_open_id)
                    logger.info("为当前任务成功生成专用请求头")
                except ValueError as e:
                    # 如果签名生成失败，可以根据业务决定是中止还是继续
                    logger.error(f"生成签名失败，后续需要签名的操作将无法执行: {e}")
                    # 在此可以引发BusinessError中止流程，或允许流程继续但API调用会失败
                    raise BusinessError(f"生成签名失败: {e}")
            
            # 2. 检查文件状态时，传入 task_headers
            file_status = await context.knowledge_service.query_file_status(
                [context.zhenzhi_file_id], 
                judge_mode=Constants.JudgeMode.CHECK_FILE_STATUS,
                headers=task_headers  # 【新增】传入headers
            )
            
            # 文件处理阶段
            if file_status.get("success"):
                # 文件已上线，直接处理
                
                # ===== 新增：权限校验通过后记录统计（使用新Redis键格式） =====
                try:
                    # 获取当前日期
                    today = datetime.now().strftime('%Y-%m-%d')
                    
                    # 使用新格式的Redis键，包含系统名称和日期
                    verification_key = Constants.get_verification_count_key(context.message.fileMetadata.systemName, today)
                    
                    verification_count = await context.cache_handler.increment(verification_key)
                    logger.info(f"权限校验通过（文档状态查询成功） - 系统: {context.message.fileMetadata.systemName}, 日期: {today}, 验证完成总数: {verification_count}")
                except Exception as e:
                    logger.warning(f"记录验证完成统计失败，但不影响主流程: {str(e)}")
                
                # 3. 将 task_headers 传递给后续处理函数
                await handle_file_online_operations(context, session, task_headers) # 【修改】增加session和headers参数
            else:
                # 文件未上线，记录错误日志
                await handle_file_not_online(context, session, error_logger)
            
            # === 主要修改区域 结束 ===
            
            # 记录同步日志
            await record_sync_log(context, session)
            
            # 关闭缓存处理器
            if context.cache_handler:
                await context.cache_handler.close()
            
            return {"status": "success"}


# 配置 messageType 到模型类和处理函数路径的映射
handlers = {
    Constants.MessageType.FILE_NOT_CHANGE: (FileNotChange, "src.services.knowledge_role_service.process_file_not_change_message"),
}

if __name__ == "__main__":
    processor = MessageHandler(handlers)
    asyncio.run(processor.run())
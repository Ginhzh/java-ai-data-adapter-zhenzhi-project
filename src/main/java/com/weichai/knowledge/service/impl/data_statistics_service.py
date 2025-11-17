import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy import func, and_, text, desc, or_, case
from sqlalchemy.orm import Session
import json
from src.db.database import get_db, SessionLocal
from src.schemas.schemas_comment import KafkaMessageLog, UnstructuredDocument, KnowledgeRoleSyncLog, VirtualGroupSyncLog
from src.configs.log_config import get_logger
from src.utils.cache_handler import CacheHandler

logger = get_logger(__name__)

class DataStatisticsService:
    """
    数据统计服务，提供数据接入和成功量的统计功能
    """

    def __init__(self):
        """初始化数据统计服务"""
        # 初始化缓存处理器，用于存储统计结果
        self.cache_handler = CacheHandler()
        # 异步初始化需要使用异步方法启动
        self._initialized = False
        logger.info("DataStatisticsService初始化完成")
        
    async def _ensure_initialized(self):
        """确保服务已初始化"""
        if not self._initialized:
            try:
                # 初始化缓存服务
                await self.cache_handler.start()
                self._initialized = True
                logger.info("DataStatisticsService缓存服务初始化成功")
            except Exception as e:
                logger.error(f"DataStatisticsService缓存服务初始化失败: {str(e)}")
                # 即使缓存初始化失败，服务仍可降级使用
                
    async def shutdown(self):
        """关闭服务并释放资源"""
        logger.info("正在关闭DataStatisticsService...")
        try:
            # 关闭缓存处理器
            if hasattr(self, 'cache_handler') and self.cache_handler:
                try:
                    if hasattr(self.cache_handler, 'close'):
                        await self.cache_handler.close()
                        logger.info("数据统计服务缓存处理器已关闭")
                except Exception as e:
                    logger.error(f"关闭数据统计服务缓存处理器失败: {str(e)}")
            
            logger.info("DataStatisticsService已完全关闭")
        except Exception as e:
            logger.error(f"关闭DataStatisticsService时出错: {str(e)}")
            import traceback
            logger.error(f"关闭服务异常堆栈: {traceback.format_exc()}")
    
    def __del__(self):
        """析构函数，确保资源被释放"""
        try:
            import sys
            if hasattr(sys, 'is_finalizing') and sys.is_finalizing():
                # Python解释器正在关闭，只清空引用
                if hasattr(self, 'cache_handler'):
                    self.cache_handler = None
                return
                
            # 使用同步方法关闭缓存，但不记录日志
            if hasattr(self, 'cache_handler') and self.cache_handler:
                try:
                    self.cache_handler.stop()
                except Exception:
                    # 静默失败，不记录日志
                    self.cache_handler = None
        except Exception:
            # 静默失败，不记录日志
            pass

    async def get_daily_statistics(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        获取每日接入统计数据
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict: 包含每日统计数据的字典
        """
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 缓存键
        cache_key = f"daily_stats:{start_date}:{end_date}"
        
        # 尝试从缓存获取
        if self.cache_handler:
            cached_result = await self.cache_handler.get(cache_key)
            if cached_result:
                logger.info(f"从缓存获取每日统计数据: {start_date} 到 {end_date}")
                return cached_result
        
        logger.info(f"获取从{start_date}到{end_date}的每日接入统计")
        
        start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        
        # 使用数据库上下文管理器
        with get_db() as db:
            # 优化: 使用SQL聚合查询一次性获取所有日期的数据
            daily_stats = self._collect_daily_statistics_optimized(db, start_datetime, end_datetime)
            
            # 构建结果
            result = {
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date
                },
                "daily_statistics": daily_stats
            }
            
            # 缓存结果(5分钟)
            if self.cache_handler:
                await self.cache_handler.set(cache_key, result, expiry=300)
            
            return result
            
    def _collect_daily_statistics_optimized(self, db: Session, start_datetime: datetime, end_datetime: datetime) -> List[Dict]:
        """
        优化版：使用日期循环结合_get_incremental_stats方法来判断更新操作
        
        Args:
            db: 数据库会话
            start_datetime: 开始日期时间
            end_datetime: 结束日期时间
            
        Returns:
            List[Dict]: 每日统计数据列表
        """
        # 计算日期范围内的每一天
        current_date = start_datetime
        daily_stats = []
        
        while current_date <= end_datetime:
            next_date = current_date + timedelta(days=1)
            
            # 当天的开始和结束时间
            day_start = current_date
            day_end = next_date - timedelta(seconds=1)
            
            # 使用已有的_get_incremental_stats方法获取新增和更新统计
            incremental_stats = self._get_incremental_stats(db, day_start, day_end)
            add_count = incremental_stats["add_count"]
            update_count = incremental_stats["update_count"]
            total_count = add_count + update_count
            
            # 获取当天的成功接入量
            success_count = self._get_success_count(db, day_start, day_end)
            
            # 计算成功率
            success_rate = round(success_count / total_count * 100, 2) if total_count > 0 else 0
            
            # 添加到结果列表
            daily_stats.append({
                "日期": current_date.strftime('%Y-%m-%d'),
                "新增文档数": add_count,
                "更新文档数": update_count,
                "总接入量": total_count,
                "成功接入量": success_count,
                "成功率": success_rate
            })
            
            # 移动到下一天
            current_date = next_date
        
        return daily_stats


    def _get_incremental_stats(self, db: Session, day_start: datetime, day_end: datetime) -> Dict:
        """
        获取指定日期范围内的文件新增和更新消息统计
        
        Args:
            db: 数据库会话
            day_start: 当天开始时间
            day_end: 当天结束时间
            
        Returns:
            Dict: 包含新增和更新操作数量的字典
        """
        # 获取当天所有FILE_ADD消息记录
        file_add_records = db.query(KafkaMessageLog)\
            .filter(
                and_(
                    KafkaMessageLog.message_type == 'FILE_ADD',
                    KafkaMessageLog.created_at.between(day_start, day_end)
                )
            ).all()
        
        # 解析每条消息，提取file_number
        file_numbers = []
        file_number_to_log = {}
        
        for log in file_add_records:
            try:
                # 解析消息内容
                message_content = json.loads(log.message_content) if isinstance(log.message_content, str) else log.message_content
                
                # 提取文件元数据
                file_metadata = message_content.get('fileMetadata', {})
                file_number = file_metadata.get('fileNumber', '')
                
                if file_number:
                    file_numbers.append(file_number)
                    # 记录每个file_number对应的最新消息记录
                    if file_number not in file_number_to_log or log.created_at > file_number_to_log[file_number].created_at:
                        file_number_to_log[file_number] = log
            except Exception as e:
                logger.error(f"解析消息内容失败: {str(e)}, 消息ID: {log.id}")
            
        # 查找这些file_number之前是否已存在于非结构化文档表中
        existing_file_numbers = set()
        if file_numbers:
            existing_docs = db.query(UnstructuredDocument.file_number, 
                                     UnstructuredDocument.created_at,
                                     UnstructuredDocument.updated_at)\
                .filter(UnstructuredDocument.file_number.in_(file_numbers))\
                .all()
            
            for doc in existing_docs:
                # 如果文档的创建时间早于对应Kafka消息的时间，则判定为更新操作
                if doc.file_number in file_number_to_log:
                    kafka_log = file_number_to_log[doc.file_number]
                    if doc.created_at < kafka_log.created_at:
                        existing_file_numbers.add(doc.file_number)
            
        # 统计新增和更新的数量
        add_count = len(set(file_numbers)) - len(existing_file_numbers)
        update_count = len(existing_file_numbers)
        
        return {
            "add_count": add_count,
            "update_count": update_count
        }
    def _get_success_count(self, db: Session, day_start: datetime, day_end: datetime) -> int:
        """
        获取指定日期范围内的成功接入文件数量（不再过滤status=1）
        
        Args:
            db: 数据库会话
            day_start: 当天开始时间
            day_end: 当天结束时间
            
        Returns:
            int: 成功接入文件数量
        """
        # 查询非结构化文档记录表中在指定日期范围内创建的记录数量，不再过滤status
        count = db.query(func.count(UnstructuredDocument.id))\
            .filter(
                UnstructuredDocument.created_at.between(day_start, day_end)
            ).scalar() or 0
            
        return count
    
    async def get_summary_statistics(self, days: int = 30, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        获取数据接入汇总统计
        
        Args:
            days: 统计天数，默认30天
            start_date: 开始日期（可选），格式: YYYY-MM-DD
            end_date: 结束日期（可选），格式: YYYY-MM-DD
            
        Returns:
            Dict: 包含数据接入汇总统计的字典
        """
        # 处理日期参数，确保明确的优先级：start_date/end_date 优先于 days
        if start_date and end_date:
            # 使用明确的日期范围
            logger.info(f"使用明确的日期范围: {start_date} 到 {end_date}")
            start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
            end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        else:
            # 使用天数
            end_datetime = datetime.now()
            start_datetime = end_datetime - timedelta(days=days)
            start_date = start_datetime.strftime('%Y-%m-%d')
            end_date = end_datetime.strftime('%Y-%m-%d')
            logger.info(f"使用最近{days}天数据: {start_date} 到 {end_date}")
        
        # 缓存键
        cache_key = f"summary_stats:{start_date}:{end_date}"
        
        # 尝试从缓存获取
        if self.cache_handler:
            cached_result = await self.cache_handler.get(cache_key)
            if cached_result:
                logger.info(f"从缓存获取汇总统计数据: {start_date} 到 {end_date}")
                return cached_result
                
        logger.info(f"计算从{start_date}到{end_date}的数据接入汇总统计")
        
        # 使用数据库上下文管理器
        with get_db() as db:
            # 优化: 使用单次查询获取系统维度的统计数据
            system_stats = self._get_system_statistics_optimized(db, start_datetime, end_datetime)
            
            # 计算总量
            total_incremental = sum(stat["incremental_count"] for stat in system_stats)
            total_success = sum(stat["success_count"] for stat in system_stats)
            success_rate = round(total_success / total_incremental * 100, 2) if total_incremental > 0 else 0
            
            # 构建结果
            result = {
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date
                },
                "total_incremental": total_incremental,
                "total_success": total_success,
                "success_rate": success_rate,
                "system_statistics": system_stats
            }
            
            # 缓存结果(5分钟)
            if self.cache_handler:
                await self.cache_handler.set(cache_key, result, expiry=300)
            
            return result
            
    def _get_system_statistics_optimized(self, db: Session, start_datetime: datetime, end_datetime: datetime) -> List[Dict]:
        """
        优化版：使用_get_system_statistics方法来正确处理新增和更新统计
        
        Args:
            db: 数据库会话
            start_datetime: 开始日期时间
            end_datetime: 结束日期时间
            
        Returns:
            List[Dict]: 系统维度统计数据列表
        """
        # 直接使用现有的方法
        return self._get_system_statistics(db, start_datetime, end_datetime)
    
    def _get_system_statistics(self, db: Session, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        获取按来源系统分组的统计数据
        
        Args:
            db: 数据库会话
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            List[Dict]: 按系统分组的统计数据
        """
        # 获取所有系统名称
        systems = db.query(KafkaMessageLog.system_name)\
            .filter(
                and_(
                    KafkaMessageLog.message_type == 'FILE_ADD',
                    KafkaMessageLog.created_at.between(start_date, end_date)
                )
            )\
            .group_by(KafkaMessageLog.system_name)\
            .all()
            
        system_stats = []
        
        # 对每个系统进行单独统计
        for system in systems:
            system_name = system[0]
            
            # 获取该系统的详细接入统计
            incremental_stats = self._get_system_incremental_stats(db, system_name, start_date, end_date)
            incremental_count = incremental_stats["add_count"] + incremental_stats["update_count"]
            
            # 获取该系统的成功数据统计
            success_count = db.query(func.count(UnstructuredDocument.id))\
                .filter(
                    and_(
                        UnstructuredDocument.system_name == system_name,
                        UnstructuredDocument.created_at.between(start_date, end_date)
                    )
                ).scalar() or 0
                
            # 计算成功率
            success_rate = round(success_count / incremental_count * 100, 2) if incremental_count > 0 else 0
            
            # 添加到结果
            system_stats.append({
                "system_name": system_name,
                "incremental_count": incremental_count,
                "add_count": incremental_stats["add_count"],
                "update_count": incremental_stats["update_count"],
                "success_count": success_count,
                "success_rate": success_rate
            })
            
        return system_stats
        
    def _get_system_incremental_stats(self, db: Session, system_name: str, start_date: datetime, end_date: datetime) -> Dict:
        """
        获取指定系统在日期范围内的文件新增和更新统计
        
        Args:
            db: 数据库会话
            system_name: 系统名称
            start_date: 开始时间
            end_date: 结束时间
            
        Returns:
            Dict: 包含新增和更新操作数量的字典
        """
        # 获取所有FILE_ADD消息记录
        file_add_records = db.query(KafkaMessageLog)\
            .filter(
                and_(
                    KafkaMessageLog.message_type == 'FILE_ADD',
                    KafkaMessageLog.system_name == system_name,
                    KafkaMessageLog.created_at.between(start_date, end_date)
                )
            ).all()
        
        # 解析每条消息，提取file_number
        file_numbers = []
        file_number_to_log = {}
        
        for log in file_add_records:
            try:
                # 解析消息内容
                message_content = json.loads(log.message_content) if isinstance(log.message_content, str) else log.message_content
                
                # 提取文件元数据
                file_metadata = message_content.get('fileMetadata', {})
                file_number = file_metadata.get('fileNumber', '')
                
                if file_number:
                    file_numbers.append(file_number)
                    # 记录每个file_number对应的最新消息记录
                    if file_number not in file_number_to_log or log.created_at > file_number_to_log[file_number].created_at:
                        file_number_to_log[file_number] = log
            except Exception as e:
                logger.error(f"解析消息内容失败: {str(e)}, 消息ID: {log.id}")
                
        # 查找这些file_number之前是否已存在于非结构化文档表中
        existing_file_numbers = set()
        if file_numbers:
            existing_docs = db.query(UnstructuredDocument.file_number, 
                                     UnstructuredDocument.created_at,
                                     UnstructuredDocument.updated_at)\
                .filter(
                    and_(
                        UnstructuredDocument.system_name == system_name,
                        UnstructuredDocument.file_number.in_(file_numbers)
                    )
                ).all()
            
            for doc in existing_docs:
                # 如果文档的创建时间早于对应Kafka消息的时间，则判定为更新操作
                if doc.file_number in file_number_to_log:
                    kafka_log = file_number_to_log[doc.file_number]
                    if doc.created_at < kafka_log.created_at:
                        existing_file_numbers.add(doc.file_number)
            
        # 统计新增和更新的数量
        add_count = len(set(file_numbers)) - len(existing_file_numbers)
        update_count = len(existing_file_numbers)
        
        return {
            "add_count": add_count,
            "update_count": update_count
        }

    async def get_file_status_by_name(self, file_name: str) -> Dict:
        """
        通过文件名获取特定文件的详细状态信息
        
        Args:
            file_name: 文件名
            
        Returns:
            Dict: 包含文件在Kafka和非结构化文档表中的状态信息
        """
        # 确保服务已初始化
        await self._ensure_initialized()
        
        with get_db() as db:
            # 查询Kafka消息日志，通过文件名查找
            kafka_log = db.query(KafkaMessageLog)\
                .filter(
                    and_(
                        KafkaMessageLog.message_type == 'FILE_ADD',
                        KafkaMessageLog.message_content.like(f'%"fileName": "{file_name}"%')
                    )
                )\
                .order_by(desc(KafkaMessageLog.created_at))\
                .first()
            
            # 如果找不到Kafka记录，则无法继续查询
            if not kafka_log:
                return {
                    "file_name": file_name,
                    "kafka_status": {
                        "exists": False
                    },
                    "document_status": {
                        "exists": False
                    },
                    "message": "未找到相关Kafka消息记录"
                }
                
            try:
                # 解析消息内容，获取file_number和其他信息
                message_content = json.loads(kafka_log.message_content) if isinstance(kafka_log.message_content, str) else kafka_log.message_content
                file_metadata = message_content.get('fileMetadata', {})
                file_number = file_metadata.get('fileNumber', '')
                
                # 使用file_number查询非结构化文档表
                doc = db.query(UnstructuredDocument)\
                    .filter(UnstructuredDocument.file_number == file_number)\
                    .first()
                    
                # 构建结果
                result = {
                    "file_name": file_name,
                    "file_number": file_number,
                    "kafka_status": {
                        "exists": kafka_log is not None
                    },
                    "document_status": {
                        "exists": doc is not None
                    }
                }
                
                # 添加Kafka消息详情
                if kafka_log:
                    result["kafka_status"].update({
                        "id": kafka_log.id,
                        "system_name": kafka_log.system_name,
                        "message_datetime": kafka_log.message_datetime.strftime('%Y-%m-%d %H:%M:%S') if kafka_log.message_datetime else None,
                        "created_at": kafka_log.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                        "file_path": file_metadata.get('filePath', '未知路径')
                    })
                
                # 添加文档详情
                if doc:
                    result["document_status"].update({
                        "id": doc.id,
                        "system_name": doc.system_name,
                        "file_name": doc.file_name,
                        "status": doc.status,  # 0-未成功，1-成功
                        "status_text": "成功" if doc.status == 1 else "未成功",
                        "zhenzhi_file_id": doc.zhenzhi_file_id,
                        "version": doc.version,
                        "created_at": doc.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                        "updated_at": doc.updated_at.strftime('%Y-%m-%d %H:%M:%S')
                    })
                    
                return result
                
            except Exception as e:
                logger.error(f"解析消息内容失败: {str(e)}, 消息ID: {kafka_log.id if kafka_log else 'unknown'}")
                return {
                    "file_name": file_name,
                    "kafka_status": {
                        "exists": kafka_log is not None
                    },
                    "document_status": {
                        "exists": False
                    },
                    "error": str(e)
                }

    async def get_file_permission_statistics(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        获取每日文件权限变更统计（简化版）
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict: 包含每日文件权限变更的简化统计信息
        """
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"获取从{start_date}到{end_date}的每日文件权限变更统计")
        
        start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        
        # 使用数据库上下文管理器
        with get_db() as db:
            # 获取原始统计数据
            daily_stats = self._collect_file_permission_statistics(db, start_datetime, end_datetime)
            
            # 简化每日统计
            simplified_daily_stats = []
            totals = {
                "总Kafka消息数": 0,
                "总同步日志数": 0,
                "总角色变更数": 0,
                "总用户变更数": 0,
                "平均成功率": 0
            }
            
            for day in daily_stats:
                # 提取关键信息
                simplified_day = {
                    "日期": day["date"],
                    "Kafka消息数": day["kafka_message_count"],
                    "同步日志数": day["sync_log_count"],
                    "角色变更": {
                        "添加": day["sync_add_role_count"],
                        "删除": day["sync_del_role_count"],
                        "总计": day["sync_total_role_count"]
                    },
                    "用户变更": {
                        "添加": day["sync_add_user_count"],
                        "删除": day["sync_del_user_count"],
                        "总计": day["sync_total_user_count"]
                    },
                    "成功率": day["success_rate"]
                }
                simplified_daily_stats.append(simplified_day)
                
                # 累计总数
                totals["总Kafka消息数"] += day["kafka_message_count"]
                totals["总同步日志数"] += day["sync_log_count"]
                totals["总角色变更数"] += day["sync_total_role_count"]
                totals["总用户变更数"] += day["sync_total_user_count"]
            
            # 计算平均成功率
            if totals["总Kafka消息数"] > 0:
                totals["平均成功率"] = round(totals["总同步日志数"] / totals["总Kafka消息数"] * 100, 2)
        
        # 构建结果
        result = {
            "日期范围": {
                "start_date": start_date,
                "end_date": end_date
            },
            "每日统计": simplified_daily_stats,
            "汇总统计": totals
        }
        
        return result

    def _collect_file_permission_statistics(self, db: Session, start_datetime: datetime, end_datetime: datetime) -> List[Dict]:
        """
        收集每日文件权限变更统计数据，同时处理Kafka原始消息和文件权限同步表数据
        
        Args:
            db: 数据库会话
            start_datetime: 开始日期时间
            end_datetime: 结束日期时间
            
        Returns:
            List[Dict]: 每日统计数据列表
        """
        # 计算日期范围内的每一天
        current_date = start_datetime
        daily_stats = []
        
        while current_date <= end_datetime:
            next_date = current_date + timedelta(days=1)
            
            # 当天的开始和结束时间
            day_start = current_date
            day_end = next_date - timedelta(seconds=1)
            
            # 获取当天的FILE_NOT_CHANGE消息
            kafka_logs = db.query(KafkaMessageLog)\
                .filter(
                    and_(
                        KafkaMessageLog.message_type == 'FILE_NOT_CHANGE',
                        KafkaMessageLog.created_at.between(day_start, day_end)
                    )
                ).all()
            
            kafka_count = len(kafka_logs)
            
            # 获取当天成功同步的文件变更日志数量
            sync_logs = db.query(KnowledgeRoleSyncLog)\
                .filter(
                    and_(
                        KnowledgeRoleSyncLog.message_type == 'FILE_NOT_CHANGE',
                        KnowledgeRoleSyncLog.created_at.between(day_start, day_end)
                    )
                ).all()
            
            sync_log_count = len(sync_logs)
            
            # 从Kafka消息中提取角色和用户数量
            kafka_add_role_count = 0
            kafka_del_role_count = 0
            kafka_add_user_count = 0
            kafka_del_user_count = 0
            
            for log in kafka_logs:
                try:
                    # 解析消息内容
                    message_content = json.loads(log.message_content) if isinstance(log.message_content, str) else log.message_content
                    
                    # 提取角色和用户列表
                    add_roles = message_content.get('fileAddRoleList', [])
                    del_roles = message_content.get('fileDelRoleList', [])
                    add_users = message_content.get('fileAddUserList', [])
                    del_users = message_content.get('fileDelUserList', [])
                    
                    # 统计数量
                    kafka_add_role_count += len(add_roles)
                    kafka_del_role_count += len(del_roles)
                    kafka_add_user_count += len(add_users)
                    kafka_del_user_count += len(del_users)
                    
                except Exception as e:
                    logger.error(f"解析消息内容失败: {str(e)}, 消息ID: {log.id}")
            
            # 从文件权限同步表中获取角色变更数量统计（在Python中处理JSON数组）
            sync_add_role_count = 0
            sync_del_role_count = 0
            
            # 用于调试的角色列表
            add_role_details = []
            del_role_details = []
            
            for log in sync_logs:
                try:
                    # 解析add_role_list
                    if log.add_role_list:
                        add_role_list = json.loads(log.add_role_list) if isinstance(log.add_role_list, str) else log.add_role_list
                        sync_add_role_count += len(add_role_list)
                        # 保存角色详情用于调试
                        add_role_details.extend(add_role_list)
                    
                    # 解析del_role_list
                    if log.del_role_list:
                        del_role_list = json.loads(log.del_role_list) if isinstance(log.del_role_list, str) else log.del_role_list
                        sync_del_role_count += len(del_role_list)
                        # 保存角色详情用于调试
                        del_role_details.extend(del_role_list)
                except Exception as e:
                    logger.error(f"解析角色列表失败: {str(e)}, 日志ID: {log.id}")
            
            sync_total_role_count = sync_add_role_count + sync_del_role_count
            
            # 从文件权限同步表中获取用户变更数量统计（在Python中处理JSON数组）
            sync_add_user_count = 0
            sync_del_user_count = 0
            
            # 用于调试的用户列表
            add_user_details = []
            del_user_details = []
            
            for log in sync_logs:
                try:
                    # 解析add_user_list
                    if log.add_user_list:
                        add_user_list = json.loads(log.add_user_list) if isinstance(log.add_user_list, str) else log.add_user_list
                        sync_add_user_count += len(add_user_list)
                        # 收集用户详情
                        add_user_details.extend(add_user_list)
                    
                    # 解析del_user_list
                    if log.del_user_list:
                        del_user_list = json.loads(log.del_user_list) if isinstance(log.del_user_list, str) else log.del_user_list
                        sync_del_user_count += len(del_user_list)
                        # 收集用户详情
                        del_user_details.extend(del_user_list)
                except Exception as e:
                    logger.error(f"解析用户列表失败: {str(e)}, 日志ID: {log.id}")
            
            # 记录调试信息
            logger.info(f"日期: {current_date.strftime('%Y-%m-%d')}, 消息类型: FILE_NOT_CHANGE, 同步日志调试信息:")
            logger.info(f"添加角色数量: {sync_add_role_count}, 详情: {add_role_details}")
            logger.info(f"删除角色数量: {sync_del_role_count}, 详情: {del_role_details}")
            logger.info(f"添加用户数量: {sync_add_user_count}, 详情: {add_user_details}")
            logger.info(f"删除用户数量: {sync_del_user_count}, 详情: {del_user_details}")
            
            sync_total_user_count = sync_add_user_count + sync_del_user_count
            
            # 计算Kafka总数
            kafka_total_role_count = kafka_add_role_count + kafka_del_role_count
            kafka_total_user_count = kafka_add_user_count + kafka_del_user_count
            
            # 添加到结果列表
            daily_stats.append({
                "date": current_date.strftime('%Y-%m-%d'),
                "kafka_message_count": kafka_count,
                "sync_log_count": sync_log_count,
                # Kafka消息中的数据
                "kafka_add_role_count": kafka_add_role_count,
                "kafka_del_role_count": kafka_del_role_count,
                "kafka_total_role_count": kafka_total_role_count,
                "kafka_add_user_count": kafka_add_user_count,
                "kafka_del_user_count": kafka_del_user_count,
                "kafka_total_user_count": kafka_total_user_count,
                # 同步表中的数据
                "sync_add_role_count": sync_add_role_count,
                "sync_del_role_count": sync_del_role_count,
                "sync_total_role_count": sync_total_role_count,
                "sync_add_user_count": sync_add_user_count,
                "sync_del_user_count": sync_del_user_count,
                "sync_total_user_count": sync_total_user_count,
                # 成功率
                "success_rate": round(sync_log_count / kafka_count * 100, 2) if kafka_count > 0 else 0
            })
            
            # 移动到下一天
            current_date = next_date
            
        return daily_stats

    async def get_role_user_statistics(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        获取每日角色和用户变更统计（简化版）
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict: 包含角色和用户变更统计的简化版信息
        """
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"获取从{start_date}到{end_date}的每日角色用户变更统计")
        
        start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        
        # 使用数据库上下文管理器
        with get_db() as db:
            # 获取原始统计数据
            daily_stats = self._collect_role_user_statistics(db, start_datetime, end_datetime)
            type_summary = self._get_role_user_type_summary(db, start_datetime, end_datetime)
            
            # 简化每日统计
            simplified_daily_stats = []
            for day in daily_stats:
                simplified_day = {
                    "日期": day["date"],
                    "总同步消息数": day["total_sync_count"],
                    "总Kafka消息数": day["total_kafka_message_count"],
                    "同步角色数": day["total_sync_roles_affected"],
                    "同步用户数": day["total_sync_users_affected"],
                    "成功率": day["total_success_rate"]
                }
                simplified_daily_stats.append(simplified_day)
            
            # 简化类型汇总
            simplified_summary = {
                "总同步消息数": type_summary["total"]["sync_count"],
                "总Kafka消息数": type_summary["total"]["kafka_message_count"],
                "同步角色数": type_summary["total"]["sync_role_count"],
                "同步用户数": type_summary["total"]["sync_user_count"],
                "成功率": type_summary["total"]["success_rate"],
                "类型明细": {
                    "角色添加": {
                        "同步数": type_summary["ADD_ROLE"]["sync_count"],
                        "Kafka数": type_summary["ADD_ROLE"]["kafka_message_count"]
                    },
                    "角色删除": {
                        "同步数": type_summary["DEL_ROLE"]["sync_count"],
                        "Kafka数": type_summary["DEL_ROLE"]["kafka_message_count"]
                    },
                    "角色添加用户": {
                        "同步数": type_summary["ROLE_ADD_USER"]["sync_count"],
                        "Kafka数": type_summary["ROLE_ADD_USER"]["kafka_message_count"],
                        "用户数": type_summary["ROLE_ADD_USER"]["sync_user_count"]
                    },
                    "角色删除用户": {
                        "同步数": type_summary["ROLE_DEL_USER"]["sync_count"],
                        "Kafka数": type_summary["ROLE_DEL_USER"]["kafka_message_count"],
                        "用户数": type_summary["ROLE_DEL_USER"]["sync_user_count"]
                    }
                }
            }
        
        # 构建结果
        result = {
            "日期范围": {
                "start_date": start_date,
                "end_date": end_date
            },
            "每日统计": simplified_daily_stats,
            "汇总统计": simplified_summary
        }
        
        return result

    def _collect_role_user_statistics(self, db: Session, start_datetime: datetime, end_datetime: datetime) -> List[Dict]:
        """
        收集每日角色和用户变更统计数据，同时处理Kafka原始消息和虚拟组同步日志表数据
        
        Args:
            db: 数据库会话
            start_datetime: 开始日期时间
            end_datetime: 结束日期时间
            
        Returns:
            List[Dict]: 每日统计数据列表
        """
        # 计算日期范围内的每一天
        current_date = start_datetime
        daily_stats = []
        
        # 需要统计的消息类型
        message_types = ['ADD_ROLE', 'DEL_ROLE', 'ROLE_ADD_USER', 'ROLE_DEL_USER']
        
        while current_date <= end_datetime:
            next_date = current_date + timedelta(days=1)
            
            # 当天的开始和结束时间
            day_start = current_date
            day_end = next_date - timedelta(seconds=1)
            
            # 统计汇总数据
            day_stats = {
                "date": current_date.strftime('%Y-%m-%d'),
                "total_kafka_message_count": 0,
                "total_sync_count": 0,
                "total_kafka_users_affected": 0,
                "total_kafka_roles_affected": 0,
                "total_sync_users_affected": 0,
                "total_sync_roles_affected": 0,
                "by_message_type": {}
            }
            
            # 统计每种消息类型的详细数据
            for msg_type in message_types:
                # 获取该类型Kafka消息
                kafka_logs = db.query(KafkaMessageLog)\
                    .filter(
                        and_(
                            KafkaMessageLog.message_type == msg_type,
                            KafkaMessageLog.created_at.between(day_start, day_end)
                        )
                    ).all()
                
                kafka_count = len(kafka_logs)
                
                # 获取虚拟组同步日志表中的同步日志数量
                sync_logs = db.query(VirtualGroupSyncLog)\
                    .filter(
                        and_(
                            VirtualGroupSyncLog.message_type == msg_type,
                            VirtualGroupSyncLog.created_at.between(day_start, day_end)
                        )
                    ).all()
                
                sync_count = len(sync_logs)
                
                # 初始化该类型的统计数据
                type_stats = {
                    "kafka_message_count": kafka_count,
                    "sync_count": sync_count,
                    "success_rate": round(sync_count / kafka_count * 100, 2) if kafka_count > 0 else 0,
                    "kafka_user_count": 0,
                    "kafka_role_count": 0,
                    "sync_user_count": 0,
                    "sync_role_count": 0
                }
                
                # 处理Kafka消息内容
                for log in kafka_logs:
                    try:
                        # 解析消息内容
                        message_content = json.loads(log.message_content) if isinstance(log.message_content, str) else log.message_content
                        
                        if msg_type in ['ADD_ROLE', 'DEL_ROLE']:
                            # 这些类型主要影响角色
                            role_id = message_content.get('roleName', '') or message_content.get('role_id', '') or message_content.get('roleId', '')
                            # 调试输出角色ID
                            logger.info(f"消息类型: {msg_type}, 角色ID: {role_id}, 消息ID: {log.id}")
                            if role_id:
                                type_stats["kafka_role_count"] += 1
                                day_stats["total_kafka_roles_affected"] += 1
                        
                        elif msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                            # 这些类型主要影响用户 - 正确计算用户列表中的所有用户
                            user_list = None
                            for field_name in ['userList', 'user_list', 'UserList', 'users']:
                                if field_name in message_content and message_content[field_name]:
                                    user_list = message_content[field_name]
                                    logger.info(f"找到用户列表字段: {field_name}, 类型: {type(user_list)}")
                                    break
                            
                            if user_list and isinstance(user_list, list):
                                user_count = len(user_list)
                                # 调试输出用户列表
                                logger.info(f"消息类型: {msg_type}, 用户列表(list): {user_list}, 消息ID: {log.id}")
                                type_stats["kafka_user_count"] += user_count
                                day_stats["total_kafka_users_affected"] += user_count
                            elif user_list and isinstance(user_list, str):
                                # 尝试解析JSON字符串
                                try:
                                    parsed_user_list = json.loads(user_list)
                                    if isinstance(parsed_user_list, list):
                                        user_count = len(parsed_user_list)
                                        # 调试输出解析后的用户列表
                                        logger.info(f"消息类型: {msg_type}, 用户列表(parsed JSON): {parsed_user_list}, 消息ID: {log.id}")
                                        type_stats["kafka_user_count"] += user_count
                                        day_stats["total_kafka_users_affected"] += user_count
                                except Exception as e:
                                    logger.error(f"解析用户列表字符串失败: {str(e)}, 消息ID: {log.id}, 原始内容: {user_list}")
                    
                    except Exception as e:
                        logger.error(f"解析消息内容失败: {str(e)}, 消息ID: {log.id}")
                
                # 从虚拟组同步日志表中统计角色和用户数量
                if msg_type in ['ADD_ROLE', 'DEL_ROLE']:
                    # 统计角色数量
                    role_count = db.query(func.count(VirtualGroupSyncLog.role_id))\
                        .filter(
                            and_(
                                VirtualGroupSyncLog.message_type == msg_type,
                                VirtualGroupSyncLog.created_at.between(day_start, day_end)
                            )
                        ).scalar() or 0
                    type_stats["sync_role_count"] = role_count
                    
                    # 获取具体角色ID列表用于调试
                    role_records = db.query(VirtualGroupSyncLog.role_id)\
                        .filter(
                            and_(
                                VirtualGroupSyncLog.message_type == msg_type,
                                VirtualGroupSyncLog.created_at.between(day_start, day_end)
                            )
                        ).all()
                    role_ids = [record.role_id for record in role_records]
                    logger.info(f"消息类型: {msg_type}, 角色ID列表: {role_ids}")
                    
                    # 恢复被删除的代码行
                    day_stats["total_sync_roles_affected"] += role_count
                
                elif msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                    # 统计用户数量（在Python中处理JSON数组）
                    add_users_count = 0
                    del_users_count = 0
                    
                    # 用于调试的用户列表
                    add_user_details = []
                    del_user_details = []
                    
                    # 获取所有同步日志记录用于详细调试
                    logs = db.query(VirtualGroupSyncLog)\
                        .filter(
                            and_(
                                VirtualGroupSyncLog.message_type == msg_type,
                                VirtualGroupSyncLog.created_at.between(day_start, day_end)
                            )
                        ).all()
                    
                    logger.info(f"消息类型: {msg_type}, 获取到同步日志数量: {len(logs)}")
                    
                    for log in logs:
                        try:
                            # 记录当前日志相关信息
                            logger.info(f"同步日志ID: {log.id}, 角色ID: {log.role_id}")
                            
                            # 解析add_user_list
                            if log.add_user_list:
                                logger.info(f"add_user_list类型: {type(log.add_user_list)}, 值: {log.add_user_list}")
                                if isinstance(log.add_user_list, str):
                                    add_user_list = json.loads(log.add_user_list)
                                    logger.info(f"解析后的add_user_list: {add_user_list}")
                                else:
                                    add_user_list = log.add_user_list
                                    logger.info(f"原始add_user_list: {add_user_list}")
                                    
                                if isinstance(add_user_list, list):
                                    add_users_count += len(add_user_list)
                                    add_user_details.extend(add_user_list)
                                    logger.info(f"添加用户列表长度: {len(add_user_list)}, 当前总数: {add_users_count}")
                                else:
                                    logger.warning(f"add_user_list不是列表类型: {type(add_user_list)}")
                            
                            # 解析del_user_list
                            if log.del_user_list:
                                logger.info(f"del_user_list类型: {type(log.del_user_list)}, 值: {log.del_user_list}")
                                if isinstance(log.del_user_list, str):
                                    del_user_list = json.loads(log.del_user_list)
                                    logger.info(f"解析后的del_user_list: {del_user_list}")
                                else:
                                    del_user_list = log.del_user_list
                                    logger.info(f"原始del_user_list: {del_user_list}")
                                    
                                if isinstance(del_user_list, list):
                                    del_users_count += len(del_user_list)
                                    del_user_details.extend(del_user_list)
                                    logger.info(f"删除用户列表长度: {len(del_user_list)}, 当前总数: {del_users_count}")
                                else:
                                    logger.warning(f"del_user_list不是列表类型: {type(del_user_list)}")
                        except Exception as e:
                            logger.error(f"解析用户列表失败: {str(e)}, 日志ID: {log.id}")
                    
                    sync_user_count = add_users_count + del_users_count
                    
                    logger.info(f"汇总 - 消息类型: {msg_type}, 添加用户总数: {add_users_count}, 删除用户总数: {del_users_count}")
                    logger.info(f"用户详情 - 添加用户: {add_user_details}")
                    logger.info(f"用户详情 - 删除用户: {del_user_details}")
                    
                    type_stats["sync_user_count"] = sync_user_count
                    type_stats["sync_add_user_count"] = add_users_count
                    type_stats["sync_del_user_count"] = del_users_count
                    
                    # 恢复被删除的代码行
                    day_stats["total_sync_users_affected"] += sync_user_count
                
                # 更新统计数据
                day_stats["by_message_type"][msg_type] = type_stats
                day_stats["total_kafka_message_count"] += kafka_count
                day_stats["total_sync_count"] += sync_count
            
            # 计算总成功率
            day_stats["total_success_rate"] = round(
                day_stats["total_sync_count"] / day_stats["total_kafka_message_count"] * 100, 2
            ) if day_stats["total_kafka_message_count"] > 0 else 0
            
            # 添加到结果列表
            daily_stats.append(day_stats)
            
            # 移动到下一天
            current_date = next_date
        
        return daily_stats

    def _get_role_user_type_summary(self, db: Session, start_datetime: datetime, end_datetime: datetime) -> Dict:
        """
        获取按消息类型汇总的角色和用户变更统计，同时处理Kafka原始消息和虚拟组同步日志表数据
        
        Args:
            db: 数据库会话
            start_datetime: 开始日期时间
            end_datetime: 结束日期时间
            
        Returns:
            Dict: 按消息类型汇总的统计数据
        """
        # 需要统计的消息类型
        message_types = ['ADD_ROLE', 'DEL_ROLE', 'ROLE_ADD_USER', 'ROLE_DEL_USER']
        summary = {}
        
        for msg_type in message_types:
            # 获取该类型的Kafka消息
            kafka_logs = db.query(KafkaMessageLog)\
                .filter(
                    and_(
                        KafkaMessageLog.message_type == msg_type,
                        KafkaMessageLog.created_at.between(start_datetime, end_datetime)
                    )
                ).all()
            
            kafka_count = len(kafka_logs)
            
            # 获取虚拟组同步日志表中的同步记录数量
            sync_logs = db.query(VirtualGroupSyncLog)\
                .filter(
                    and_(
                        VirtualGroupSyncLog.message_type == msg_type,
                        VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
                    )
                ).all()
            
            sync_count = len(sync_logs)
            
            # 初始化该类型的统计数据
            type_stats = {
                "kafka_message_count": kafka_count,
                "sync_count": sync_count,
                "success_rate": round(sync_count / kafka_count * 100, 2) if kafka_count > 0 else 0,
                "kafka_user_count": 0,
                "kafka_role_count": 0,
                "sync_user_count": 0,
                "sync_role_count": 0
            }
            
            # 处理Kafka消息内容
            for log in kafka_logs:
                try:
                    # 解析消息内容
                    message_content = json.loads(log.message_content) if isinstance(log.message_content, str) else log.message_content
                    
                    if msg_type in ['ADD_ROLE', 'DEL_ROLE']:
                        # 这些类型主要影响角色
                        role_id = message_content.get('roleId', '') or message_content.get('role_id', '')
                        if role_id:
                            type_stats["kafka_role_count"] += 1
                    
                    elif msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                        # 这些类型主要影响用户 - 正确计算用户列表中的所有用户数量
                        # 检查所有可能的用户列表字段名
                        user_list = None
                        for field_name in ['userList', 'user_list', 'UserList', 'users']:
                            if field_name in message_content and message_content[field_name]:
                                user_list = message_content[field_name]
                                break
                        
                        if user_list and isinstance(user_list, list):
                            type_stats["kafka_user_count"] += len(user_list)
                        elif user_list and isinstance(user_list, str):
                            # 尝试解析JSON字符串
                            try:
                                parsed_user_list = json.loads(user_list)
                                if isinstance(parsed_user_list, list):
                                    type_stats["kafka_user_count"] += len(parsed_user_list)
                            except Exception as e:
                                logger.error(f"解析用户列表字符串失败: {str(e)}, 消息ID: {log.id}")
                
                except Exception as e:
                    logger.error(f"解析消息内容失败: {str(e)}, 消息ID: {log.id}")
            
            # 从虚拟组同步日志表中统计角色和用户数量
            if msg_type in ['ADD_ROLE', 'DEL_ROLE']:
                # 统计角色数量
                role_count = db.query(func.count(VirtualGroupSyncLog.role_id))\
                    .filter(
                        and_(
                            VirtualGroupSyncLog.message_type == msg_type,
                            VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
                        )
                    ).scalar() or 0
                type_stats["sync_role_count"] = role_count
                
                # 获取具体角色ID列表用于调试
                role_records = db.query(VirtualGroupSyncLog.role_id)\
                    .filter(
                        and_(
                            VirtualGroupSyncLog.message_type == msg_type,
                            VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
                        )
                    ).all()
                role_ids = [record.role_id for record in role_records]
                logger.info(f"消息类型: {msg_type}, 角色ID列表: {role_ids}")
            
            elif msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                # 统计用户数量（在Python中处理JSON数组）
                add_users_count = 0
                del_users_count = 0
                
                for log in sync_logs:
                    try:
                        # 解析add_user_list
                        if log.add_user_list:
                            if isinstance(log.add_user_list, str):
                                add_user_list = json.loads(log.add_user_list)
                            else:
                                add_user_list = log.add_user_list
                                
                            if isinstance(add_user_list, list):
                                add_users_count += len(add_user_list)
                        
                        # 解析del_user_list
                        if log.del_user_list:
                            if isinstance(log.del_user_list, str):
                                del_user_list = json.loads(log.del_user_list)
                            else:
                                del_user_list = log.del_user_list
                                
                            if isinstance(del_user_list, list):
                                del_users_count += len(del_user_list)
                    except Exception as e:
                        logger.error(f"解析用户列表失败: {str(e)}, 日志ID: {log.id}")
                
                sync_user_count = add_users_count + del_users_count
                
                type_stats["sync_user_count"] = sync_user_count
                type_stats["sync_add_user_count"] = add_users_count
                type_stats["sync_del_user_count"] = del_users_count
            
            # 更新摘要
            summary[msg_type] = type_stats
        
        # 计算总量
        total_kafka_count = sum(s["kafka_message_count"] for s in summary.values())
        total_sync_count = sum(s["sync_count"] for s in summary.values())
        
        summary["total"] = {
            "kafka_message_count": total_kafka_count,
            "sync_count": total_sync_count,
            "success_rate": round(total_sync_count / total_kafka_count * 100, 2) if total_kafka_count > 0 else 0,
            "kafka_user_count": sum(s["kafka_user_count"] for s in summary.values()),
            "kafka_role_count": sum(s["kafka_role_count"] for s in summary.values()),
            "sync_user_count": sum(s["sync_user_count"] for s in summary.values()),
            "sync_role_count": sum(s["sync_role_count"] for s in summary.values())
        }
        
        return summary

    async def get_comprehensive_statistics(self, start_date: Optional[str] = None, end_date: Optional[str] = None, 
                                         days: int = 7, include_details: bool = False) -> Dict:
        """
        获取综合性的统计数据，包括文档接入、角色用户变更、文件权限变更等
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            days: 汇总统计天数，默认为7天
            include_details: 是否包含详细的角色和用户列表，默认为False
            
        Returns:
            Dict: 包含所有统计信息的综合性数据
        """
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"获取从{start_date}到{end_date}的综合性统计数据")
        
        # 整合所有统计数据
        result = {
            "日期范围": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
        # 1. 获取每日数据接入统计
        daily_stats = await self.get_daily_statistics(start_date, end_date)
        result["文档接入统计"] = {
            "每日统计": daily_stats["daily_statistics"],
            "总接入量": sum(day["总接入量"] for day in daily_stats["daily_statistics"]),
            "总成功量": sum(day["成功接入量"] for day in daily_stats["daily_statistics"]),
            "平均成功率": round(sum(day["成功率"] for day in daily_stats["daily_statistics"]) / len(daily_stats["daily_statistics"]), 2) if daily_stats["daily_statistics"] else 0
        }
        
        # 2. 获取数据接入汇总统计 - 传递相同的日期范围参数
        summary_stats = await self.get_summary_statistics(days, start_date, end_date)
        result["接入系统统计"] = {
            "总接入量": summary_stats["total_incremental"],
            "总成功量": summary_stats["total_success"],
            "总成功率": summary_stats["success_rate"],
            "系统明细": summary_stats["system_statistics"]
        }
        
        # 3. 获取文件权限变更统计
        permission_stats = await self.get_file_permission_statistics(start_date, end_date)
        result["文件权限变更统计"] = {
            "每日统计": permission_stats["每日统计"],
            "汇总统计": permission_stats["汇总统计"]
        }
        
        # 4. 获取角色和用户变更统计
        role_user_stats = await self.get_role_user_statistics(start_date, end_date)
        result["角色用户变更统计"] = {
            "每日统计": role_user_stats["每日统计"],
            "汇总统计": role_user_stats["汇总统计"]
        }
        
        # 5. 添加角色用户统计信息，根据include_details控制是否包含详细列表
        with get_db() as db:
            start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
            end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
            
            # 获取虚拟组同步日志表中的角色用户变更详情 
            role_user_details = self._get_role_user_details(db, start_datetime, end_datetime, only_counts=not include_details)
            result["角色用户详情"] = role_user_details
            
            # 获取文件权限同步表中的角色用户变更详情
            file_permission_details = self._get_file_permission_details(db, start_datetime, end_datetime, only_counts=not include_details)
            result["文件权限详情"] = file_permission_details
        
        return result

    def _get_role_user_details(self, db: Session, start_datetime: datetime, end_datetime: datetime, only_counts: bool = False) -> Dict:
        """
        获取角色用户变更的详细信息，用于调试
        
        Args:
            db: 数据库会话
            start_datetime: 开始日期时间
            end_datetime: 结束日期时间
            only_counts: 是否只返回计数信息，默认为False
            
        Returns:
            Dict: 详细的角色用户变更信息
        """
        # 需要统计的消息类型
        message_types = ['ADD_ROLE', 'DEL_ROLE', 'ROLE_ADD_USER', 'ROLE_DEL_USER']
        details = {}
        
        for msg_type in message_types:
            # 获取虚拟组同步日志表中的同步记录
            sync_logs = db.query(VirtualGroupSyncLog)\
                .filter(
                    and_(
                        VirtualGroupSyncLog.message_type == msg_type,
                        VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
                    )
                ).all()
            
            # 初始化该类型的详细数据
            type_details = {
                "同步记录数": len(sync_logs),
            }
            
            # 只有在需要详细列表时才添加
            if not only_counts:
                if msg_type in ['ADD_ROLE', 'DEL_ROLE']:
                    type_details["角色列表"] = [] if msg_type in ['ADD_ROLE', 'DEL_ROLE'] else None
                elif msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                    type_details["用户列表"] = {
                        "添加": [],
                        "删除": []
                    }
            
            # 处理同步记录
            role_count = 0
            add_user_count = 0
            del_user_count = 0
            
            for log in sync_logs:
                if msg_type in ['ADD_ROLE', 'DEL_ROLE'] and log.role_id:
                    role_count += 1
                    if not only_counts:
                        type_details["角色列表"].append(log.role_id)
                
                elif msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                    # 解析add_user_list
                    if log.add_user_list:
                        try:
                            add_user_list = json.loads(log.add_user_list) if isinstance(log.add_user_list, str) else log.add_user_list
                            if isinstance(add_user_list, list):
                                add_user_count += len(add_user_list)
                                if not only_counts:
                                    type_details["用户列表"]["添加"].extend(add_user_list)
                        except Exception as e:
                            logger.error(f"解析add_user_list失败: {str(e)}, 日志ID: {log.id}")
                    
                    # 解析del_user_list
                    if log.del_user_list:
                        try:
                            del_user_list = json.loads(log.del_user_list) if isinstance(log.del_user_list, str) else log.del_user_list
                            if isinstance(del_user_list, list):
                                del_user_count += len(del_user_list)
                                if not only_counts:
                                    type_details["用户列表"]["删除"].extend(del_user_list)
                        except Exception as e:
                            logger.error(f"解析del_user_list失败: {str(e)}, 日志ID: {log.id}")
            
            # 计算统计数据
            if msg_type in ['ADD_ROLE', 'DEL_ROLE']:
                type_details["角色数量"] = role_count
            
            elif msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                type_details["用户数量"] = {
                    "添加": add_user_count,
                    "删除": del_user_count,
                    "总计": add_user_count + del_user_count
                }
            
            details[msg_type] = type_details
        
        return details
    
    def _get_file_permission_details(self, db: Session, start_datetime: datetime, end_datetime: datetime, only_counts: bool = False) -> Dict:
        """
        获取文件权限变更的详细信息，用于调试
        
        Args:
            db: 数据库会话
            start_datetime: 开始日期时间
            end_datetime: 结束日期时间
            only_counts: 是否只返回计数信息，默认为False
            
        Returns:
            Dict: 详细的文件权限变更信息
        """
        # 获取文件权限同步表中的同步记录
        sync_logs = db.query(KnowledgeRoleSyncLog)\
            .filter(
                and_(
                    KnowledgeRoleSyncLog.message_type == 'FILE_NOT_CHANGE',
                    KnowledgeRoleSyncLog.created_at.between(start_datetime, end_datetime)
                )
            ).all()
        
        # 初始化详细数据
        details = {
            "同步记录数": len(sync_logs)
        }
        
        # 只在需要详细信息时添加列表
        if not only_counts:
            details["角色列表"] = {
                "添加": [],
                "删除": []
            }
            details["用户列表"] = {
                "添加": [],
                "删除": []
            }
        
        # 处理同步记录
        add_role_count = 0
        del_role_count = 0
        add_user_count = 0
        del_user_count = 0
        
        for log in sync_logs:
            # 解析add_role_list
            if log.add_role_list:
                try:
                    add_role_list = json.loads(log.add_role_list) if isinstance(log.add_role_list, str) else log.add_role_list
                    if isinstance(add_role_list, list):
                        add_role_count += len(add_role_list)
                        if not only_counts:
                            details["角色列表"]["添加"].extend(add_role_list)
                except Exception as e:
                    logger.error(f"解析add_role_list失败: {str(e)}, 日志ID: {log.id}")
            
            # 解析del_role_list
            if log.del_role_list:
                try:
                    del_role_list = json.loads(log.del_role_list) if isinstance(log.del_role_list, str) else log.del_role_list
                    if isinstance(del_role_list, list):
                        del_role_count += len(del_role_list)
                        if not only_counts:
                            details["角色列表"]["删除"].extend(del_role_list)
                except Exception as e:
                    logger.error(f"解析del_role_list失败: {str(e)}, 日志ID: {log.id}")
            
            # 解析add_user_list
            if log.add_user_list:
                try:
                    add_user_list = json.loads(log.add_user_list) if isinstance(log.add_user_list, str) else log.add_user_list
                    if isinstance(add_user_list, list):
                        add_user_count += len(add_user_list)
                        if not only_counts:
                            details["用户列表"]["添加"].extend(add_user_list)
                except Exception as e:
                    logger.error(f"解析add_user_list失败: {str(e)}, 日志ID: {log.id}")
            
            # 解析del_user_list
            if log.del_user_list:
                try:
                    del_user_list = json.loads(log.del_user_list) if isinstance(log.del_user_list, str) else log.del_user_list
                    if isinstance(del_user_list, list):
                        del_user_count += len(del_user_list)
                        if not only_counts:
                            details["用户列表"]["删除"].extend(del_user_list)
                except Exception as e:
                    logger.error(f"解析del_user_list失败: {str(e)}, 日志ID: {log.id}")
        
        # 计算统计数据
        details["角色数量"] = {
            "添加": add_role_count,
            "删除": del_role_count,
            "总计": add_role_count + del_role_count
        }
        
        details["用户数量"] = {
            "添加": add_user_count,
            "删除": del_user_count,
            "总计": add_user_count + del_user_count
        }
        
        return details

    async def get_system_verification_statistics(self, mode: Optional[str] = None, system_name: Optional[str] = None, 
                                                start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        获取系统验证成功数据统计，仿照get_system_daily_statistics的实现
        
        Args:
            mode: 统计模式 - 'daily'(每日统计), 'system'(系统统计), 'summary'(汇总统计), 默认为'summary'
            system_name: 系统名称，可选
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict: 包含验证成功统计数据的字典
        """
        await self._ensure_initialized()
        
        # 设置默认日期范围和模式
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not mode:
            mode = 'summary'
            
        # 缓存键
        cache_key = self.cache_handler.generate_key("verification_stats", mode, system_name or "all", start_date, end_date)
        
        # 尝试从缓存获取
        if self.cache_handler:
            cached_result = await self.cache_handler.get(cache_key)
            if cached_result:
                logger.info(f"从缓存获取验证统计数据: {mode} 模式")
                return cached_result
        
        logger.info(f"获取验证成功统计数据 - 模式: {mode}, 系统: {system_name or '全部'}, 时间范围: {start_date} 到 {end_date}")
        
        try:
            # 根据模式分发统计逻辑
            if mode == 'daily':
                result = await self._get_daily_verification_statistics(system_name, start_date, end_date)
            elif mode == 'system':
                result = await self._get_system_verification_statistics(start_date, end_date)
            else:  # summary
                result = await self._get_summary_verification_statistics(system_name, start_date, end_date)
            
            # 缓存结果(5分钟)
            if self.cache_handler:
                await self.cache_handler.set(cache_key, result, expiry=300)
            
            return result
            
        except Exception as e:
            logger.error(f"获取验证统计数据失败: {str(e)}")
            raise
    
    async def _get_daily_verification_statistics(self, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取每日验证统计数据"""
        result = {
            "统计模式": "每日统计",
            "系统名称": system_name or "全部系统",
            "日期范围": {"start_date": start_date, "end_date": end_date},
            "每日数据": []
        }
        
        # 生成日期列表
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_datetime
        
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 从Redis获取当日各类型验证数据
            daily_data = {
                "日期": date_str,
                "知识文档验证": await self._get_redis_verification_data("knowledge", system_name, date_str),
                "文件权限验证": await self._get_redis_verification_data("permission", system_name, date_str),
                "角色用户验证": await self._get_redis_role_user_verification_data(system_name, date_str)
            }
            
            result["每日数据"].append(daily_data)
            current_date += timedelta(days=1)
        
        return result
    
    async def _get_system_verification_statistics(self, start_date: str, end_date: str) -> Dict:
        """获取系统维度验证统计数据"""
        result = {
            "统计模式": "系统统计",
            "日期范围": {"start_date": start_date, "end_date": end_date},
            "系统数据": []
        }
        
        # 获取所有系统名称（从数据库或Redis中获取）
        systems = await self._get_all_systems()
        
        for system in systems:
            # 汇总该系统在日期范围内的验证数据
            system_data = {
                "系统名称": system,
                "知识文档验证": await self._get_redis_verification_data_range("knowledge", system, start_date, end_date),
                "文件权限验证": await self._get_redis_verification_data_range("permission", system, start_date, end_date),
                "角色用户验证": await self._get_redis_role_user_verification_data_range(system, start_date, end_date)
            }
            result["系统数据"].append(system_data)
        
        return result
    
    async def _get_summary_verification_statistics(self, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取汇总验证统计数据"""
        result = {
            "统计模式": "汇总统计",
            "系统名称": system_name or "全部系统",
            "日期范围": {"start_date": start_date, "end_date": end_date},
            "汇总数据": {}
        }
        
        # 汇总指定系统或全部系统在日期范围内的验证数据
        result["汇总数据"] = {
            "知识文档验证": await self._get_redis_verification_data_range("knowledge", system_name, start_date, end_date),
            "文件权限验证": await self._get_redis_verification_data_range("permission", system_name, start_date, end_date),
            "角色用户验证": await self._get_redis_role_user_verification_data_range(system_name, start_date, end_date)
        }
        
        return result
    
    async def _get_redis_data_generic(self, 
                                      data_type: str, 
                                      system_name: Optional[str], 
                                      date: str,
                                      operation_type: str,
                                      count_field: str,
                                      success_field: str,
                                      count_label: str,
                                      success_label: str,
                                      fallback_key_pattern: Optional[str] = None) -> Dict:
        """通用Redis数据获取方法"""
        if not self.cache_handler:
            return {count_label: 0, success_label: 0, "成功率": 0}
        
        try:
            # 构建新的Redis键格式：{system}:{service}:{operation}:{type}:{date}
            if system_name:
                redis_key = f"{system_name}:{data_type}:{operation_type}:{date}"
            else:
                # 如果没有指定系统，使用回退格式
                redis_key = fallback_key_pattern or f"{data_type}:{operation_type}:count"
            
            # 从Redis Hash获取统计数据
            count_value = await self.cache_handler.get_hash_field(redis_key, count_field) or 0
            success_value = await self.cache_handler.get_hash_field(redis_key, success_field) or 0
            
            # 计算成功率
            success_rate = round(success_value / count_value * 100, 2) if count_value > 0 else 0
            
            return {
                count_label: int(count_value),
                success_label: int(success_value),
                "成功率": success_rate
            }
        except Exception as e:
            logger.error(f"获取Redis数据失败: {data_type}, {system_name}, {date}, {operation_type}, 错误: {str(e)}")
            return {count_label: 0, success_label: 0, "成功率": 0}

    async def _get_redis_verification_data(self, data_type: str, system_name: Optional[str], date: str) -> Dict:
        """从Redis获取指定类型和日期的验证数据"""
        return await self._get_redis_data_generic(
            data_type=data_type,
            system_name=system_name,
            date=date,
            operation_type="verification:passed",
            count_field="message_count",
            success_field="success_count",
            count_label="报文数",
            success_label="验证通过数",
            fallback_key_pattern=f"{data_type}:verification:completed:count"
        )
    
    async def _get_redis_verification_data_range(self, data_type: str, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取日期范围内的验证数据汇总"""
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_datetime
        
        total_message_count = 0
        total_success_count = 0
        
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            daily_data = await self._get_redis_verification_data(data_type, system_name, date_str)
            total_message_count += daily_data["报文数"]
            total_success_count += daily_data["验证通过数"]
            current_date += timedelta(days=1)
        
        success_rate = round(total_success_count / total_message_count * 100, 2) if total_message_count > 0 else 0
        
        return {
            "报文数": total_message_count,
            "验证通过数": total_success_count,
            "成功率": success_rate
        }
    
    async def _get_redis_multi_operation_data(self, 
                                           data_type: str,
                                           system_name: Optional[str], 
                                           date: str,
                                           operations: list,
                                           operation_type: str,
                                           fields: dict,
                                           result_prefix: str = "") -> Dict:
        """通用多操作Redis数据获取方法"""
        if not self.cache_handler:
            default_result = {}
            for operation in operations:
                operation_key = f"{result_prefix}{operation.upper()}" if result_prefix else operation.upper()
                default_result[operation_key] = {label: 0 for label in fields.values()}
            return default_result
        
        try:
            result = {}
            for operation in operations:
                # 构建Redis键格式
                if system_name:
                    redis_key = f"{system_name}:{data_type}:{operation}:{operation_type}:{date}"
                else:
                    redis_key = f"{data_type}:{operation}:{operation_type}"
                
                operation_data = {}
                for field_name, field_label in fields.items():
                    field_value = await self.cache_handler.get_hash_field(redis_key, field_name) or 0
                    operation_data[field_label] = int(field_value)
                
                operation_key = f"{result_prefix}{operation.upper()}" if result_prefix else operation.upper()
                result[operation_key] = operation_data
            
            return result
        except Exception as e:
            logger.error(f"获取Redis多操作数据失败: {data_type}, {system_name}, {date}, 错误: {str(e)}")
            default_result = {}
            for operation in operations:
                operation_key = f"{result_prefix}{operation.upper()}" if result_prefix else operation.upper()
                default_result[operation_key] = {label: 0 for label in fields.values()}
            return default_result

    async def _get_redis_role_user_verification_data(self, system_name: Optional[str], date: str) -> Dict:
        """获取角色用户验证数据"""
        return await self._get_redis_multi_operation_data(
            data_type="role_user",
            system_name=system_name,
            date=date,
            operations=["add_user", "del_user"],
            operation_type="verification:passed",
            fields={"message_count": "报文数", "user_count": "用户数"},
            result_prefix="ROLE_"
        )
    
    async def _get_redis_role_user_verification_data_range(self, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取日期范围内的角色用户验证数据汇总"""
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_datetime
        
        totals = {"ROLE_ADD_USER": {"报文数": 0, "用户数": 0}, "ROLE_DEL_USER": {"报文数": 0, "用户数": 0}}
        
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            daily_data = await self._get_redis_role_user_verification_data(system_name, date_str)
            
            for operation in ["ROLE_ADD_USER", "ROLE_DEL_USER"]:
                totals[operation]["报文数"] += daily_data[operation]["报文数"]
                totals[operation]["用户数"] += daily_data[operation]["用户数"]
            
            current_date += timedelta(days=1)
        
        return totals
    
    async def _get_all_systems(self) -> List[str]:
        """获取所有系统名称"""
        # 这里应该从数据库或其他数据源获取系统列表
        # 为了演示，返回一些常见的系统名称
        with get_db() as db:
            try:
                # 从KafkaMessageLog表获取所有系统名称
                systems = db.query(KafkaMessageLog.system_name)\
                    .group_by(KafkaMessageLog.system_name)\
                    .all()
                return [system[0] for system in systems if system[0]]
            except Exception as e:
                logger.error(f"获取系统列表失败: {str(e)}")
                return ["WPROS", "WPROS_TEST", "WPROS_STRUCT", "SIS", "SMS"]

    async def get_system_api_call_statistics(self, mode: Optional[str] = None, system_name: Optional[str] = None, 
                                           start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """
        获取系统接口调用成功数据统计，仿照get_system_daily_statistics的实现
        
        Args:
            mode: 统计模式 - 'daily'(每日统计), 'system'(系统统计), 'summary'(汇总统计), 默认为'summary'
            system_name: 系统名称，可选
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict: 包含接口调用成功统计数据的字典
        """
        await self._ensure_initialized()
        
        # 设置默认日期范围和模式
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not mode:
            mode = 'summary'
            
        # 缓存键
        cache_key = self.cache_handler.generate_key("api_call_stats", mode, system_name or "all", start_date, end_date)
        
        # 尝试从缓存获取
        if self.cache_handler:
            cached_result = await self.cache_handler.get(cache_key)
            if cached_result:
                logger.info(f"从缓存获取接口调用统计数据: {mode} 模式")
                return cached_result
        
        logger.info(f"获取接口调用成功统计数据 - 模式: {mode}, 系统: {system_name or '全部'}, 时间范围: {start_date} 到 {end_date}")
        
        try:
            # 根据模式分发统计逻辑
            if mode == 'daily':
                result = await self._get_daily_api_call_statistics(system_name, start_date, end_date)
            elif mode == 'system':
                result = await self._get_system_api_call_statistics(start_date, end_date)
            else:  # summary
                result = await self._get_summary_api_call_statistics(system_name, start_date, end_date)
            
            # 缓存结果(5分钟)
            if self.cache_handler:
                await self.cache_handler.set(cache_key, result, expiry=300)
            
            return result
            
        except Exception as e:
            logger.error(f"获取接口调用统计数据失败: {str(e)}")
            raise
    
    async def _get_daily_api_call_statistics(self, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取每日接口调用统计数据"""
        result = {
            "统计模式": "每日统计",
            "系统名称": system_name or "全部系统",
            "日期范围": {"start_date": start_date, "end_date": end_date},
            "每日数据": []
        }
        
        # 生成日期列表
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_datetime
        
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 从Redis获取当日各类型接口调用数据
            daily_data = {
                "日期": date_str,
                "知识文档接口": await self._get_redis_api_call_data("knowledge", system_name, date_str),
                "文件权限接口": await self._get_redis_api_call_data("permission", system_name, date_str),
                "角色用户接口": await self._get_redis_role_user_api_call_data(system_name, date_str)
            }
            
            result["每日数据"].append(daily_data)
            current_date += timedelta(days=1)
        
        return result
    
    async def _get_system_api_call_statistics(self, start_date: str, end_date: str) -> Dict:
        """获取系统维度接口调用统计数据"""
        result = {
            "统计模式": "系统统计",
            "日期范围": {"start_date": start_date, "end_date": end_date},
            "系统数据": []
        }
        
        # 获取所有系统名称
        systems = await self._get_all_systems()
        
        for system in systems:
            # 汇总该系统在日期范围内的接口调用数据
            system_data = {
                "系统名称": system,
                "知识文档接口": await self._get_redis_api_call_data_range("knowledge", system, start_date, end_date),
                "文件权限接口": await self._get_redis_api_call_data_range("permission", system, start_date, end_date),
                "角色用户接口": await self._get_redis_role_user_api_call_data_range(system, start_date, end_date)
            }
            result["系统数据"].append(system_data)
        
        return result
    
    async def _get_summary_api_call_statistics(self, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取汇总接口调用统计数据"""
        result = {
            "统计模式": "汇总统计",
            "系统名称": system_name or "全部系统",
            "日期范围": {"start_date": start_date, "end_date": end_date},
            "汇总数据": {}
        }
        
        # 汇总指定系统或全部系统在日期范围内的接口调用数据
        result["汇总数据"] = {
            "知识文档接口": await self._get_redis_api_call_data_range("knowledge", system_name, start_date, end_date),
            "文件权限接口": await self._get_redis_api_call_data_range("permission", system_name, start_date, end_date),
            "角色用户接口": await self._get_redis_role_user_api_call_data_range(system_name, start_date, end_date)
        }
        
        return result
    
    async def _get_redis_api_call_data(self, data_type: str, system_name: Optional[str], date: str) -> Dict:
        """从Redis获取指定类型和日期的接口调用数据"""
        return await self._get_redis_data_generic(
            data_type=data_type,
            system_name=system_name,
            date=date,
            operation_type="operation:success",
            count_field="call_count",
            success_field="success_count",
            count_label="调用次数",
            success_label="成功次数",
            fallback_key_pattern=f"{data_type}:permission:set:success:count"
        )
    
    async def _get_redis_api_call_data_range(self, data_type: str, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取日期范围内的接口调用数据汇总"""
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_datetime
        
        total_call_count = 0
        total_success_count = 0
        
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            daily_data = await self._get_redis_api_call_data(data_type, system_name, date_str)
            total_call_count += daily_data["调用次数"]
            total_success_count += daily_data["成功次数"]
            current_date += timedelta(days=1)
        
        success_rate = round(total_success_count / total_call_count * 100, 2) if total_call_count > 0 else 0
        
        return {
            "调用次数": total_call_count,
            "成功次数": total_success_count,
            "成功率": success_rate
        }
    
    async def _get_redis_role_user_api_call_data(self, system_name: Optional[str], date: str) -> Dict:
        """获取角色用户接口调用数据"""
        if not self.cache_handler:
            return {"ROLE_ADD_USER": {"调用次数": 0, "用户数": 0}, "ROLE_DEL_USER": {"调用次数": 0, "用户数": 0}}
        
        try:
            result = {}
            for operation in ["add_user", "del_user"]:
                # 构建新的Redis键格式
                if system_name:
                    api_call_key = f"{system_name}:role_user:{operation}:operation:success:{date}"
                else:
                    api_call_key = f"role_user:{operation}:operation:success"
                
                call_count = await self.cache_handler.get_hash_field(api_call_key, "message_count") or 0
                user_count = await self.cache_handler.get_hash_field(api_call_key, "user_count") or 0
                
                result[f"ROLE_{operation.upper()}"] = {
                    "调用次数": int(call_count),
                    "用户数": int(user_count)
                }
            
            return result
        except Exception as e:
            logger.error(f"获取Redis角色用户接口调用数据失败: {system_name}, {date}, 错误: {str(e)}")
            return {"ROLE_ADD_USER": {"调用次数": 0, "用户数": 0}, "ROLE_DEL_USER": {"调用次数": 0, "用户数": 0}}
    
    async def _get_redis_role_user_api_call_data_range(self, system_name: Optional[str], start_date: str, end_date: str) -> Dict:
        """获取日期范围内的角色用户接口调用数据汇总"""
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start_datetime
        
        totals = {"ROLE_ADD_USER": {"调用次数": 0, "用户数": 0}, "ROLE_DEL_USER": {"调用次数": 0, "用户数": 0}}
        
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            daily_data = await self._get_redis_role_user_api_call_data(system_name, date_str)
            
            for operation in ["ROLE_ADD_USER", "ROLE_DEL_USER"]:
                totals[operation]["调用次数"] += daily_data[operation]["调用次数"]
                totals[operation]["用户数"] += daily_data[operation]["用户数"]
            
            current_date += timedelta(days=1)
        
        return totals
    
    async def get_system_daily_statistics_with_redis(self) -> Dict:
        """
        仿照get_system_daily_statistics，混合使用Kafka数据和Redis校验数据
        - 文件操作：使用Kafka数据（已校验）
        - 角色组操作：使用Kafka数据（已校验） 
        - 文件权限设置：使用Redis校验通过数据
        - 角色组用户操作：使用Redis校验通过数据
        保持与原函数完全相同的返回格式
        
        Returns:
            Dict: 与get_system_daily_statistics完全相同格式的统计数据
        """
        await self._ensure_initialized()
        
        # 计算当天的日期范围
        today = datetime.now()
        start_date = today.strftime('%Y-%m-%d')
        
        logger.info(f"获取{start_date}的每个系统混合统计数据（Kafka+Redis校验）")
        
        start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{start_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        
        # 每次实时查询（不使用缓存）
        with get_db() as db:
            # 获取所有系统名称
            systems = db.query(KafkaMessageLog.system_name).filter(
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            ).group_by(KafkaMessageLog.system_name).all()
            
            system_names = [system[0] for system in systems]
            
            result = {
                "日期": start_date,
                "系统统计": []
            }
            
            # 初始化总计统计数据
            total_stats = self._init_total_stats()
            
            # 对每个系统进行统计
            for system_name in system_names:
                logger.info(f"处理系统: {system_name}")
                
                system_stats = await self._get_system_mixed_comprehensive_stats(
                    db, system_name, start_datetime, end_datetime, start_date
                )
                
                # 累计到总统计
                self._accumulate_total_stats(total_stats, system_stats)
                
                # 添加到结果
                result["系统统计"].append(system_stats)
            
            # 添加总和统计到结果
            result["总计"] = total_stats
            
            logger.info(f"混合统计数据处理完成，共处理 {len(system_names)} 个系统")
            return result
    
    async def _get_system_mixed_comprehensive_stats(self, db: Session, system_name: str, 
                                                   start_datetime: datetime, end_datetime: datetime, 
                                                   date: str) -> Dict:
        """获取单个系统的混合综合统计（Kafka + Redis校验数据），保持原格式"""
        
        # 1. 文件操作统计（使用Kafka数据，已校验）
        file_add_stats = self._get_file_add_stats(db, system_name, start_datetime, end_datetime)
        file_del_stats = self._get_file_del_stats(db, system_name, start_datetime, end_datetime)
        
        # 2. 文件更新统计（使用Redis统计数据）
        file_not_change_stats = await self._get_file_not_change_stats(db, system_name, start_datetime, end_datetime)
        
        # 3. 虚拟组操作统计（使用Redis校验数据替换原来的Kafka统计）
        role_ops_stats = await self._get_role_ops_redis_stats(db, system_name, start_datetime, end_datetime, date)
        
        # 组装系统统计结果（完全按照原格式）
        system_stats = {
            "系统名称": system_name,
            "文件操作": {
                "新增": file_add_stats["total_operations"],
                "删除": file_del_stats["delete_operations"]
            },
            "文件更新": {
                "文件更新记录": file_not_change_stats["sync_record_count"],
                "角色添加": file_not_change_stats["sync_roles"]["add_count"],
                "角色删除": file_not_change_stats["sync_roles"]["del_count"],
                "用户添加": file_not_change_stats["sync_users"]["add_count"],
                "用户删除": file_not_change_stats["sync_users"]["del_count"]
            },
            "虚拟组操作": {
                "角色组创建": role_ops_stats["sync_operations"].get("ADD_ROLE", 0),
                "角色组创建用户数": role_ops_stats["sync_users"].get("ADD_ROLE", 0),
                "角色组删除": role_ops_stats["sync_operations"].get("DEL_ROLE", 0),
                "角色添加记录": role_ops_stats["sync_operations"].get("ROLE_ADD_USER", 0),
                "角色添加用户": role_ops_stats["sync_users"].get("ROLE_ADD_USER", 0),
                "角色删除记录": role_ops_stats["sync_operations"].get("ROLE_DEL_USER", 0),
                "角色删除用户": role_ops_stats["sync_users"].get("ROLE_DEL_USER", 0)
            },
            "Kafka消息统计": {
                "文件新增消息": file_add_stats["kafka_message_count"],
                "文件删除消息": file_del_stats["kafka_message_count"],
                "文件更新消息": file_not_change_stats["kafka_message_count"],
                "角色组创建消息": role_ops_stats["kafka_messages"].get("ADD_ROLE", 0),
                "角色组删除消息": role_ops_stats["kafka_messages"].get("DEL_ROLE", 0),
                "角色添加用户消息": role_ops_stats["kafka_messages"].get("ROLE_ADD_USER", 0),
                "角色删除用户消息": role_ops_stats["kafka_messages"].get("ROLE_DEL_USER", 0),
                "文件更新": {
                    "添加用户数": file_not_change_stats["kafka_users"]["add_count"],
                    "删除用户数": file_not_change_stats["kafka_users"]["del_count"]
                },
                "角色用户变更": {
                    "添加用户数": role_ops_stats["kafka_users"].get("ROLE_ADD_USER", 0),
                    "删除用户数": role_ops_stats["kafka_users"].get("ROLE_DEL_USER", 0)
                }
            }
        }
        
        return system_stats
    
    def _init_total_stats(self) -> Dict:
        """初始化总统计数据结构（与原函数完全相同）"""
        return {
            "系统名称": "总计",
            "文件操作": {"新增": 0, "删除": 0},
            "文件更新": {
                "文件更新记录": 0,
                "角色添加": 0,
                "角色删除": 0,
                "用户添加": 0,
                "用户删除": 0
            },
            "虚拟组操作": {
                "角色组创建": 0,
                "角色组删除": 0,
                "角色组创建用户数": 0,
                "角色添加记录": 0,
                "角色添加用户": 0,
                "角色删除记录": 0,
                "角色删除用户": 0
            },
            "Kafka消息统计": {
                "文件新增消息": 0,
                "文件删除消息": 0,
                "文件更新消息": 0,
                "角色组创建消息": 0,
                "角色组删除消息": 0,
                "角色添加用户消息": 0,
                "角色删除用户消息": 0,
                "文件更新": {"添加用户数": 0, "删除用户数": 0},
                "角色用户变更": {"添加用户数": 0, "删除用户数": 0}
            }
        }
    
    def _accumulate_total_stats(self, total_stats: Dict, system_stats: Dict):
        """累计系统统计到总统计（与原函数完全相同）"""
        total_stats["文件操作"]["新增"] += system_stats["文件操作"]["新增"]
        total_stats["文件操作"]["删除"] += system_stats["文件操作"]["删除"]
        
        total_stats["文件更新"]["文件更新记录"] += system_stats["文件更新"]["文件更新记录"]
        total_stats["文件更新"]["角色添加"] += system_stats["文件更新"]["角色添加"]
        total_stats["文件更新"]["角色删除"] += system_stats["文件更新"]["角色删除"]
        total_stats["文件更新"]["用户添加"] += system_stats["文件更新"]["用户添加"]
        total_stats["文件更新"]["用户删除"] += system_stats["文件更新"]["用户删除"]
        
        for key in total_stats["虚拟组操作"]:
            total_stats["虚拟组操作"][key] += system_stats["虚拟组操作"][key]
        
        for key in total_stats["Kafka消息统计"]:
            if isinstance(total_stats["Kafka消息统计"][key], dict):
                for sub_key in total_stats["Kafka消息统计"][key]:
                    total_stats["Kafka消息统计"][key][sub_key] += system_stats["Kafka消息统计"][key][sub_key]
            else:
                total_stats["Kafka消息统计"][key] += system_stats["Kafka消息统计"][key]
    
    def _get_file_add_stats(self, db: Session, system_name: str, start_datetime: datetime, end_datetime: datetime) -> Dict:
        """获取FILE_ADD类型消息的统计信息（使用与StatisticsService相同的逻辑）"""
        # 1. 统计Kafka消息数量
        file_add_msg_count = db.query(func.count(KafkaMessageLog.id)).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_ADD',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).scalar() or 0
        
        # 2. 使用与StatisticsService._get_system_incremental_stats_for_period相同的逻辑
        # 获取FILE_ADD消息记录
        file_add_records = db.query(
            KafkaMessageLog.message_content,
            KafkaMessageLog.file_number,
            KafkaMessageLog.created_at
        ).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_ADD',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        # 提取file_numbers和创建映射
        unique_file_numbers = set()
        file_number_to_log = {}
        
        for record in file_add_records:
            try:
                file_number = record.file_number
                
                if file_number:
                    unique_file_numbers.add(file_number)
                    if (file_number not in file_number_to_log or 
                        record.created_at > file_number_to_log[file_number]['created_at']):
                        file_number_to_log[file_number] = {
                            'created_at': record.created_at
                        }
            except Exception as e:
                logger.error(f"获取file_number失败: {str(e)}")
        
        add_count = 0
        update_count = 0
        
        if unique_file_numbers:
            # 批量查询文档
            existing_docs = db.query(
                UnstructuredDocument.file_number,
                UnstructuredDocument.created_at
            ).filter(
                and_(
                    UnstructuredDocument.system_name == system_name,
                    UnstructuredDocument.file_number.in_(list(unique_file_numbers))
                )
            ).all()
            
            doc_map = {doc.file_number: doc for doc in existing_docs}
            
            for file_number in unique_file_numbers:
                kafka_log_info = file_number_to_log.get(file_number)
                if not kafka_log_info:
                    continue
                    
                doc = doc_map.get(file_number)
                if doc:
                    if doc.created_at < kafka_log_info['created_at']:
                        update_count += 1
                    elif (start_datetime.replace(tzinfo=None) <= doc.created_at.replace(tzinfo=None) <= 
                          end_datetime.replace(tzinfo=None)):
                        add_count += 1
        
        return {
            "kafka_message_count": file_add_msg_count,
            "add_count": add_count,
            "update_count": update_count,
            "total_operations": add_count + update_count
        }
    
    def _get_file_del_stats(self, db: Session, system_name: str, start_datetime: datetime, end_datetime: datetime) -> Dict:
        """获取FILE_DEL类型消息的统计信息（复制自统计服务）"""
        file_del_msg_count = db.query(func.count(KafkaMessageLog.id)).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_DEL',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).scalar() or 0
        
        return {
            "kafka_message_count": file_del_msg_count,
            "delete_operations": file_del_msg_count
        }
    
    async def _get_file_not_change_stats(self, db: Session, system_name: str, start_datetime: datetime, end_datetime: datetime) -> Dict:
        """获取FILE_NOT_CHANGE类型消息的统计信息（使用现有的Redis统计键）"""
        try:
            await self._ensure_initialized()
            
            # 按日期获取现有Redis统计数据
            current_date = start_datetime
            total_verification_passed = 0  # 验证通过数量
            total_permission_success = 0   # 权限设置成功数量
            
            while current_date <= end_datetime:
                date_str = current_date.strftime('%Y-%m-%d')
                
                if self.cache_handler:
                    # 1. 获取验证通过统计（文件状态查询成功） - 按系统按日
                    verification_key = f"{system_name}:knowledge:verification:passed:{date_str}"
                    daily_verification = await self.cache_handler.get(verification_key) or 0
                    total_verification_passed += int(daily_verification)
                    
                    # 2. 获取权限设置成功统计  - 按系统按日
                    permission_key = f"{system_name}:knowledge:operation:success:{date_str}"
                    daily_permission = await self.cache_handler.get(permission_key) or 0
                    total_permission_success += int(daily_permission)
                
                current_date += timedelta(days=1)
            
            logger.info(f"从Redis键获取FILE_NOT_CHANGE统计 - 系统: {system_name}, 验证通过: {total_verification_passed}, 权限设置成功: {total_permission_success}")
            
            # 如果Redis中没有数据，fallback到数据库查询（不再使用全局旧键）
            if total_verification_passed == 0 and total_permission_success == 0:
                logger.info("Redis中暂无按系统的统计数据，使用数据库fallback")
                return await self._get_file_not_change_stats_fallback(db, system_name, start_datetime, end_datetime)
            
            # 同步相关统计从数据库获取
            sync_stats = self._get_file_not_change_sync_stats(db, system_name, start_datetime, end_datetime)

            # 以数据库中 KnowledgeRoleSyncLog 的记录数作为“同步记录数”，保证与 fallback 一致
            file_not_change_sync_count = db.query(func.count(KnowledgeRoleSyncLog.id)).filter(
                and_(
                    KnowledgeRoleSyncLog.message_type == 'FILE_NOT_CHANGE',
                    KnowledgeRoleSyncLog.system_name == system_name,
                    KnowledgeRoleSyncLog.created_at.between(start_datetime, end_datetime)
                )
            ).scalar() or 0
            
            # 从数据库获取用户操作统计作为补充
            file_not_change_msgs = db.query(KafkaMessageLog.message_content).filter(
                and_(
                    KafkaMessageLog.message_type == 'FILE_NOT_CHANGE',
                    KafkaMessageLog.system_name == system_name,
                    KafkaMessageLog.created_at.between(start_datetime, end_datetime)
                )
            ).all()
            kafka_add_user_count, kafka_del_user_count = self._parse_file_not_change_users(file_not_change_msgs)
            
            return {
                "kafka_message_count": total_verification_passed,  # 使用验证通过数量作为消息处理数量
                "sync_record_count": total_permission_success,     # 使用权限设置成功数量作为文件更新记录
                "kafka_users": {
                    "add_count": kafka_add_user_count,
                    "del_count": kafka_del_user_count
                },
                "sync_roles": sync_stats["roles"],
                "sync_users": sync_stats["users"]
            }
            
        except Exception as e:
            logger.error(f"获取FILE_NOT_CHANGE Redis统计失败: {str(e)}，fallback到数据库查询")
            return await self._get_file_not_change_stats_fallback(db, system_name, start_datetime, end_datetime)
    
    async def _get_file_not_change_stats_fallback(self, db: Session, system_name: str, start_datetime: datetime, end_datetime: datetime) -> Dict:
        """FILE_NOT_CHANGE统计的数据库fallback方法"""
        file_not_change_msg_count = db.query(func.count(KafkaMessageLog.id)).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_NOT_CHANGE',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).scalar() or 0
        
        file_not_change_msgs = db.query(KafkaMessageLog.message_content).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_NOT_CHANGE',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        kafka_add_user_count, kafka_del_user_count = self._parse_file_not_change_users(file_not_change_msgs)
        
        file_not_change_sync_count = db.query(func.count(KnowledgeRoleSyncLog.id)).filter(
            and_(
                KnowledgeRoleSyncLog.message_type == 'FILE_NOT_CHANGE',
                KnowledgeRoleSyncLog.system_name == system_name,
                KnowledgeRoleSyncLog.created_at.between(start_datetime, end_datetime)
            )
        ).scalar() or 0
        
        sync_stats = self._get_file_not_change_sync_stats(db, system_name, start_datetime, end_datetime)
        
        return {
            "kafka_message_count": file_not_change_msg_count,
            "sync_record_count": file_not_change_sync_count,  # fallback时仍使用数据库同步记录数
            "kafka_users": {
                "add_count": kafka_add_user_count,
                "del_count": kafka_del_user_count
            },
            "sync_roles": sync_stats["roles"],
            "sync_users": sync_stats["users"]
        }
    
    async def _get_redis_verification_stats(self, system_name: str, date: str) -> Dict:
        """获取知识库验证统计（支持新格式和兼容旧格式）"""
        try:
            logger.info(f"获取知识库验证统计，系统: {system_name}, 日期: {date}")
            
            # 根据系统名称决定使用新格式还是旧格式
            if system_name == "legacy_system":
                # 旧格式键 - 暂时没有知识库验证的旧格式键，返回默认值
                logger.info("使用旧格式，但知识库验证暂无旧格式键")
                completed_count = 0
                failed_count = 0
            else:
                # 新格式键：{system_name}:knowledge:verification:passed:{date}
                verification_passed_key = f"{system_name}:knowledge:verification:passed:{date}"  
                verification_failed_key = f"{system_name}:knowledge:verification:failed:{date}"
                
                logger.info(f"使用新格式知识库验证键: {verification_passed_key}")
                
                # 获取验证统计数据
                completed_count = await self.cache_handler.get(verification_passed_key) or 0
                failed_count = await self.cache_handler.get(verification_failed_key) or 0
                
                # 如果是简单计数器而不是hash，直接转换
                try:
                    completed_count = int(completed_count)
                    failed_count = int(failed_count)
                except (ValueError, TypeError):
                    completed_count = 0 
                    failed_count = 0
            
            total_count = completed_count + failed_count
            success_rate = round(completed_count / total_count * 100, 2) if total_count > 0 else 0
            
            return {
                "验证总数": total_count,
                "验证成功": completed_count,
                "验证失败": failed_count,
                "成功率": success_rate
            }
        except Exception as e:
            logger.error(f"获取验证统计失败: {system_name}, {date}, {str(e)}")
            return {"验证总数": 0, "验证成功": 0, "验证失败": 0, "成功率": 0}
    
    async def _get_redis_permission_stats(self, system_name: str, date: str) -> Dict:
        """获取权限设置统计（支持新格式和兼容旧格式）"""
        try:
            logger.info(f"获取权限设置统计，系统: {system_name}, 日期: {date}")
            
            # 根据系统名称决定使用新格式还是旧格式
            if system_name == "legacy_system":
                # 旧格式键
                permission_key = "knowledge:permission:set:success:count"
                logger.info(f"使用旧格式权限键: {permission_key}")
                permission_count = await self.cache_handler.get(permission_key) or 0
            else:
                # 新格式键：{system_name}:knowledge:operation:success:{date}
                permission_key = f"{system_name}:knowledge:operation:success:{date}"
                logger.info(f"使用新格式权限键: {permission_key}")
                permission_count = await self.cache_handler.get(permission_key) or 0
            
            # 转换为整数
            try:
                permission_count = int(permission_count)
            except (ValueError, TypeError):
                permission_count = 0
            
            return {
                "权限设置次数": permission_count
            }
        except Exception as e:
            logger.error(f"获取权限统计失败: {system_name}, {date}, {str(e)}")
            return {"权限设置次数": 0}
    
    async def _get_redis_role_user_stats(self, system_name: str, date: str) -> Dict:
        """获取角色用户操作统计（支持新格式和兼容旧格式）"""
        try:
            logger.info(f"获取角色用户统计，系统: {system_name}, 日期: {date}")
            
            # 根据系统名称决定使用新格式还是旧格式
            if system_name == "legacy_system":
                # 旧格式键
                add_user_verification_key = "role_user:add_user:verification:passed"
                del_user_verification_key = "role_user:del_user:verification:passed"
                logger.info("使用旧格式Redis键")
            else:
                # 新格式键：{system_name}:role_user:add_user:verification:passed:{date}
                add_user_verification_key = f"{system_name}:role_user:add_user:verification:passed:{date}"
                del_user_verification_key = f"{system_name}:role_user:del_user:verification:passed:{date}"
                logger.info(f"使用新格式Redis键: {add_user_verification_key}")
            
            # 获取添加用户统计
            add_verification_data = await self.cache_handler.get_hash_all(add_user_verification_key)
            logger.info(f"添加用户验证数据: {add_verification_data}")
            
            # 获取删除用户统计
            del_verification_data = await self.cache_handler.get_hash_all(del_user_verification_key)
            logger.info(f"删除用户验证数据: {del_verification_data}")
            
            # 提取统计数据
            add_count = add_verification_data.get("message_count", 0)
            add_users = add_verification_data.get("user_count", 0)
            del_count = del_verification_data.get("message_count", 0) 
            del_users = del_verification_data.get("user_count", 0)
            
            return {
                "添加用户操作": {
                    "操作次数": int(add_count),
                    "用户数量": int(add_users)
                },
                "删除用户操作": {
                    "操作次数": int(del_count),
                    "用户数量": int(del_users)
                }
            }
        except Exception as e:
            logger.error(f"获取角色用户统计失败: {system_name}, {date}, {str(e)}")
            return {
                "添加用户操作": {"操作次数": 0, "用户数量": 0},
                "删除用户操作": {"操作次数": 0, "用户数量": 0}
            }
    
    async def _get_database_fallback_statistics(self, date: str) -> Dict:
        """数据库降级查询统计（简化版）"""
        logger.info(f"使用数据库降级统计: {date}")
        
        start_datetime = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        
        with get_db() as db:
            # 获取系统名称
            systems = db.query(KafkaMessageLog.system_name).filter(
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            ).group_by(KafkaMessageLog.system_name).all()
            
            system_names = [system[0] for system in systems]
            
            result = {
                "日期": date,
                "数据源": "数据库降级",
                "系统统计": []
            }
            
            total_stats = self._init_database_fallback_total_stats()
            
            for system_name in system_names:
                system_stats = self._get_database_system_stats(db, system_name, start_datetime, end_datetime)
                result["系统统计"].append(system_stats)
                self._accumulate_database_total_stats(total_stats, system_stats)
            
            result["总计"] = total_stats
            return result
    
    def _get_database_system_stats(self, db: Session, system_name: str, start_datetime: datetime, end_datetime: datetime) -> Dict:
        """数据库查询单个系统统计（简化版）"""
        # 基本消息统计
        total_messages = db.query(KafkaMessageLog).filter(
            and_(
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).count()
        
        # 文档相关统计
        file_add_count = db.query(KafkaMessageLog).filter(
            and_(
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.message_type == 'FILE_ADD',
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).count()
        
        file_del_count = db.query(KafkaMessageLog).filter(
            and_(
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.message_type == 'FILE_DEL',
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).count()
        
        # 角色同步统计
        role_sync_count = db.query(KnowledgeRoleSyncLog).filter(
            and_(
                KnowledgeRoleSyncLog.system_name == system_name,
                KnowledgeRoleSyncLog.created_at.between(start_datetime, end_datetime)
            )
        ).count()
        
        return {
            "系统名称": system_name,
            "数据库统计": {
                "总消息数": total_messages,
                "文件新增": file_add_count,
                "文件删除": file_del_count,
                "角色同步": role_sync_count
            }
        }
    
    def _init_redis_total_stats(self) -> Dict:
        """初始化Redis统计总计"""
        return {
            "系统名称": "总计",
            "Redis统计": {
                "知识库验证": {"验证总数": 0, "验证成功": 0, "验证失败": 0, "成功率": 0},
                "文档权限设置": {"权限设置次数": 0},
                "角色用户操作": {
                    "添加用户操作": {"操作次数": 0, "用户数量": 0},
                    "删除用户操作": {"操作次数": 0, "用户数量": 0}
                }
            }
        }
    
    def _init_database_fallback_total_stats(self) -> Dict:
        """初始化数据库降级总计"""
        return {
            "系统名称": "总计",
            "数据库统计": {
                "总消息数": 0,
                "文件新增": 0,
                "文件删除": 0,
                "角色同步": 0
            }
        }
    
    def _accumulate_redis_total_stats(self, total_stats: Dict, system_stats: Dict):
        """累积Redis统计到总计 - 支持按系统分类"""
        system_name = system_stats["系统名称"]
        redis_stats = system_stats["Redis统计"]
        
        # 如果是第一次处理这个系统，需要初始化系统分类统计
        if "按系统分类" not in total_stats:
            total_stats["按系统分类"] = {}
            
        if system_name not in total_stats["按系统分类"]:
            total_stats["按系统分类"][system_name] = {
                "知识库验证": {"验证总数": 0, "验证成功": 0, "验证失败": 0, "成功率": 0},
                "文档权限设置": {"权限设置次数": 0},
                "角色用户操作": {
                    "添加用户操作": {"操作次数": 0, "用户数量": 0},
                    "删除用户操作": {"操作次数": 0, "用户数量": 0}
                }
            }
        
        # 累积到系统分类统计
        system_redis = total_stats["按系统分类"][system_name]
        
        # 累积角色用户统计到系统分类
        role_user = redis_stats["角色用户操作"]
        system_role_user = system_redis["角色用户操作"]
        system_role_user["添加用户操作"]["操作次数"] += role_user["添加用户操作"]["操作次数"]
        system_role_user["添加用户操作"]["用户数量"] += role_user["添加用户操作"]["用户数量"]
        system_role_user["删除用户操作"]["操作次数"] += role_user["删除用户操作"]["操作次数"]
        system_role_user["删除用户操作"]["用户数量"] += role_user["删除用户操作"]["用户数量"]
        
        # 累积验证统计到系统分类
        verification = redis_stats["知识库验证"]
        system_redis["知识库验证"]["验证总数"] += verification["验证总数"]
        system_redis["知识库验证"]["验证成功"] += verification["验证成功"]
        system_redis["知识库验证"]["验证失败"] += verification["验证失败"]
        
        # 重新计算系统级别成功率
        system_verification = system_redis["知识库验证"]["验证总数"]
        system_success = system_redis["知识库验证"]["验证成功"]
        system_redis["知识库验证"]["成功率"] = round(system_success / system_verification * 100, 2) if system_verification > 0 else 0
        
        # 累积权限统计到系统分类
        system_redis["文档权限设置"]["权限设置次数"] += redis_stats["文档权限设置"]["权限设置次数"]
        
        # 同时也累积到总计（保持原有功能）
        total_redis = total_stats["Redis统计"]
        
        # 累积验证统计到总计
        total_redis["知识库验证"]["验证总数"] += verification["验证总数"]
        total_redis["知识库验证"]["验证成功"] += verification["验证成功"]
        total_redis["知识库验证"]["验证失败"] += verification["验证失败"]
        
        # 重新计算总体成功率  
        total_verification = total_redis["知识库验证"]["验证总数"]
        total_success = total_redis["知识库验证"]["验证成功"]
        total_redis["知识库验证"]["成功率"] = round(total_success / total_verification * 100, 2) if total_verification > 0 else 0
        
        # 累积权限统计到总计
        total_redis["文档权限设置"]["权限设置次数"] += redis_stats["文档权限设置"]["权限设置次数"]
        
        # 累积角色用户统计到总计
        total_role_user = total_redis["角色用户操作"]
        total_role_user["添加用户操作"]["操作次数"] += role_user["添加用户操作"]["操作次数"]
        total_role_user["添加用户操作"]["用户数量"] += role_user["添加用户操作"]["用户数量"]
        total_role_user["删除用户操作"]["操作次数"] += role_user["删除用户操作"]["操作次数"]
        total_role_user["删除用户操作"]["用户数量"] += role_user["删除用户操作"]["用户数量"]
    
    def _accumulate_database_total_stats(self, total_stats: Dict, system_stats: Dict):
        """累积数据库统计到总计"""
        db_stats = system_stats["数据库统计"]
        total_db = total_stats["数据库统计"]
        
        total_db["总消息数"] += db_stats["总消息数"]
        total_db["文件新增"] += db_stats["文件新增"]
        total_db["文件删除"] += db_stats["文件删除"]
        total_db["角色同步"] += db_stats["角色同步"]

    async def _get_role_ops_redis_stats(self, db: Session, system_name: str, 
                                       start_datetime: datetime, end_datetime: datetime, 
                                       date: str) -> Dict:
        """获取虚拟组操作统计 - 混合Redis验证数据和Kafka角色组数据"""
        
        # 1. 从Kafka获取角色组操作数据（ADD_ROLE, DEL_ROLE - 不需要验证）
        add_role_messages = db.query(VirtualGroupSyncLog).filter(and_(
            VirtualGroupSyncLog.system_name == system_name,
            VirtualGroupSyncLog.message_type == 'ADD_ROLE',
            VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
        )).all()
        
        del_role_messages = db.query(VirtualGroupSyncLog).filter(and_(
            VirtualGroupSyncLog.system_name == system_name,
            VirtualGroupSyncLog.message_type == 'DEL_ROLE',
            VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
        )).all()
        
        # 统计角色组操作
        kafka_add_role_count = len(add_role_messages)
        kafka_del_role_count = len(del_role_messages)
        
        # 统计通过角色组操作影响的用户数
        kafka_add_role_users = sum(len(msg.add_user_list or []) for msg in add_role_messages)
        kafka_del_role_users = sum(len(msg.del_user_list or []) for msg in del_role_messages)
        
        # 2. 从Redis获取角色用户操作验证数据（ROLE_ADD_USER, ROLE_DEL_USER - 需要验证）
        try:
            # 根据系统名称决定使用新格式还是旧格式
            if system_name == "legacy_system":
                # 旧格式键
                add_user_verification_key = "role_user:add_user:verification:passed"
                del_user_verification_key = "role_user:del_user:verification:passed"
                add_user_success_key = "role_user:add_user:operation:success"
                del_user_success_key = "role_user:del_user:operation:success"
            else:
                # 新格式键：{system_name}:role_user:add_user:verification:passed:{date}
                add_user_verification_key = f"{system_name}:role_user:add_user:verification:passed:{date}"
                del_user_verification_key = f"{system_name}:role_user:del_user:verification:passed:{date}"
                add_user_success_key = f"{system_name}:role_user:add_user:operation:success:{date}"
                del_user_success_key = f"{system_name}:role_user:del_user:operation:success:{date}"
            
            # 获取Redis验证统计数据
            add_verification_data = await self.cache_handler.get_hash_all(add_user_verification_key)
            del_verification_data = await self.cache_handler.get_hash_all(del_user_verification_key)
            add_success_data = await self.cache_handler.get_hash_all(add_user_success_key)
            del_success_data = await self.cache_handler.get_hash_all(del_user_success_key)
            
            # 提取Redis统计数据
            redis_add_user_messages = add_verification_data.get("message_count", 0)
            redis_add_user_count = add_verification_data.get("user_count", 0)
            redis_add_user_success_messages = add_success_data.get("message_count", 0)
            redis_add_user_success_count = add_success_data.get("user_count", 0)
            
            redis_del_user_messages = del_verification_data.get("message_count", 0)
            redis_del_user_count = del_verification_data.get("user_count", 0)
            redis_del_user_success_messages = del_success_data.get("message_count", 0)
            redis_del_user_success_count = del_success_data.get("user_count", 0)
            
        except Exception as e:
            logger.error(f"获取Redis角色用户统计失败: {str(e)}")
            # Redis失败时设置默认值
            redis_add_user_messages = redis_add_user_count = 0
            redis_add_user_success_messages = redis_add_user_success_count = 0
            redis_del_user_messages = redis_del_user_count = 0
            redis_del_user_success_messages = redis_del_user_success_count = 0
        
        # 3. 组装混合统计结果（保持原格式）
        return {
            "add_count": kafka_add_role_count + int(redis_add_user_success_messages),  # 角色组创建 + 用户添加成功
            "add_user_count": kafka_add_role_users + int(redis_add_user_success_count),  # 通过角色组影响的用户 + 直接添加的用户
            "del_count": kafka_del_role_count + int(redis_del_user_success_messages),  # 角色组删除 + 用户删除成功
            "del_user_count": kafka_del_role_users + int(redis_del_user_success_count),  # 通过角色组影响的用户 + 直接删除的用户
            "sync_roles": kafka_add_role_count + kafka_del_role_count,  # 同步的角色组数量
            "kafka_messages": {
                "ADD_ROLE": kafka_add_role_count,
                "DEL_ROLE": kafka_del_role_count,
                "ROLE_ADD_USER": int(redis_add_user_messages),  # 使用Redis验证数据中的消息数量
                "ROLE_DEL_USER": int(redis_del_user_messages)   # 使用Redis验证数据中的消息数量
            },
            "kafka_users": {
                "ADD_ROLE": kafka_add_role_users,
                "DEL_ROLE": kafka_del_role_users,
                "ROLE_ADD_USER": int(redis_add_user_count),  # 使用Redis验证数据中的用户数量
                "ROLE_DEL_USER": int(redis_del_user_count)   # 使用Redis验证数据中的用户数量
            },
            "sync_operations": {
                "ADD_ROLE": kafka_add_role_count,
                "DEL_ROLE": kafka_del_role_count,
                "ROLE_ADD_USER": int(redis_add_user_success_messages),
                "ROLE_DEL_USER": int(redis_del_user_success_messages)
            },
            "sync_users": {
                "ADD_ROLE": kafka_add_role_users,
                "DEL_ROLE": kafka_del_role_users,
                "ROLE_ADD_USER": int(redis_add_user_success_count),
                "ROLE_DEL_USER": int(redis_del_user_success_count)
            }
        }

    def _parse_file_not_change_users(self, msgs: List) -> Tuple[int, int]:
        """解析FILE_NOT_CHANGE消息中的用户数量"""
        kafka_add_user_count = 0
        kafka_del_user_count = 0
        
        for msg in msgs:
            try:
                message_content = self._parse_kafka_json(msg.message_content)
                
                add_users = message_content.get('fileAddUserList', [])
                del_users = message_content.get('fileDelUserList', [])
                
                if isinstance(add_users, list):
                    kafka_add_user_count += len(add_users)
                if isinstance(del_users, list):
                    kafka_del_user_count += len(del_users)
                    
            except Exception as e:
                logger.error(f"解析FILE_NOT_CHANGE消息内容失败: {str(e)}")
        
        return kafka_add_user_count, kafka_del_user_count

    def _parse_kafka_json(self, raw):
        """
        统一处理 Kafka 消息的 JSON 解析
        
        Args:
            raw: 原始数据，可能是字符串或已解析的对象
            
        Returns:
            解析后的对象，如果解析失败或为空则返回空字典
        """
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"JSON解析失败: {str(e)}, 原始数据: {raw}")
                return {}
        return raw or {}

    def _get_file_not_change_sync_stats(self, db: Session, system_name: str, 
                                      start_datetime: datetime, end_datetime: datetime) -> Dict:
        """获取FILE_NOT_CHANGE同步统计"""
        sync_logs = db.query(
            KnowledgeRoleSyncLog.add_role_list,
            KnowledgeRoleSyncLog.del_role_list,
            KnowledgeRoleSyncLog.add_user_list,
            KnowledgeRoleSyncLog.del_user_list
        ).filter(
            and_(
                KnowledgeRoleSyncLog.message_type == 'FILE_NOT_CHANGE',
                KnowledgeRoleSyncLog.system_name == system_name,
                KnowledgeRoleSyncLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        sync_add_role_count = 0
        sync_del_role_count = 0
        sync_add_user_count = 0
        sync_del_user_count = 0
        
        for log in sync_logs:
            # 解析各种列表
            if log.add_role_list:
                try:
                    parsed_list = self._parse_kafka_json(log.add_role_list)
                    if isinstance(parsed_list, list):
                        sync_add_role_count += len(parsed_list)
                except Exception as e:
                    logger.error(f"解析add_role_list失败: {str(e)}")
            
            if log.del_role_list:
                try:
                    parsed_list = self._parse_kafka_json(log.del_role_list)
                    if isinstance(parsed_list, list):
                        sync_del_role_count += len(parsed_list)
                except Exception as e:
                    logger.error(f"解析del_role_list失败: {str(e)}")
            
            if log.add_user_list:
                try:
                    parsed_list = self._parse_kafka_json(log.add_user_list)
                    if isinstance(parsed_list, list):
                        sync_add_user_count += len(parsed_list)
                except Exception as e:
                    logger.error(f"解析add_user_list失败: {str(e)}")
            
            if log.del_user_list:
                try:
                    parsed_list = self._parse_kafka_json(log.del_user_list)
                    if isinstance(parsed_list, list):
                        sync_del_user_count += len(parsed_list)
                except Exception as e:
                    logger.error(f"解析del_user_list失败: {str(e)}")
        
        return {
            "roles": {
                "add_count": sync_add_role_count,
                "del_count": sync_del_role_count
            },
            "users": {
                "add_count": sync_add_user_count,
                "del_count": sync_del_user_count
            }
        }
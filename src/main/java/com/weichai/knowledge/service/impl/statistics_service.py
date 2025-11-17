import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy import func, and_, desc, or_
from sqlalchemy.orm import Session
import json

from src.db.database import get_db
from src.schemas.schemas_comment import (
    KafkaMessageLog, 
    UnstructuredDocument, 
    KnowledgeRoleSyncLog, 
    VirtualGroupSyncLog
)
from src.configs.log_config import get_logger
from src.utils.cache_handler import CacheHandler

logger = get_logger(__name__)


class StatisticsService:
    """
    统计服务类 - 专门处理数据统计和分析功能
    职责：数据统计、汇总分析、报表生成
    """

    def __init__(self):
        """初始化统计服务"""
        self.cache_handler = CacheHandler()
        self._initialized = False
        logger.info("StatisticsService初始化完成")
        
    async def _ensure_initialized(self):
        """确保服务已初始化"""
        if not self._initialized:
            try:
                # start() 现在只是记录日志，连接由管理器自动处理
                await self.cache_handler.start()
                self._initialized = True
                logger.info("StatisticsService缓存服务初始化成功")
            except Exception as e:
                logger.error(f"StatisticsService缓存服务初始化失败: {str(e)}")
                
    async def shutdown(self):
        """关闭服务并释放资源"""
        logger.info("正在关闭StatisticsService...")
        try:
            if hasattr(self, 'cache_handler') and self.cache_handler:
                if hasattr(self.cache_handler, 'close'):
                    # close() 现在只是记录日志，实际连接由全局管理器统一管理
                    await self.cache_handler.close()
                    logger.info("统计服务缓存处理器已关闭")
            logger.info("StatisticsService已完全关闭")
        except Exception as e:
            logger.error(f"关闭StatisticsService时出错: {str(e)}")

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

    async def get_daily_statistics(self, start_date: Optional[str] = None, 
                                 end_date: Optional[str] = None) -> Dict:
        """
        获取每日接入统计数据
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict: 包含每日统计数据的字典
        """
        await self._ensure_initialized()
        
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
        
        with get_db() as db:
            daily_stats = self._collect_daily_statistics_optimized(db, start_datetime, end_datetime)
            
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

    def _collect_daily_statistics_optimized(self, db: Session, start_datetime: datetime, 
                                          end_datetime: datetime) -> List[Dict]:
        """
        优化版：收集每日统计数据
        使用更高效的SQL查询和批量处理
        """
        daily_stats = []
        current_date = start_datetime
        
        while current_date <= end_datetime:
            next_date = current_date + timedelta(days=1)
            day_start = current_date
            day_end = next_date - timedelta(seconds=1)
            
            # 使用一次查询获取当天所有FILE_ADD消息
            file_add_records = self._get_file_add_records_for_day(db, day_start, day_end)
            
            # 提取并去重file_number
            unique_file_numbers = self._extract_unique_file_numbers(file_add_records)
            total_count = len(unique_file_numbers)
            
            if total_count > 0:
                # 批量查询文档状态
                incremental_stats = self._get_incremental_stats_batch(
                    db, unique_file_numbers, file_add_records, day_start, day_end
                )
                success_count = self._get_success_count_batch(
                    db, unique_file_numbers, day_start, day_end
                )
            else:
                incremental_stats = {"add_count": 0, "update_count": 0}
                success_count = 0
            
            # 计算成功率
            success_rate = round(success_count / total_count * 100, 2) if total_count > 0 else 0
            
            daily_stats.append({
                "日期": current_date.strftime('%Y-%m-%d'),
                "新增文档数": incremental_stats["add_count"],
                "更新文档数": incremental_stats["update_count"],
                "总接入量": total_count,
                "成功接入量": success_count,
                "成功率": success_rate
            })
            
            current_date = next_date
        
        return daily_stats

    def _get_file_add_records_for_day(self, db: Session, day_start: datetime, 
                                    day_end: datetime) -> List:
        """获取指定日期的FILE_ADD记录"""
        return db.query(
            KafkaMessageLog.id,
            KafkaMessageLog.message_content,
            KafkaMessageLog.file_number,
            KafkaMessageLog.created_at
        ).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_ADD',
                KafkaMessageLog.created_at.between(day_start, day_end)
            )
        ).all()

    def _extract_unique_file_numbers(self, file_add_records: List) -> set:
        """从FILE_ADD记录中提取唯一的file_number"""
        unique_file_numbers = set()
        
        for record in file_add_records:
            try:
                file_number = record.file_number
                
                if file_number:
                    unique_file_numbers.add(file_number)
            except Exception as e:
                logger.error(f"获取file_number失败: {str(e)}, 记录ID: {record.id}")
        
        return unique_file_numbers

    def _get_incremental_stats_batch(self, db: Session, unique_file_numbers: set, 
                                   file_add_records: List, day_start: datetime, 
                                   day_end: datetime) -> Dict:
        """批量获取新增和更新统计"""
        if not unique_file_numbers:
            return {"add_count": 0, "update_count": 0}
        
        # 批量查询所有相关文档
        existing_docs = db.query(
            UnstructuredDocument.file_number,
            UnstructuredDocument.created_at
        ).filter(
            UnstructuredDocument.file_number.in_(list(unique_file_numbers))
        ).all()
        
        doc_map = {doc.file_number: doc for doc in existing_docs}
        
        # 创建file_number到最新Kafka消息的映射
        file_number_to_log = {}
        for record in file_add_records:
            try:
                file_number = record.file_number
                
                if file_number and (file_number not in file_number_to_log or 
                                  record.created_at > file_number_to_log[file_number]['created_at']):
                    file_number_to_log[file_number] = {
                        'created_at': record.created_at
                    }
            except Exception as e:
                logger.error(f"处理消息记录失败: {str(e)}, 记录ID: {record.id}")
        
        add_count = 0
        update_count = 0
        
        for file_number in unique_file_numbers:
            kafka_log_info = file_number_to_log.get(file_number)
            if not kafka_log_info:
                continue
                
            doc = doc_map.get(file_number)
            if doc:
                # 比较文档创建时间和Kafka消息时间
                if doc.created_at < kafka_log_info['created_at']:
                    update_count += 1
                elif (day_start <= doc.created_at.replace(tzinfo=None) <= 
                      day_end.replace(tzinfo=None)):
                    add_count += 1
        
        return {"add_count": add_count, "update_count": update_count}

    def _get_success_count_batch(self, db: Session, unique_file_numbers: set, 
                               day_start: datetime, day_end: datetime) -> int:
        """批量获取成功接入数量"""
        if not unique_file_numbers:
            return 0
        
        # 使用更高效的EXISTS子查询
        count = db.query(func.count(func.distinct(UnstructuredDocument.file_number))).filter(
            and_(
                UnstructuredDocument.file_number.in_(list(unique_file_numbers)),
                or_(
                    # 新增文档：创建时间在查询范围内
                    UnstructuredDocument.created_at.between(day_start, day_end),
                    # 更新文档：创建时间早于查询开始时间
                    UnstructuredDocument.created_at < day_start
                )
            )
        ).scalar() or 0
        
        return count

    async def get_summary_statistics(self, days: int = 30, start_date: Optional[str] = None, 
                                   end_date: Optional[str] = None) -> Dict:
        """
        获取数据接入汇总统计
        
        Args:
            days: 统计天数，默认30天
            start_date: 开始日期（可选），格式: YYYY-MM-DD
            end_date: 结束日期（可选），格式: YYYY-MM-DD
            
        Returns:
            Dict: 包含数据接入汇总统计的字典
        """
        await self._ensure_initialized()
        
        # 处理日期参数
        if start_date and end_date:
            logger.info(f"使用明确的日期范围: {start_date} 到 {end_date}")
            start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
            end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        else:
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
        
        with get_db() as db:
            system_stats = self._get_system_statistics_optimized(db, start_datetime, end_datetime)
            
            # 计算总量
            total_incremental = sum(stat["incremental_count"] for stat in system_stats)
            total_success = sum(stat["success_count"] for stat in system_stats)
            success_rate = round(total_success / total_incremental * 100, 2) if total_incremental > 0 else 0
            
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

    def _get_system_statistics_optimized(self, db: Session, start_date: datetime, 
                                       end_date: datetime) -> List[Dict]:
        """
        优化版：获取按来源系统分组的统计数据
        使用更高效的SQL查询和批量处理
        """
        # 使用一次查询获取所有系统的FILE_ADD消息
        systems_query = db.query(
            KafkaMessageLog.system_name,
            KafkaMessageLog.message_content,
            KafkaMessageLog.file_number,
            KafkaMessageLog.created_at
        ).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_ADD',
                KafkaMessageLog.created_at.between(start_date, end_date)
            )
        ).all()
        
        # 按系统分组处理数据
        systems_data = {}
        for record in systems_query:
            system_name = record.system_name
            if system_name not in systems_data:
                systems_data[system_name] = []
            systems_data[system_name].append(record)
        
        system_stats = []
        
        # 批量查询所有文档以减少数据库访问
        all_file_numbers = set()
        system_file_numbers = {}
        
        for system_name, records in systems_data.items():
            file_numbers = self._extract_unique_file_numbers(records)
            all_file_numbers.update(file_numbers)
            system_file_numbers[system_name] = {
                'file_numbers': file_numbers,
                'records': records
            }
        
        # 一次性查询所有相关文档
        if all_file_numbers:
            all_docs = db.query(
                UnstructuredDocument.file_number,
                UnstructuredDocument.system_name,
                UnstructuredDocument.created_at
            ).filter(
                UnstructuredDocument.file_number.in_(list(all_file_numbers))
            ).all()
            
            docs_by_system = {}
            for doc in all_docs:
                if doc.system_name not in docs_by_system:
                    docs_by_system[doc.system_name] = []
                docs_by_system[doc.system_name].append(doc)
        else:
            docs_by_system = {}
        
        # 处理每个系统的统计
        for system_name, data in system_file_numbers.items():
            file_numbers = data['file_numbers']
            records = data['records']
            
            incremental_count = len(file_numbers)
            
            # 获取该系统的详细接入统计
            incremental_stats = self._get_system_incremental_stats_optimized(
                system_name, file_numbers, records, docs_by_system.get(system_name, []), 
                start_date, end_date
            )
            
            # 获取成功数据统计
            success_count = len([doc for doc in docs_by_system.get(system_name, []) 
                               if doc.file_number in file_numbers])
            
            # 计算成功率
            success_rate = round(success_count / incremental_count * 100, 2) if incremental_count > 0 else 0
            
            system_stats.append({
                "system_name": system_name,
                "incremental_count": incremental_count,
                "add_count": incremental_stats["add_count"],
                "update_count": incremental_stats["update_count"],
                "success_count": success_count,
                "success_rate": success_rate
            })
        
        return system_stats

    def _get_system_incremental_stats_optimized(self, system_name: str, file_numbers: set, 
                                              records: List, docs: List, 
                                              start_date: datetime, end_date: datetime) -> Dict:
        """优化版：获取系统的新增和更新统计"""
        if not file_numbers:
            return {"add_count": 0, "update_count": 0}
        
        # 创建文档映射
        doc_map = {doc.file_number: doc for doc in docs}
        
        # 创建file_number到最新Kafka消息的映射
        file_number_to_log = {}
        for record in records:
            try:
                file_number = record.file_number
                
                if file_number and (file_number not in file_number_to_log or 
                                  record.created_at > file_number_to_log[file_number]['created_at']):
                    file_number_to_log[file_number] = {
                        'created_at': record.created_at
                    }
            except Exception as e:
                logger.error(f"处理消息记录失败: {str(e)}, 记录ID: {record.id}")
        
        add_count = 0
        update_count = 0
        
        for file_number in file_numbers:
            kafka_log_info = file_number_to_log.get(file_number)
            if not kafka_log_info:
                continue
                
            doc = doc_map.get(file_number)
            if doc:
                if doc.created_at < kafka_log_info['created_at']:
                    update_count += 1
                elif (start_date.replace(tzinfo=None) <= doc.created_at.replace(tzinfo=None) <= 
                      end_date.replace(tzinfo=None)):
                    add_count += 1
        
        return {"add_count": add_count, "update_count": update_count}

    async def get_role_user_statistics(self, start_date: Optional[str] = None, 
                                     end_date: Optional[str] = None) -> Dict:
        """
        获取每日角色和用户变更统计（优化版）
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict: 包含角色和用户变更统计的信息
        """
        await self._ensure_initialized()
        
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"获取从{start_date}到{end_date}的每日角色用户变更统计")
        
        start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        
        with get_db() as db:
            daily_stats = self._collect_role_user_statistics_optimized(db, start_datetime, end_datetime)
            
            # 简化每日统计
            simplified_daily_stats = []
            total_sync_count = 0
            total_kafka_count = 0
            total_sync_roles = 0
            total_sync_users = 0
            
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
                
                # 累计统计
                total_sync_count += day["total_sync_count"]
                total_kafka_count += day["total_kafka_message_count"]
                total_sync_roles += day["total_sync_roles_affected"]
                total_sync_users += day["total_sync_users_affected"]
            
            # 简化汇总统计
            success_rate = round(total_sync_count / total_kafka_count * 100, 2) if total_kafka_count > 0 else 0
            
            simplified_summary = {
                "总同步消息数": total_sync_count,
                "总Kafka消息数": total_kafka_count,
                "同步角色数": total_sync_roles,
                "同步用户数": total_sync_users,
                "成功率": success_rate
            }
        
        result = {
            "日期范围": {
                "start_date": start_date,
                "end_date": end_date
            },
            "每日统计": simplified_daily_stats,
            "汇总统计": simplified_summary
        }
        
        return result

    def _collect_role_user_statistics_optimized(self, db: Session, start_datetime: datetime, 
                                              end_datetime: datetime) -> List[Dict]:
        """
        优化版：收集每日角色和用户变更统计数据
        使用批量查询减少数据库访问次数
        """
        message_types = ['ADD_ROLE', 'DEL_ROLE', 'ROLE_ADD_USER', 'ROLE_DEL_USER']
        
        # 批量查询所有相关的Kafka消息和同步日志
        kafka_messages = db.query(
            KafkaMessageLog.message_type,
            KafkaMessageLog.message_content,
            KafkaMessageLog.created_at
        ).filter(
            and_(
                KafkaMessageLog.message_type.in_(message_types),
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        sync_logs = db.query(
            VirtualGroupSyncLog.message_type,
            VirtualGroupSyncLog.role_id,
            VirtualGroupSyncLog.add_user_list,
            VirtualGroupSyncLog.del_user_list,
            VirtualGroupSyncLog.created_at
        ).filter(
            and_(
                VirtualGroupSyncLog.message_type.in_(message_types),
                VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        # 按日期分组数据
        daily_stats = []
        current_date = start_datetime
        
        while current_date <= end_datetime:
            next_date = current_date + timedelta(days=1)
            day_start = current_date
            day_end = next_date - timedelta(seconds=1)
            
            # 过滤当天的数据
            day_kafka_messages = [msg for msg in kafka_messages 
                                if day_start <= msg.created_at <= day_end]
            day_sync_logs = [log for log in sync_logs 
                           if day_start <= log.created_at <= day_end]
            
            # 处理当天统计
            day_stats = self._process_daily_role_user_stats(
                day_kafka_messages, day_sync_logs, current_date.strftime('%Y-%m-%d')
            )
            
            daily_stats.append(day_stats)
            current_date = next_date
        
        return daily_stats

    def _process_daily_role_user_stats(self, kafka_messages: List, sync_logs: List, 
                                     date_str: str) -> Dict:
        """处理单日的角色用户统计"""
        day_stats = {
            "date": date_str,
            "total_kafka_message_count": len(kafka_messages),
            "total_sync_count": len(sync_logs),
            "total_kafka_users_affected": 0,
            "total_kafka_roles_affected": 0,
            "total_sync_users_affected": 0,
            "total_sync_roles_affected": 0,
        }
        
        # 处理Kafka消息
        for msg in kafka_messages:
            try:
                message_content = self._parse_kafka_json(msg.message_content)
                
                if msg.message_type in ['ADD_ROLE', 'DEL_ROLE']:
                    role_id = (message_content.get('roleName', '') or 
                             message_content.get('role_id', '') or 
                             message_content.get('roleId', ''))
                    if role_id:
                        day_stats["total_kafka_roles_affected"] += 1
                
                elif msg.message_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                    user_list = self._extract_user_list_from_message(message_content)
                    if user_list:
                        day_stats["total_kafka_users_affected"] += len(user_list)
                        
            except Exception as e:
                logger.error(f"解析消息内容失败: {str(e)}")
        
        # 处理同步日志
        for log in sync_logs:
            try:
                if log.message_type in ['ADD_ROLE', 'DEL_ROLE']:
                    if log.role_id:
                        day_stats["total_sync_roles_affected"] += 1
                
                elif log.message_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                    user_count = 0
                    
                    if log.add_user_list:
                        add_users = self._parse_kafka_json(log.add_user_list)
                        if isinstance(add_users, list):
                            user_count += len(add_users)
                    
                    if log.del_user_list:
                        del_users = self._parse_kafka_json(log.del_user_list)
                        if isinstance(del_users, list):
                            user_count += len(del_users)
                    
                    day_stats["total_sync_users_affected"] += user_count
                    
            except Exception as e:
                logger.error(f"解析同步日志失败: {str(e)}")
        
        # 计算成功率
        day_stats["total_success_rate"] = round(
            day_stats["total_sync_count"] / day_stats["total_kafka_message_count"] * 100, 2
        ) if day_stats["total_kafka_message_count"] > 0 else 0
        
        return day_stats

    def _extract_user_list_from_message(self, message_content: dict) -> Optional[List]:
        """从消息内容中提取用户列表"""
        field_names = ['userList', 'user_list', 'UserList', 'users', 'addUserList', 'delUserList']
        
        for field_name in field_names:
            if field_name in message_content and message_content[field_name]:
                user_list = message_content[field_name]
                
                if isinstance(user_list, list):
                    return user_list
                elif isinstance(user_list, str):
                    try:
                        parsed_list = self._parse_kafka_json(user_list)
                        if isinstance(parsed_list, list):
                            return parsed_list
                    except Exception:
                        continue
        
        return None

    async def get_comprehensive_statistics(self, start_date: Optional[str] = None, 
                                         end_date: Optional[str] = None, days: int = 7, 
                                         system_name: Optional[str] = None) -> Dict:
        """
        获取综合性的统计数据，包括文档接入、角色用户变更等
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            days: 汇总统计天数，默认为7天
            system_name: 系统名称，用于按系统进行数据筛选，默认为None（所有系统）
            
        Returns:
            Dict: 包含所有统计信息的综合性数据
        """
        await self._ensure_initialized()
        
        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"获取从{start_date}到{end_date}的综合性统计数据，系统名称: {system_name or '全部'}")
        
        result = {
            "日期范围": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
        if system_name:
            result["系统名称"] = system_name
        
        # 1. 获取每日数据接入统计
        daily_stats = await self.get_daily_statistics(start_date, end_date)
        
        result["文档接入统计"] = {
            "每日统计": daily_stats["daily_statistics"],
            "总接入量": sum(day["总接入量"] for day in daily_stats["daily_statistics"]),
            "总成功量": sum(day["成功接入量"] for day in daily_stats["daily_statistics"]),
            "平均成功率": round(
                sum(day["成功率"] for day in daily_stats["daily_statistics"]) / 
                len(daily_stats["daily_statistics"]), 2
            ) if daily_stats["daily_statistics"] else 0
        }
        
        # 2. 获取数据接入汇总统计
        summary_stats = await self.get_summary_statistics(days, start_date, end_date)
        
        # 如果指定了system_name，则过滤系统统计数据
        if system_name:
            system_statistics = [stat for stat in summary_stats["system_statistics"] 
                               if stat["system_name"] == system_name]
            total_incremental = sum(stat["incremental_count"] for stat in system_statistics)
            total_success = sum(stat["success_count"] for stat in system_statistics)
            success_rate = round(total_success / total_incremental * 100, 2) if total_incremental > 0 else 0
        else:
            system_statistics = summary_stats["system_statistics"]
            total_incremental = summary_stats["total_incremental"]
            total_success = summary_stats["total_success"]
            success_rate = summary_stats["success_rate"]
        
        result["接入系统统计"] = {
            "总接入量": total_incremental,
            "总成功量": total_success,
            "总成功率": success_rate,
            "系统明细": system_statistics
        }
        
        # 3. 获取角色和用户变更统计
        role_user_stats = await self.get_role_user_statistics(start_date, end_date)
        result["角色用户变更统计"] = {
            "每日统计": role_user_stats["每日统计"],
            "汇总统计": role_user_stats["汇总统计"]
        }
        
        return result

    async def get_system_daily_statistics(self) -> Dict:
        """
        获取每个系统当天的综合性数据统计
        使用四个专门的处理函数进行重构优化
        
        Returns:
            Dict: 包含每个系统当天综合性数据统计的字典
        """
        await self._ensure_initialized()



        # 计算当天的日期范围
        today = datetime.now()
        start_date = today.strftime('%Y-%m-%d')
        
        logger.info(f"获取{start_date}的每个系统综合性数据统计")
        
        start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{start_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
        
        # 缓存键 - 移除缓存以避免按系统区分的问题，每次都重新计算
        # cache_key = f"system_daily_stats_v2:{start_date}"
        
        # 暂时不使用缓存，确保每个系统的数据都是实时计算的
        # if self.cache_handler:
        #     cached_result = await self.cache_handler.get(cache_key)
        #     if cached_result:
        #         logger.info(f"从缓存获取系统每日统计数据: {start_date}")
        #         return cached_result
        
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
            
            # 初始化总和统计数据
            total_stats = self._init_total_stats()
            
            # 对每个系统进行统计
            for system_name in system_names:
                logger.info(f"处理系统: {system_name}")
                
                system_stats = await self._get_system_comprehensive_stats(
                    db, system_name, start_datetime, end_datetime
                )
                
                # 累计到总统计
                self._accumulate_total_stats(total_stats, system_stats)
                
                # 添加到结果
                result["系统统计"].append(system_stats)
            
            # 添加总和统计到结果
            result["总计"] = total_stats
            
            # 暂时不缓存结果，确保数据实时性
            # if self.cache_handler:
            #     await self.cache_handler.set(cache_key, result, expiry=3600)
            
            logger.info(f"系统每日统计数据处理完成，共处理 {len(system_names)} 个系统")
            return result

    async def get_unmatched_file_not_change_numbers(self, start_date: Optional[str] = None, 
                                                 end_date: Optional[str] = None) -> Dict[str, List[str]]:
        """
        查询在指定日期范围内，只存在于FILE_NOT_CHANGE消息中，但不存在于同一系统FILE_ADD消息中的file_number，并按系统返回。
        
        Args:
            start_date: 开始日期，格式: YYYY-MM-DD，默认为7天前
            end_date: 结束日期，格式: YYYY-MM-DD，默认为今天
            
        Returns:
            Dict[str, List[str]]: 一个字典，键是系统名称，值是只在FILE_NOT_CHANGE中出现的file_number列表。
        """
        await self._ensure_initialized()

        # 设置默认日期范围
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"查询从 {start_date} 到 {end_date} 范围内 FILE_NOT_CHANGE 与 FILE_ADD 的差异 file_number (按系统匹配)")

        start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")

        with get_db() as db:
            # 1. 获取指定日期范围内所有 FILE_ADD 消息中的唯一 (system_name, file_number) 对
            file_add_pairs_query = db.query(
                KafkaMessageLog.system_name,
                KafkaMessageLog.file_number
            ).filter(
                and_(
                    KafkaMessageLog.message_type == 'FILE_ADD',
                    KafkaMessageLog.created_at.between(start_datetime, end_datetime),
                    KafkaMessageLog.file_number.isnot(None),
                    KafkaMessageLog.system_name.isnot(None)
                )
            ).distinct()
            file_add_pairs = {(row.system_name, row.file_number) for row in file_add_pairs_query}
            logger.debug(f"找到 {len(file_add_pairs)} 个 FILE_ADD 的 (system_name, file_number) 对")

            # 2. 获取指定日期范围内所有 FILE_NOT_CHANGE 消息中的 (system_name, file_number) 对
            file_not_change_query = db.query(
                KafkaMessageLog.system_name,
                KafkaMessageLog.file_number
            ).filter(
                and_(
                    KafkaMessageLog.message_type == 'FILE_NOT_CHANGE',
                    KafkaMessageLog.created_at.between(start_datetime, end_datetime),
                    KafkaMessageLog.file_number.isnot(None),
                    KafkaMessageLog.system_name.isnot(None)
                )
            ).distinct()

            # 3. 找出差异并按系统分组
            unmatched_files_by_system = {}
            for record in file_not_change_query.all():
                system_name = record.system_name
                file_number = record.file_number

                # 检查 (system_name, file_number) 对是否存在于 FILE_ADD 集合中
                if (system_name, file_number) not in file_add_pairs:
                    if system_name not in unmatched_files_by_system:
                        unmatched_files_by_system[system_name] = []
                    # 避免重复添加
                    if file_number not in unmatched_files_by_system[system_name]:
                        unmatched_files_by_system[system_name].append(file_number)

            logger.info(f"差异查询完成，找到 {sum(len(v) for v in unmatched_files_by_system.values())} 个不匹配的 file_number")
            return unmatched_files_by_system

    def _init_total_stats(self) -> Dict:
        """初始化总统计数据结构"""
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

    async def _get_system_comprehensive_stats(self, db: Session, system_name: str, 
                                      start_datetime: datetime, end_datetime: datetime) -> Dict:
        """获取单个系统的综合统计数据"""
        # 1. 获取FILE_ADD统计
        file_add_stats = self._get_file_add_statistics_optimized(
            db, system_name, start_datetime, end_datetime
        )
        
        # 2. 获取FILE_DEL统计（使用Redis统计）
        file_del_stats = await self._get_file_del_statistics_optimized(
            db, system_name, start_datetime, end_datetime
        )
        
        # 3. 获取FILE_NOT_CHANGE统计
        file_not_change_stats = self._get_file_not_change_statistics_optimized(
            db, system_name, start_datetime, end_datetime
        )
        
        # 4. 获取角色操作统计
        role_ops_stats = self._get_role_operations_statistics_optimized(
            db, system_name, start_datetime, end_datetime
        )
        
        # 构建系统统计数据
        return {
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
                    "删除用户数": file_not_change_stats["kafka_users"]["del_count"],
                },
                "角色用户变更": {
                    "添加用户数": role_ops_stats["kafka_users"].get("ROLE_ADD_USER", 0),
                    "删除用户数": role_ops_stats["kafka_users"].get("ROLE_DEL_USER", 0),
                }
            }
        }

    def _accumulate_total_stats(self, total_stats: Dict, system_stats: Dict):
        """累计系统统计到总统计"""
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

    def _get_file_add_statistics_optimized(self, db: Session, system_name: str, 
                                         start_datetime: datetime, end_datetime: datetime) -> Dict:
        """优化版：获取FILE_ADD类型消息的统计信息"""
        # 使用单次查询获取消息数量
        file_add_msg_count = db.query(func.count(KafkaMessageLog.id)).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_ADD',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).scalar() or 0
        
        # 获取文件新增和更新统计
        incremental_stats = self._get_system_incremental_stats_for_period(
            db, system_name, start_datetime, end_datetime
        )
        
        return {
            "kafka_message_count": file_add_msg_count,
            "add_count": incremental_stats["add_count"],
            "update_count": incremental_stats["update_count"],
            "total_operations": incremental_stats["add_count"] + incremental_stats["update_count"]
        }

    async def _get_file_del_statistics_optimized(self, db: Session, system_name: str, 
                                         start_datetime: datetime, end_datetime: datetime) -> Dict:
        """优化版：获取FILE_DEL类型消息的统计信息（使用按系统区分的Redis统计键）"""
        try:
            await self._ensure_initialized()
            
            # 按日期获取Redis统计数据
            current_date = start_datetime
            total_verification_messages = 0  # 验证通过数量
            total_success_messages = 0       # 删除成功数量
            
            while current_date <= end_datetime:
                date_str = current_date.strftime('%Y-%m-%d')
                
                if self.cache_handler:
                    # 1. 获取验证通过统计（使用新的按系统区分的格式）
                    verification_key = f"{system_name}:knowledge:delete:verification:passed:{date_str}"
                    daily_verification = await self.cache_handler.get_hash_field(verification_key, "message_count") or 0
                    total_verification_messages += int(daily_verification)
                    
                    # 2. 获取删除成功统计
                    success_key = f"{system_name}:knowledge:delete:operation:success:{date_str}"
                    daily_success = await self.cache_handler.get_hash_field(success_key, "message_count") or 0
                    total_success_messages += int(daily_success)
                
                current_date += timedelta(days=1)
            
            # 如果没有按日期的统计，尝试获取总计数（旧格式，兼容性）
            if total_verification_messages == 0 and total_success_messages == 0 and self.cache_handler:
                # 尝试旧格式（全局统计）
                verification_hash_key = "knowledge:delete:verification:passed"
                total_verification_messages = int(await self.cache_handler.get_hash_field(verification_hash_key, "message_count") or 0)
                
                success_hash_key = "knowledge:delete:operation:success"
                total_success_messages = int(await self.cache_handler.get_hash_field(success_hash_key, "message_count") or 0)
            
            logger.info(f"从Redis获取FILE_DEL统计 - 系统: {system_name}, 验证通过消息数: {total_verification_messages}, 删除成功消息数: {total_success_messages}")
            
            # 如果Redis中有数据，优先使用Redis统计
            if total_verification_messages > 0 or total_success_messages > 0:
                return {
                    "kafka_message_count": total_verification_messages,  # 使用验证通过的消息数
                    "delete_operations": total_success_messages         # 使用删除成功的消息数
                }
            
            # 如果Redis中没有数据，fallback到数据库查询
            logger.info("Redis中暂无FILE_DEL统计数据，使用数据库fallback")
            return await self._get_file_del_statistics_fallback(db, system_name, start_datetime, end_datetime)
            
        except Exception as e:
            logger.error(f"获取FILE_DEL Redis统计失败: {str(e)}，fallback到数据库查询")
            return await self._get_file_del_statistics_fallback(db, system_name, start_datetime, end_datetime)
    
    async def _get_file_del_statistics_fallback(self, db: Session, system_name: str, 
                                               start_datetime: datetime, end_datetime: datetime) -> Dict:
        """FILE_DEL统计的数据库fallback方法"""
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

    def _get_file_not_change_statistics_optimized(self, db: Session, system_name: str, 
                                                start_datetime: datetime, end_datetime: datetime) -> Dict:
        """优化版：获取FILE_NOT_CHANGE类型消息的统计信息"""
        # 统计Kafka消息数量
        file_not_change_msg_count = db.query(func.count(KafkaMessageLog.id)).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_NOT_CHANGE',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).scalar() or 0
        
        # 批量获取消息内容进行解析
        file_not_change_msgs = db.query(KafkaMessageLog.message_content).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_NOT_CHANGE',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        kafka_add_user_count, kafka_del_user_count = self._parse_file_not_change_users(file_not_change_msgs)
        
        # 统计同步记录数量
        file_not_change_sync_count = db.query(func.count(KnowledgeRoleSyncLog.id)).filter(
            and_(
                KnowledgeRoleSyncLog.message_type == 'FILE_NOT_CHANGE',
                KnowledgeRoleSyncLog.system_name == system_name,
                KnowledgeRoleSyncLog.created_at.between(start_datetime, end_datetime)
            )
        ).scalar() or 0
        
        # 获取同步表中的统计
        sync_stats = self._get_file_not_change_sync_stats(db, system_name, start_datetime, end_datetime)
        
        return {
            "kafka_message_count": file_not_change_msg_count,
            "sync_record_count": file_not_change_sync_count,
            "kafka_users": {
                "add_count": kafka_add_user_count,
                "del_count": kafka_del_user_count
            },
            "sync_roles": sync_stats["roles"],
            "sync_users": sync_stats["users"]
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
            for field, counter in [
                (log.add_role_list, lambda x: sync_add_role_count.__iadd__(x)),
                (log.del_role_list, lambda x: sync_del_role_count.__iadd__(x)),
                (log.add_user_list, lambda x: sync_add_user_count.__iadd__(x)),
                (log.del_user_list, lambda x: sync_del_user_count.__iadd__(x))
            ]:
                if field:
                    try:
                        parsed_list = self._parse_kafka_json(field)
                        if isinstance(parsed_list, list):
                            if field == log.add_role_list:
                                sync_add_role_count += len(parsed_list)
                            elif field == log.del_role_list:
                                sync_del_role_count += len(parsed_list)
                            elif field == log.add_user_list:
                                sync_add_user_count += len(parsed_list)
                            elif field == log.del_user_list:
                                sync_del_user_count += len(parsed_list)
                    except Exception as e:
                        logger.error(f"解析同步列表失败: {str(e)}")
        
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

    def _get_role_operations_statistics_optimized(self, db: Session, system_name: str, 
                                                start_datetime: datetime, end_datetime: datetime) -> Dict:
        """优化版：获取角色相关操作统计信息"""
        role_message_types = ['ADD_ROLE', 'DEL_ROLE', 'ROLE_ADD_USER', 'ROLE_DEL_USER']
        
        # 批量查询Kafka消息
        kafka_msgs = db.query(
            KafkaMessageLog.message_type,
            KafkaMessageLog.message_content
        ).filter(
            and_(
                KafkaMessageLog.message_type.in_(role_message_types),
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        # 批量查询同步日志
        sync_logs = db.query(
            VirtualGroupSyncLog.message_type,
            VirtualGroupSyncLog.role_id,
            VirtualGroupSyncLog.add_user_list,
            VirtualGroupSyncLog.del_user_list
        ).filter(
            and_(
                VirtualGroupSyncLog.message_type.in_(role_message_types),
                VirtualGroupSyncLog.system_name == system_name,
                VirtualGroupSyncLog.created_at.between(start_datetime, end_datetime)
            )
        ).all()
        
        return self._process_role_operations_data(kafka_msgs, sync_logs)

    def _process_role_operations_data(self, kafka_msgs: List, sync_logs: List) -> Dict:
        """处理角色操作数据"""
        result = {
            "kafka_messages": {},
            "sync_operations": {},
            "kafka_users": {},
            "sync_users": {}
        }
        
        # 按消息类型分组处理
        kafka_by_type = {}
        sync_by_type = {}
        
        for msg in kafka_msgs:
            msg_type = msg.message_type
            if msg_type not in kafka_by_type:
                kafka_by_type[msg_type] = []
            kafka_by_type[msg_type].append(msg)
        
        for log in sync_logs:
            msg_type = log.message_type
            if msg_type not in sync_by_type:
                sync_by_type[msg_type] = []
            sync_by_type[msg_type].append(log)
        
        # 处理各种消息类型
        for msg_type in ['ADD_ROLE', 'DEL_ROLE', 'ROLE_ADD_USER', 'ROLE_DEL_USER']:
            kafka_count = len(kafka_by_type.get(msg_type, []))
            sync_count = len(sync_by_type.get(msg_type, []))
            
            result["kafka_messages"][msg_type] = kafka_count
            result["sync_operations"][msg_type] = sync_count
            
            # 处理用户数量统计
            if msg_type in ['ROLE_ADD_USER', 'ROLE_DEL_USER']:
                kafka_user_count = self._count_users_in_kafka_messages(kafka_by_type.get(msg_type, []))
                sync_user_count = self._count_users_in_sync_logs(sync_by_type.get(msg_type, []), msg_type)
                
                result["kafka_users"][msg_type] = kafka_user_count
                result["sync_users"][msg_type] = sync_user_count
            elif msg_type == 'ADD_ROLE':
                # ADD_ROLE时统计创建角色的用户数
                sync_user_count = self._count_users_in_sync_logs(sync_by_type.get(msg_type, []), msg_type)
                result["sync_users"][msg_type] = sync_user_count
        
        return result

    def _count_users_in_kafka_messages(self, kafka_msgs: List) -> int:
        """统计Kafka消息中的用户数量"""
        total_users = 0
        
        for msg in kafka_msgs:
            try:
                message_content = self._parse_kafka_json(msg.message_content)
                user_list = self._extract_user_list_from_message(message_content)
                if user_list:
                    total_users += len(user_list)
            except Exception as e:
                logger.error(f"统计Kafka消息用户数失败: {str(e)}")
        
        return total_users

    def _count_users_in_sync_logs(self, sync_logs: List, msg_type: str) -> int:
        """统计同步日志中的用户数量"""
        total_users = 0
        
        for log in sync_logs:
            try:
                if msg_type in ['ROLE_ADD_USER', 'ADD_ROLE'] and log.add_user_list:
                    add_users = self._parse_kafka_json(log.add_user_list)
                    if isinstance(add_users, list):
                        total_users += len(add_users)
                
                if msg_type == 'ROLE_DEL_USER' and log.del_user_list:
                    del_users = self._parse_kafka_json(log.del_user_list)
                    if isinstance(del_users, list):
                        total_users += len(del_users)
                        
            except Exception as e:
                logger.error(f"统计同步日志用户数失败: {str(e)}")
        
        return total_users

    def _get_system_incremental_stats_for_period(self, db: Session, system_name: str, 
                                               start_date: datetime, end_date: datetime) -> Dict:
        """获取指定系统和时间段的新增更新统计"""
        # 获取FILE_ADD消息记录
        file_add_records = db.query(
            KafkaMessageLog.message_content,
            KafkaMessageLog.file_number,
            KafkaMessageLog.created_at
        ).filter(
            and_(
                KafkaMessageLog.message_type == 'FILE_ADD',
                KafkaMessageLog.system_name == system_name,
                KafkaMessageLog.created_at.between(start_date, end_date)
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
        
        if not unique_file_numbers:
            return {"add_count": 0, "update_count": 0}
        
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
        
        add_count = 0
        update_count = 0
        
        for file_number in unique_file_numbers:
            kafka_log_info = file_number_to_log.get(file_number)
            if not kafka_log_info:
                continue
                
            doc = doc_map.get(file_number)
            if doc:
                if doc.created_at < kafka_log_info['created_at']:
                    update_count += 1
                elif (start_date.replace(tzinfo=None) <= doc.created_at.replace(tzinfo=None) <= 
                      end_date.replace(tzinfo=None)):
                    add_count += 1
        
        return {"add_count": add_count, "update_count": update_count}

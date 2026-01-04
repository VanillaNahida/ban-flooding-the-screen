import asyncio
import json
import os
import re
from typing import Dict, Any, Optional

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register


@register(
    "ban_flooding_the_screen",
    "香草味的纳西妲喵（VanillaNahida）",
    "刷屏禁言插件",
    "1.0.0"
)
class BanFloodingTheScreenPlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.context = context
        self.config = config or {}
        
        # 刷屏状态管理: { "gid:uid": {"timer": asyncio.Task, "messages": [], "delete": callable} }
        self.flood_states: Dict[str, Dict[str, Any]] = {}
        
        # 累计触发次数管理: { "gid:uid": count }
        self.offense_counts: Dict[str, int] = {}
        
        # 从配置文件 schema 读取默认值
        schema_path = os.path.join(os.path.dirname(__file__), "_conf_schema.json")
        schema_defaults = {}
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
                for key, value in schema.items():
                    schema_defaults[key] = value.get("default")
        except Exception as e:
            logger.warning(f"[刷屏禁言] 读取配置 schema 失败: {e}")
        
        # 从配置文件读取配置，如果不存在则使用 schema 中的默认值
        try:
            self.enabled_groups = self.config.get("enabled_groups", schema_defaults.get("enabled_groups", []))
            self.detection_period = self.config.get("detection_period", schema_defaults.get("detection_period", 4))
            self.message_threshold = self.config.get("message_threshold", schema_defaults.get("message_threshold", 4))
            self.mute_time = self.config.get("mute_time", schema_defaults.get("mute_time", 10))
            self.mute_message = self.config.get("mute_message", schema_defaults.get("mute_message", "检测到刷屏，已自动禁言，如有异议请联系管理员"))
            self.enable_kick_repeat_offender = self.config.get("enable_kick_repeat_offender", schema_defaults.get("enable_kick_repeat_offender", True))
            self.kick_threshold = self.config.get("kick_threshold", schema_defaults.get("kick_threshold", 5))
            self.kick_message = self.config.get("kick_message", schema_defaults.get("kick_message", "{at_user} 你已累计触发刷屏禁言 {count} 次，已被请出本群。"))
            self.kick_delay = self.config.get("kick_delay", schema_defaults.get("kick_delay", 3))
            
            # 群级别配置
            self.group_configs = self.config.get("group_configs", {})
        except Exception:
            self.enabled_groups = schema_defaults.get("enabled_groups", [])
            self.detection_period = schema_defaults.get("detection_period", 4)
            self.message_threshold = schema_defaults.get("message_threshold", 4)
            self.mute_time = schema_defaults.get("mute_time", 10)
            self.mute_message = schema_defaults.get("mute_message", "检测到刷屏，已自动禁言，如有异议请联系管理员")
            self.enable_kick_repeat_offender = schema_defaults.get("enable_kick_repeat_offender", True)
            self.kick_threshold = schema_defaults.get("kick_threshold", 5)
            self.kick_message = schema_defaults.get("kick_message", "{at_user} 你已累计触发刷屏禁言 {count} 次，已被请出本群。")
            self.kick_delay = schema_defaults.get("kick_delay", 3)
            self.group_configs = {}

    def _save_config(self):
        """保存配置到磁盘"""
        try:
            self.config["enabled_groups"] = self.enabled_groups
            self.config["detection_period"] = self.detection_period
            self.config["message_threshold"] = self.message_threshold
            self.config["mute_time"] = self.mute_time
            self.config["mute_message"] = self.mute_message
            self.config["enable_kick_repeat_offender"] = self.enable_kick_repeat_offender
            self.config["kick_threshold"] = self.kick_threshold
            self.config["kick_message"] = self.kick_message
            self.config["kick_delay"] = self.kick_delay
            self.config["group_configs"] = self.group_configs
            
            logger.info("[刷屏禁言] 配置已更新到内存")
        except Exception as e:
            logger.error(f"[刷屏禁言] 更新配置失败: {e}")

    def _parse_time_string(self, time_str: str) -> Optional[int]:
        """解析时间字符串，返回分钟数
        
        支持格式：
        - 1分, 1分钟 -> 1分钟
        - 1小时 -> 60分钟
        - 1天 -> 1440分钟
        - 1h, 1H -> 60分钟
        - 1m, 1M -> 1分钟
        - 1d, 1D -> 1440分钟
        - 1s, 1S -> 0分钟（秒不支持，返回0）
        - 纯数字 -> 视为分钟
        """
        time_str = time_str.strip().lower()
        
        # 匹配数字+单位格式
        match = re.match(r'^(\d+)\s*([天小时分秒天hmds]+)?$', time_str)
        if not match:
            return None
        
        num = int(match.group(1))
        unit = match.group(2) or ""
        
        if not unit or unit in ["分", "分钟", "m"]:
            return num
        elif unit in ["小时", "h"]:
            return num * 60
        elif unit in ["天", "d"]:
            return num * 1440
        elif unit in ["秒", "s"]:
            return 0
        
        return None

    async def _check_permission(self, event: AstrMessageEvent) -> tuple[bool, str]:
        """检查用户权限和机器人权限
        
        返回:
            (True, ""): 有权限
            (False, "bot权限不足，需要管理员权限"): 机器人权限不足
            (False, "该命令仅限管理员使用"): 用户权限不足
        """
        raw = event.message_obj.raw_message
        gid = raw.get("group_id")
        uid = str(raw.get("user_id"))
        
        # 检查机器人权限
        try:
            bot_info = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=int(event.get_self_id()))
            bot_role = bot_info.get("role")
            if bot_role not in ["admin", "owner"]:
                return (False, "bot权限不足，需要管理员权限")
        except Exception as e:
            logger.error(f"[刷屏禁言] 检查机器人权限失败: {e}")
            return (False, "bot权限不足，需要管理员权限")
        
        # 检查用户权限
        try:
            user_info = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=int(uid))
            user_role = user_info.get("role")
            if user_role not in ["admin", "owner"]:
                return (False, "该命令仅限管理员使用")
        except Exception as e:
            logger.error(f"[刷屏禁言] 检查用户权限失败: {e}")
            return (False, "该命令仅限管理员使用")
        
        return (True, "")

    def _get_group_config(self, gid: int) -> Dict[str, Any]:
        """获取群级别配置"""
        gid_str = str(gid)
        if gid_str not in self.group_configs:
            self.group_configs[gid_str] = {
                "enabled": False,
                "mute_time": self.mute_time,
                "enable_kick": self.enable_kick_repeat_offender,
                "kick_threshold": self.kick_threshold,
                "kick_delay": self.kick_delay
            }
        return self.group_configs[gid_str]

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_group_message(self, event: AstrMessageEvent):
        """处理群消息，检测刷屏"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        gid = raw.get("group_id")
        uid = str(raw.get("user_id"))
        
        # 获取群级别配置
        config = self._get_group_config(gid)
        
        # 检查群是否启用了刷屏检测
        if not config.get("enabled", False):
            return
        
        # 获取用户的刷屏状态
        state_key = f"{gid}:{uid}"
        flood_state = self._get_flood_state(state_key)
        
        # 添加消息到列表
        flood_state["messages"].append(event.message_str)
        
        # 检查是否达到阈值
        if len(flood_state["messages"]) >= self.message_threshold:
            await self._handle_flooding(event, gid, uid, state_key, config)
        else:
            # 如果没有达到阈值，设置定时器
            if not flood_state["timer"] or flood_state["timer"].cancelled():
                flood_state["timer"] = asyncio.create_task(self._reset_flood_state(state_key))

    async def _handle_flooding(self, event: AstrMessageEvent, gid: int, uid: str, state_key: str, config: Dict[str, Any]):
        """处理刷屏事件"""
        # 取消定时器
        flood_state = self.flood_states.get(state_key)
        if flood_state and flood_state.get("timer") and not flood_state["timer"].cancelled():
            flood_state["timer"].cancel()
        
        # 检查机器人是否有权限
        try:
            group_info = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=int(event.get_self_id()))
            bot_role = group_info.get("role")
            if bot_role not in ["admin", "owner"]:
                logger.warning(f"[刷屏禁言] 机器人在群 {gid} 没有管理员权限，无法禁言")
                flood_state["delete"]()
                return
        except Exception as e:
            logger.error(f"[刷屏禁言] 检查机器人权限失败: {e}")
            flood_state["delete"]()
            return
        
        # 检查用户角色
        try:
            member_info = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=int(uid))
            user_role = member_info.get("role")
            if user_role == "owner":
                logger.info(f"[刷屏禁言] 用户 {uid} 是群主，跳过禁言")
                flood_state["delete"]()
                return
            if user_role == "admin" and bot_role != "owner":
                logger.info(f"[刷屏禁言] 用户 {uid} 是管理员，机器人不是群主，跳过禁言")
                flood_state["delete"]()
                return
        except Exception as e:
            logger.error(f"[刷屏禁言] 检查用户角色失败: {e}")
            flood_state["delete"]()
            return
        
        # 获取群级别配置的禁言时间
        mute_time = config.get("mute_time", self.mute_time)
        
        # 执行禁言
        try:
            await event.bot.api.call_action(
                "set_group_ban",
                group_id=gid,
                user_id=int(uid),
                duration=mute_time * 60
            )
            logger.info(f"[刷屏禁言] 已禁言用户 {uid}，时长 {mute_time} 分钟")
        except Exception as e:
            logger.error(f"[刷屏禁言] 禁言失败: {e}")
            flood_state["delete"]()
            return
        
        # 发送禁言消息
        if self.mute_message:
            try:
                at_user = f"[CQ:at,qq={uid}]"
                nickname = member_info.get("card") or member_info.get("nickname") or uid
                message = self.mute_message.format(
                    at_user=at_user,
                    nickname=nickname,
                    mute_time=mute_time
                )
                
                # 如果启用了累计踢人，添加累计次数提示
                enable_kick = config.get("enable_kick", self.enable_kick_repeat_offender)
                kick_threshold = config.get("kick_threshold", self.kick_threshold)
                if enable_kick:
                    message += f"\n\n你已触犯 {offense_count} 次，如果次数达到 {kick_threshold} 次，你会被移出群。"
                
                await event.bot.api.call_action("send_group_msg", group_id=gid, message=message)
            except Exception as e:
                logger.error(f"[刷屏禁言] 发送禁言消息失败: {e}")
        
        # 更新累计触发次数
        offense_count = self.offense_counts.get(state_key, 0) + 1
        self.offense_counts[state_key] = offense_count
        
        # 持久化存储累计次数
        try:
            await self.put_kv_data(state_key, offense_count)
        except Exception as e:
            logger.error(f"[刷屏禁言] 存储累计次数失败: {e}")
        
        # 检查是否需要踢人
        enable_kick = config.get("enable_kick", self.enable_kick_repeat_offender)
        kick_threshold = config.get("kick_threshold", self.kick_threshold)
        kick_delay = config.get("kick_delay", self.kick_delay)
        if enable_kick and offense_count >= kick_threshold:
            await self._kick_user(event, gid, uid, offense_count, kick_delay)
        
        # 清除刷屏状态
        flood_state["delete"]()

    async def _kick_user(self, event: AstrMessageEvent, gid: int, uid: str, count: int, kick_delay: int = None):
        """踢出屡犯用户"""
        if kick_delay is None:
            kick_delay = self.kick_delay
        
        try:
            # 发送踢人消息
            if self.kick_message:
                at_user = f"[CQ:at,qq={uid}]"
                message = self.kick_message.format(at_user=at_user, count=count)
                await event.bot.api.call_action("send_group_msg", group_id=gid, message=message)
            
            # 延迟踢人
            await asyncio.sleep(kick_delay)
            
            # 执行踢人
            await event.bot.api.call_action(
                "set_group_kick",
                group_id=gid,
                user_id=int(uid),
                reject_add_request=False
            )
            
            logger.info(f"[刷屏禁言] 已踢出用户 {uid}，累计触发 {count} 次")
            
            # 清除累计次数
            state_key = f"{gid}:{uid}"
            self.offense_counts.pop(state_key, None)
            try:
                await self.delete_kv_data(state_key)
            except Exception as e:
                logger.error(f"[刷屏禁言] 删除累计次数失败: {e}")
        except Exception as e:
            logger.error(f"[刷屏禁言] 踢人失败: {e}")

    async def _reset_flood_state(self, state_key: str):
        """重置刷屏状态"""
        await asyncio.sleep(self.detection_period)
        flood_state = self.flood_states.get(state_key)
        if flood_state:
            flood_state["delete"]()

    def _get_flood_state(self, state_key: str) -> Dict[str, Any]:
        """获取或创建用户的刷屏状态"""
        if state_key not in self.flood_states:
            self.flood_states[state_key] = {
                "timer": None,
                "messages": [],
                "delete": lambda: self.flood_states.pop(state_key, None)
            }
        return self.flood_states[state_key]

    @filter.command("开启刷屏禁言")
    async def enable_ban(self, event: AstrMessageEvent):
        """开启刷屏禁言功能"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        # 检查权限
        has_permission, error_msg = await self._check_permission(event)
        if not has_permission:
            yield event.plain_result(error_msg)
            return

        gid = raw.get("group_id")
        config = self._get_group_config(gid)
        
        # 检查是否已经开启
        if config.get("enabled", False):
            yield event.plain_result("已经开启啦")
            return
        
        config["enabled"] = True
        self._save_config()

        logger.info(f"[刷屏禁言] 群 {gid} 已开启刷屏禁言")
        yield event.plain_result("已开启刷屏禁言功能")

    @filter.command("关闭刷屏禁言")
    async def disable_ban(self, event: AstrMessageEvent):
        """关闭刷屏禁言功能"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        # 检查权限
        has_permission, error_msg = await self._check_permission(event)
        if not has_permission:
            yield event.plain_result(error_msg)
            return

        gid = raw.get("group_id")
        config = self._get_group_config(gid)
        config["enabled"] = False
        self._save_config()

        logger.info(f"[刷屏禁言] 群 {gid} 已关闭刷屏禁言")
        yield event.plain_result("已关闭刷屏禁言功能")

    @filter.command("设置刷屏禁言时间")
    async def set_mute_time(self, event: AstrMessageEvent):
        """设置刷屏禁言时间"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        # 检查权限
        has_permission, error_msg = await self._check_permission(event)
        if not has_permission:
            yield event.plain_result(error_msg)
            return

        # 提取时间参数
        message_str = event.message_str.strip()
        # 移除命令部分
        time_str = message_str.replace("设置刷屏禁言时间", "").strip()
        
        if not time_str:
            yield event.plain_result("请指定禁言时间，例如：/设置刷屏禁言时间 10分钟")
            return

        # 解析时间
        mute_time = self._parse_time_string(time_str)
        if mute_time is None:
            yield event.plain_result("时间格式错误，支持格式：1分、1分钟、1小时、1天、1h、1m、1d")
            return

        if mute_time == 0:
            yield event.plain_result("秒单位不支持，请使用分钟、小时或天")
            return

        gid = raw.get("group_id")
        config = self._get_group_config(gid)
        config["mute_time"] = mute_time
        self._save_config()

        logger.info(f"[刷屏禁言] 群 {gid} 禁言时间已设置为 {mute_time} 分钟")
        yield event.plain_result(f"禁言时间已设置为 {mute_time} 分钟")

    @filter.command("开启刷屏踢人")
    async def enable_kick(self, event: AstrMessageEvent):
        """开启刷屏踢人功能"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        # 检查权限
        has_permission, error_msg = await self._check_permission(event)
        if not has_permission:
            yield event.plain_result(error_msg)
            return

        gid = raw.get("group_id")
        config = self._get_group_config(gid)
        config["enable_kick"] = True
        self._save_config()

        logger.info(f"[刷屏禁言] 群 {gid} 已开启刷屏踢人")
        yield event.plain_result("已开启刷屏踢人功能")

    @filter.command("关闭刷屏踢人")
    async def disable_kick(self, event: AstrMessageEvent):
        """关闭刷屏踢人功能"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        # 检查权限
        has_permission, error_msg = await self._check_permission(event)
        if not has_permission:
            yield event.plain_result(error_msg)
            return

        gid = raw.get("group_id")
        config = self._get_group_config(gid)
        
        # 检查是否已经关闭
        if not config.get("enable_kick", False):
            yield event.plain_result("已经关闭啦")
            return
        
        config["enable_kick"] = False
        self._save_config()

        logger.info(f"[刷屏禁言] 群 {gid} 已关闭刷屏踢人")
        yield event.plain_result("已关闭刷屏踢人功能")

    @filter.command("设置刷屏踢人次数")
    async def set_kick_threshold(self, event: AstrMessageEvent):
        """设置刷屏踢人次数"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        # 检查权限
        has_permission, error_msg = await self._check_permission(event)
        if not has_permission:
            yield event.plain_result(error_msg)
            return

        # 提取次数参数
        message_str = event.message_str.strip()
        # 移除命令部分
        count_str = message_str.replace("设置刷屏踢人次数", "").strip()
        
        if not count_str:
            yield event.plain_result("请指定踢人次数，例如：/设置刷屏踢人次数 5")
            return

        # 解析次数
        try:
            count = int(count_str)
            if count < 1:
                yield event.plain_result("踢人次数必须大于0")
                return
        except ValueError:
            yield event.plain_result("次数格式错误，请输入数字")
            return

        gid = raw.get("group_id")
        config = self._get_group_config(gid)
        config["kick_threshold"] = count
        self._save_config()

        logger.info(f"[刷屏禁言] 群 {gid} 踢人次数已设置为 {count} 次")
        yield event.plain_result(f"踢人次数已设置为 {count} 次")

    @filter.command("重置刷屏次数")
    async def reset_offense_count(self, event: AstrMessageEvent):
        """重置用户的刷屏累计次数"""
        if event.get_platform_name() != "aiocqhttp":
            return

        raw = event.message_obj.raw_message
        if raw.get("post_type") != "message" or raw.get("message_type") != "group":
            return

        # 检查权限
        has_permission, error_msg = await self._check_permission(event)
        if not has_permission:
            yield event.plain_result(error_msg)
            return

        # 解析消息中的@用户 - 使用正则表达式从原始消息中提取
        message_raw = event.message_obj.raw_message.get("message", "")
        target_uid = None
        
        # 匹配 [CQ:at,qq=数字] 格式
        match = re.search(r'\[CQ:at,qq=(\d+)\]', message_raw)
        if match:
            target_uid = match.group(1)
        
        if not target_uid:
            yield event.plain_result("请@要重置次数的用户，例如：/重置刷屏次数 @用户")
            return

        gid = raw.get("group_id")
        state_key = f"{gid}:{target_uid}"
        
        # 清除内存中的累计次数
        old_count = self.offense_counts.pop(state_key, 0)
        
        # 清除持久化存储
        try:
            await self.delete_kv_data(state_key)
        except Exception as e:
            logger.error(f"[刷屏禁言] 删除累计次数失败: {e}")
        
        logger.info(f"[刷屏禁言] 已重置用户 {target_uid} 的刷屏累计次数（原次数: {old_count}）")
        
        # 获取用户信息
        try:
            member_info = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=int(target_uid))
            nickname = member_info.get("card") or member_info.get("nickname") or target_uid
            yield event.plain_result(f"已重置用户 {nickname}({target_uid}) 的刷屏累计次数（原次数: {old_count}）")
        except Exception:
            yield event.plain_result(f"已重置用户 {target_uid} 的刷屏累计次数（原次数: {old_count}）")

"""
统一订阅推送中心 - AstrBot 插件

把多个订阅源统一管理，按设定间隔/时间定时主动推送到群聊 / 会话：
- 热搜榜单：微博热搜、百度热搜、腾讯新闻热榜、B 站热门
- 信息列表：RSS/Atom 订阅、Steam 特惠、今日更新番剧
支持命令：
- /订阅 <源> [参数]         添加订阅
- /订阅 rss <链接>          订阅任意 RSS/Atom
- /退订 <编号>              移除订阅
- /我的订阅                 查看已订阅列表
- /查看 <源>                立即手动抓取并渲染推送
- /订阅间隔 HH:MM|分钟|off   设置定时（管理员）
- /推送暂停|/推送恢复        群级暂停/恢复自动推送
- /订阅帮助                 查看帮助

工程惯例：
- 持久化存放在 AstrBot data 目录的插件子目录 subs.json（原子写 + 线程锁）
- 全程 try/except，任何异常只记录日志，绝不让 AstrBot 崩溃
- 使用 astrbot 的 logger 记录日志
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import MessageChain, filter, AstrMessageEvent
from astrbot.api.message_components import Image
from astrbot.api.star import Context, Star, register

PermissionType = filter.PermissionType

from . import fetchers
from .utils import render_image

# 订阅类型：board=热搜榜单（weibo/baidu/tencent/bili），list=信息列表（rss/steam/bangumi）
# 注册表驱动：新增源在此登记即可，命令/推送/渲染自动适配。
_SOURCES = {
    "weibo": {
        "label": "微博热搜", "aliases": ["weibo", "微博", "微博热搜", "热搜"],
        "needs_payload": False, "emoji": "🔥", "render_kind": "weibo",
        "title": "微博热搜",
    },
    "baidu": {
        "label": "百度热搜", "aliases": ["baidu", "百度"],
        "needs_payload": False, "emoji": "🍃", "render_kind": "baidu",
        "title": "百度热搜",
    },
    "tencent": {
        "label": "腾讯新闻热榜", "aliases": ["tencent", "腾讯", "腾讯新闻"],
        "needs_payload": False, "emoji": "📰", "render_kind": "tencent",
        "title": "腾讯新闻热榜",
    },
    "bili": {
        "label": "B站热门", "aliases": ["bili", "b站", "哔哩", "bilibili"],
        "needs_payload": False, "emoji": "🎬", "render_kind": "bili",
        "title": "B站热门",
    },
    "steam": {
        "label": "Steam特惠", "aliases": ["steam", "steam特惠", "特惠", "折扣"],
        "needs_payload": False, "emoji": "🎮", "render_kind": "steam",
        "title": "Steam 特惠",
    },
    "bangumi": {
        "label": "今日更新番剧", "aliases": ["bangumi", "番剧", "追番", "b站番剧"],
        "needs_payload": False, "emoji": "🍙", "render_kind": "bangumi",
        "title": "今日更新番剧",
    },
    "rss": {
        "label": "RSS订阅", "aliases": ["rss", "RSS"],
        "needs_payload": True, "emoji": "📰", "render_kind": "rss",
        "title": "RSS 订阅",
    },
}
_TYPE_SET = set(_SOURCES.keys())
# 热搜类源（主标题下方无需 payload）；信息列表类源（含 meta/time）
_BOARD_KINDS = {"weibo", "baidu", "tencent", "bili"}
_LIST_KINDS = {"rss", "steam", "bangumi"}

# 默认推送间隔（分钟），作为兜底常量；实际优先读配置
DEFAULT_INTERVAL_MIN = 30
MIN_INTERVAL_MIN = 10
# RSS 抓取时的条数
RSS_MAX_ITEMS = 8

# 内置源（非 rss）的抓取条数
BUILTIN_MAX_ITEMS = 20
# /订阅测试 用于验证 RSS 抓取+渲染链路的演示源
DEMO_RSS = "https://www.ruanyifeng.com/blog/atom.xml"


@register(
    "astrbot_plugin_subhub",
    "xiaowan138",
    "统一订阅推送中心：RSS / 微博热搜定时推送",
    "1.0.0",
)
class SubHub(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.data_dir = self._resolve_data_dir()
        self.state_file = self.data_dir / "subs.json"
        self._subs: Dict[str, Dict] = {}  # key = 会话/群 umo md5 -> 订阅配置
        self._lock = threading.Lock()
        self._task: Optional[asyncio.Task] = None
        self._load()

    # ------------------------------------------------------------------
    # 数据持久化
    # ------------------------------------------------------------------
    def _resolve_data_dir(self) -> Path:
        for imp in (
            "astrbot.core.utils.astrbot_path",
        ):
            try:
                mod = __import__(imp, fromlist=["get_astrbot_plugin_data_path"])
                path = Path(mod.get_astrbot_plugin_data_path()) / "astrbot_plugin_subhub"
                path.mkdir(parents=True, exist_ok=True)
                return path
            except Exception:
                continue
        fallback = Path(__file__).resolve().parent / "data"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

    def _load(self) -> None:
        try:
            if self.state_file.exists():
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._subs = data
                    return
            self._subs = {}
        except Exception as e:
            logger.error(f"[subhub] 读取订阅配置失败: {e}")
            self._backup_corrupt()
            self._subs = {}

    def _backup_corrupt(self) -> None:
        try:
            if self.state_file.exists():
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.state_file.replace(self.state_file.with_name(f"subs.json.corrupt.{ts}"))
                logger.warning("[subhub] 订阅文件损坏，已备份")
        except Exception as e:
            logger.error(f"[subhub] 备份损坏文件失败: {e}")

    def _save(self) -> None:
        try:
            with self._lock:
                tmp = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(self._subs, f, ensure_ascii=False, indent=2)
                os.replace(tmp, self.state_file)
        except Exception as e:
            logger.error(f"[subhub] 保存订阅配置失败: {e}")

    def _group_key(self, event: AstrMessageEvent) -> str:
        return hashlib.md5(event.unified_msg_origin.encode("utf-8")).hexdigest()[:16]

    def _default_interval(self) -> int:
        try:
            v = int(self.config.get("default_interval_min", DEFAULT_INTERVAL_MIN))
            return v if v > 0 else DEFAULT_INTERVAL_MIN
        except Exception:
            return DEFAULT_INTERVAL_MIN

    def _get_group(self, event: AstrMessageEvent) -> Dict:
        key = self._group_key(event)
        with self._lock:
            g = self._subs.setdefault(
                key,
                {
                    "umo": event.unified_msg_origin,
                    "subs": [],  # 订阅项列表
                    "interval_min": self._default_interval(),
                    "last_push": "",  # "YYYY-MM-DD HH:MM"
                    "paused": False,  # 群级暂停自动推送
                },
            )
            g.setdefault("subs", [])
            g.setdefault("interval_min", self._default_interval())
            g.setdefault("last_push", "")
            g.setdefault("paused", False)
            return g

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------
    def _resolve_type(self, raw: str) -> Optional[str]:
        """把用户输入的关键词解析为注册表内的源类型。"""
        key = raw.strip().lower()
        for t, meta in _SOURCES.items():
            if key == t or key in (a.lower() for a in meta.get("aliases", [])):
                return t
        return None

    def _type_label(self, t: str) -> str:
        return _SOURCES.get(t, {}).get("label", t)

    def _all_builtin_cmd(self) -> str:
        """罗列所有内置源命令，用于帮助/订阅提示。"""
        return " | ".join(
            f"{meta['label']}"
            for t, meta in _SOURCES.items()
            if t != "rss"
        )

    @staticmethod
    def _img_comp(img_bytes: bytes) -> Image:
        """把图片字节转成 AstrBot Image 组件（Image.file 需要 base64:// 字符串）。"""
        import base64

        b64 = base64.b64encode(img_bytes).decode("utf-8")
        return Image(file=f"base64://{b64}")

    async def _send_image(self, event: AstrMessageEvent, img_bytes: bytes):
        try:
            yield event.chain_result([self._img_comp(img_bytes)])
        except Exception as e:
            logger.error(f"[subhub] 发送图片失败: {e}")
            yield event.plain_result("⚠️ 图片渲染发送失败。")

    # ------------------------------------------------------------------
    # 指令：订阅
    # ------------------------------------------------------------------
    @filter.command("订阅")
    async def add_sub(self, event: AstrMessageEvent):
        """添加订阅。用法：/订阅 <源> ｜ /订阅 rss <链接>"""
        arg = event.message_str.strip()
        # 去掉命令前缀
        arg = re.sub(r"^[/！!]?\s*订阅\s*", "", arg).strip()

        if not arg:
            yield event.plain_result(
                "📡 订阅用法：\n"
                "  内置源：" + self._all_builtin_cmd() + "\n"
                "  /订阅 rss <链接>：任意 RSS/Atom 源\n"
                "例：\n"
                "  /订阅 微博热搜\n"
                "  /订阅 rss https://www.ruanyifeng.com/blog/atom.xml"
            )
            return

        parts = arg.split(None, 1)
        raw_type = parts[0]
        typ = self._resolve_type(raw_type)
        payload = parts[1].strip() if len(parts) > 1 else ""

        if not typ:
            yield event.plain_result(
                f"❓ 未知订阅类型「{raw_type}」。支持：" + self._all_builtin_cmd() + "、rss。"
            )
            return

        meta = _SOURCES[typ]
        if not meta["needs_payload"] and payload:
            yield event.plain_result(
                f"ℹ️ {meta['label']}是内置源，无需额外参数。直接 /订阅 {raw_type} 即可。"
            )
            return

        if typ == "rss" and not payload:
            yield event.plain_result("⚠️ RSS 订阅需要提供链接。用法：/订阅 rss <链接>")
            return

        if typ == "rss" and not re.match(r"^https?://", payload):
            yield event.plain_result("⚠️ RSS 链接必须以 http(s):// 开头。")
            return

        g = self._get_group(event)
        subs = g["subs"]
        # 去重：同一会话内相同类型+相同负载不重复添加
        exists = any(s.get("type") == typ and s.get("payload") == payload for s in subs)
        if exists:
            yield event.plain_result(
                f"ℹ️ 你已经订阅过「{self._type_label(typ)}」"
                + (f"：{payload}" if payload else "")
                + " 了。"
            )
            return

        sub = {"type": typ, "payload": payload, "seen": [], "name": self._sub_name(typ, payload)}
        subs.append(sub)
        self._save()
        self._ensure_task()
        self._record_push_time(g)
        state = g["paused"] and "（⚠️ 当前已暂停自动推送，/推送恢复 后生效）" or ""
        yield event.plain_result(
            f"✅ 已订阅「{sub['name']}」。\n"
            f"之后将按间隔 {g['interval_min']} 分钟自动推送；"
            f"也可用 /查看 {raw_type} 立即查看，或用 /订阅间隔 HH:MM 设置每天定时推送。{state}"
        )

    def _sub_name(self, typ: str, payload: str) -> str:
        meta = _SOURCES.get(typ, {})
        if not meta:
            return payload or typ
        if not meta["needs_payload"]:
            return meta["label"]
        try:
            host = re.sub(r"^https?://", "", payload).split("/")[0]
            return host or payload
        except Exception:
            return payload or "RSS"

    # ------------------------------------------------------------------
    # 指令：我的订阅 / 退订
    # ------------------------------------------------------------------
    @filter.command("我的订阅")
    @filter.command("订阅列表")
    async def list_subs(self, event: AstrMessageEvent):
        g = self._get_group(event)
        subs = g["subs"]
        if not subs:
            yield event.plain_result(
                "📭 你还没有订阅任何内容。\n可订阅：" + self._all_builtin_cmd() + "、RSS。"
            )
            return
        state = "⏸ 已暂停" if g.get("paused") else "▶ 运行中"
        lines = [
            f"📋 订阅清单（共 {len(subs)} 项 · 间隔 {g['interval_min']} 分钟 · {state}）：",
        ]
        for i, s in enumerate(subs, 1):
            name = s.get("name") or s.get("payload") or "?"
            lines.append(f"  {i}. {name}" + (f"  {s.get('payload','')}" if s.get("type") == "rss" else ""))
        lines.append("用 /退订 <编号> 移除；/订阅间隔 HH:MM 改时间；/推送暂停 暂停")
        yield event.plain_result("\n".join(lines))

    @filter.command("退订")
    @filter.command("取消订阅")
    async def remove_sub(self, event: AstrMessageEvent):
        arg = re.sub(r"^[/！!]?\s*(退订|取消订阅)\s*", "", event.message_str.strip()).strip()
        g = self._get_group(event)
        subs = g["subs"]
        if not subs:
            yield event.plain_result("📭 没有可退订的订阅。")
            return
        if not arg:
            yield event.plain_result("用法：/退订 <编号>。查看编号用 /我的订阅。")
            return
        if arg.isdigit():
            idx = int(arg) - 1
            if 0 <= idx < len(subs):
                rm = subs.pop(idx)
                self._save()
                yield event.plain_result(f"🗑️ 已退订「{rm.get('name') or rm.get('payload','')}」。")
                return
            yield event.plain_result(f"❌ 没有编号 {arg} 的订阅。")
            return
        # 按名称匹配
        target = arg
        matched = [i for i, s in enumerate(subs) if target in (s.get("name") or "") or target in (s.get("payload") or "")]
        if not matched:
            yield event.plain_result(f"❌ 找不到包含「{target}」的订阅。")
            return
        if len(matched) > 1:
            yield event.plain_result(f"⚠️ 匹配到 {len(matched)} 项，请用编号精确退订。/我的订阅 查看编号。")
            return
        rm = subs.pop(matched[0])
        self._save()
        yield event.plain_result(f"🗑️ 已退订「{rm.get('name') or rm.get('payload','')}」。")

    # ------------------------------------------------------------------
    # 指令：查看（手动触发）
    # ------------------------------------------------------------------
    @filter.command("查看")
    async def view(self, event: AstrMessageEvent):
        """手动抓取并推送。用法：/查看 <源> ｜ /查看 订阅编号 ｜ /查看 rss <链接>"""
        arg = re.sub(r"^[/！!]?\s*查看\s*", "", event.message_str.strip()).strip()
        g = self._get_group(event)
        if not g["subs"]:
            yield event.plain_result("还没有订阅，先 /订阅 吧。")
            return

        # 支持 /查看 <订阅编号>
        if arg.isdigit() and 1 <= int(arg) <= len(g["subs"]):
            async for r in self._push_one(event, g["subs"][int(arg) - 1], manual=True):
                yield r
            return

        typ = self._resolve_type(arg)
        if typ is None:
            # 尝试匹配订阅名
            hits = [s for s in g["subs"] if arg in (s.get("name") or "")]
            if hits:
                async for r in self._push_one(event, hits[0], manual=True):
                    yield r
                return
            yield event.plain_result(
                "用法：/查看 " + self._all_builtin_cmd() + "、rss，或 /查看 订阅编号。"
            )
            return

        # 已订阅对应用户则用它；否则对内建源可直接查看，对 rss 需已订阅
        matched = [s for s in g["subs"] if s.get("type") == typ]
        if matched:
            async for r in self._push_one(event, matched[0], manual=True):
                yield r
            return
        if typ == "rss":
            yield event.plain_result("你还没有订阅 RSS。用 /订阅 rss <链接> 添加。")
            return
        # 内建源未订阅也能直接看一次
        async for r in self._push_one(event, {"type": typ, "payload": "", "seen": [], "name": self._type_label(typ)}, manual=True):
            yield r

    # ------------------------------------------------------------------
    # 指令：推送暂停 / 推送恢复
    # ------------------------------------------------------------------
    @filter.command("推送暂停")
    @filter.permission_type(PermissionType.ADMIN)
    async def pause_push(self, event: AstrMessageEvent):
        self._get_group(event)["paused"] = True
        self._save()
        yield event.plain_result("⏸ 已暂停本会话的自动推送。手动 /查看 仍可用；/推送恢复 恢复。")

    @filter.command("推送恢复")
    @filter.permission_type(PermissionType.ADMIN)
    async def resume_push(self, event: AstrMessageEvent):
        self._get_group(event)["paused"] = False
        self._save()
        yield event.plain_result("▶ 已恢复本会话的自动推送。")

    @filter.command("推送总开关")
    @filter.command("总开关")
    @filter.permission_type(PermissionType.ADMIN)
    async def global_switch(self, event: AstrMessageEvent):
        """全局自动推送总开关。用法：/推送总开关 on|off。"""
        arg = re.sub(r"^[/！!]?\s*(推送总开关|总开关)\s*", "", event.message_str.strip()).strip().lower()
        if arg in ("off", "关闭", "关", "0", "false", "off."):
            self._set_global_enabled(False)
            yield event.plain_result(
                "⏸ 已关闭全部自动推送（所有会话）。手动 /查看 与 /订阅测试 仍可用；/推送总开关 on 恢复。"
            )
            return
        if arg in ("on", "开启", "开", "1", "true"):
            self._set_global_enabled(True)
            yield event.plain_result("▶ 已开启全部自动推送。")
            return
        state = "开启" if self._global_enabled() else "关闭"
        yield event.plain_result(
            f"🔘 全局自动推送当前为：{state}。\n"
            "用法：/推送总开关 on ｜ off（仅管理员）。"
        )

    # ------------------------------------------------------------------
    # 指令：订阅间隔
    # ------------------------------------------------------------------
    @filter.command("订阅间隔")
    @filter.permission_type(PermissionType.ADMIN)
    async def set_interval(self, event: AstrMessageEvent):
        """设置定时推送时间。用法：/订阅间隔 HH:MM ｜ /订阅间隔 off"""
        arg = re.sub(r"^[/！!]?\s*订阅间隔\s*", "", event.message_str.strip()).strip()
        g = self._get_group(event)
        if not g["subs"]:
            yield event.plain_result("还没有订阅，先 /订阅 再设置间隔。")
            return
        if not arg:
            yield event.plain_result(f"⏰ 当前推送间隔 {g['interval_min']} 分钟。\n用法：/订阅间隔 HH:MM 或 /订阅间隔 off。")
            return
        low = arg.lower()
        if low in ("off", "关闭", "取消", "0"):
            g.pop("time", None)
            g.pop("last_push", None)
            self._save()
            yield event.plain_result("⏰ 已关闭定时推送。手动 /查看 仍可使用。")
            return
        m = re.fullmatch(r"\s*([01]?\d|2[0-3]):([0-5]\d)\s*", arg)
        if not m:
            yield event.plain_result("⚠️ 时间格式不对，请用 24 小时制 HH:MM。")
            return
        g["time"] = arg.strip()
        self._save()
        self._ensure_task()
        yield event.plain_result(f"⏰ 已设置每天 {g['time']} 定时推送。")

    # ------------------------------------------------------------------
    # 指令：一键测试所有来源（交付验证用）
    # ------------------------------------------------------------------
    @filter.command("订阅测试")
    async def test_all(self, event: AstrMessageEvent):
        """逐个抓取并推送所有来源，验证各个功能是否可用。用法：/订阅测试。"""
        logger.info("[subhub][诊断] test_all 命令处理器已进入执行")
        yield event.plain_result("🧪 开始逐个测试，请稍候…")
        g = self._get_group(event)
        umo = event.unified_msg_origin
        subs_todo = []
        for typ, meta in _SOURCES.items():
            if typ == "rss":
                # 用演示源验证 RSS 抓取+渲染链路（与是否订阅无关）
                subs_todo.append({"type": "rss", "payload": DEMO_RSS, "seen": [], "name": "RSS（演示源）"})
            else:
                subs_todo.append({"type": typ, "payload": "", "seen": [], "name": meta["label"]})
        ok = fail = 0
        detail = []
        for sub in subs_todo:
            label = sub.get("name") or self._type_label(sub["type"])
            logger.info(f"[subhub][诊断] 开始测试 {label}")
            try:
                pushed = await self._send_card_to_umo(umo, sub)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[subhub] 测试 {label} 异常: {e}")
                pushed = False
            logger.info(f"[subhub][诊断] {label} 结果 -> {pushed}")
            if pushed:
                ok += 1
            else:
                fail += 1
            detail.append(f"{'✅' if pushed else '❌'} {label}")
        yield event.plain_result(
            f"🧪 测试完成：成功 {ok} 项 / 失败 {fail} 项\n" + "\n".join(detail)
        )

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def _debug_hook(self, event: AstrMessageEvent):
        """排查用：记录本插件可能命中的指令及发送者，定位「指令没反应」问题。"""
        try:
            text = (event.message_str or "").strip().lstrip("/！!")
            if not text:
                return
            for kw in (
                "订阅测试", "功能测试", "订阅", "退订", "取消订阅", "我的订阅",
                "订阅列表", "查看", "推送暂停", "推送恢复", "推送总开关", "总开关",
                "订阅间隔", "订阅帮助", "订阅说明",
            ):
                if text == kw or text.startswith(kw + " "):
                    sender = "?"
                    try:
                        sender = event.get_sender_name()
                    except Exception:
                        pass
                    logger.info(
                        f"[subhub][诊断] 收到 `{kw}` 指令 <- 原消息 {text!r} · 发送者 {sender}"
                    )
                    break
        except Exception as e:  # 诊断钩子绝不影响正常流程
            logger.warning(f"[subhub][诊断] 钩子异常: {e}")

    # ------------------------------------------------------------------
    # 指令：帮助
    # ------------------------------------------------------------------
    @filter.command("订阅帮助")
    @filter.command("订阅说明")
    async def help(self, event: AstrMessageEvent):
        lines = [
            "📡 统一订阅推送中心 · 支持 7 类来源",
            "  微博热搜 / 百度热搜 / 腾讯新闻 / B站热门",
            "  Steam特惠 / 今日番剧 / RSS订阅",
            "── 使用 ──",
            "  /订阅 <源>|rss <链接>  添加订阅",
            "  /查看 <源>|编号       立即推送一次",
            "  /我的订阅              查看已订阅",
            "  /退订 <编号>           移除订阅",
            "  /订阅间隔 HH:MM|off   定时推送（管理员）",
            "── 开关 ──",
            "  /推送总开关 on|off     全局自动推送（管理员）",
            "  /推送暂停|恢复         本群暂停/恢复（管理员）",
            "  /订阅测试              一键验证所有功能",
            "说明：定时推送需机器人常驻；间隔最短 10 分钟。",
        ]
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # 推送实现
    # ------------------------------------------------------------------
    async def _fetch_items(self, typ: str, payload: str, max_items: int):
        """按源类型调用对应抓取器，统一返回 items 列表。
        对返回 {items,...} 的源（weibo/bangumi）取 items。"""
        if typ == "weibo":
            d = await fetchers.fetch_weibo_hot(max_items=max_items)
            return d.get("items", [])
        if typ == "baidu":
            return await fetchers.fetch_baidu_hot(max_items=max_items)
        if typ == "tencent":
            return await fetchers.fetch_tencent_news(max_items=max_items)
        if typ == "bili":
            return await fetchers.fetch_bili_hot(max_items=max_items)
        if typ == "steam":
            return await fetchers.fetch_steam_specials(max_items=max_items)
        if typ == "bangumi":
            d = await fetchers.fetch_bangumi(max_items=max_items)
            return d.get("items", [])
        if typ == "rss":
            return await fetchers.fetch_rss(payload, max_items=max_items)
        return []

    def _build_card(self, typ: str, sub: Dict, items: List[Dict]) -> Dict:
        """把抓取结果构造成渲染卡片。"""
        meta = _SOURCES.get(typ, {})
        emoji = meta.get("emoji", "📡")
        title = meta.get("title", f"{emoji} {self._type_label(typ)}")

        # 内置热搜类源：直接展示榜单；无需去重。
        is_board = typ in _BOARD_KINDS
        is_list_dedup = typ == "rss"  # 仅 RSS 需要去重防重推
        if is_list_dedup:
            new_items = [i for i in items if i.get("id") not in sub.get("seen", [])]
        else:
            new_items = items

        if not new_items:
            return {}

        if is_board:
            return {
                "kind": typ,  # weibo/baidu/tencent/bili
                "title": f"{emoji} {title}",
                "subtitle": f"实时 · 共 {len(new_items)} 条",
                "items": new_items,
            }
        if typ == "bangumi":
            return {
                "kind": "bangumi",
                "title": f"{emoji} 今日更新番剧",
                "subtitle": f"今日更新 · 共 {len(new_items)} 部",
                "items": new_items,
            }
        if typ == "steam":
            return {
                "kind": "steam",
                "title": f"{emoji} Steam 特惠",
                "subtitle": f"限时特惠 · 共 {len(new_items)} 款",
                "items": new_items,
            }
        if typ == "rss":
            name = sub.get("name") or "RSS 订阅"
            return {
                "kind": "rss",
                "title": f"📰 {name}",
                "subtitle": f"新更新 {len(new_items)} 条",
                "items": new_items,
            }
        return {}

    def _mark_seen(self, sub: Dict, items: List[Dict]) -> None:
        """仅对 RSS 记录已推送条目的 id，用于去重。"""
        if sub.get("type") != "rss" or not items:
            return
        sub["seen"] = list(sub.get("seen", [])) + [i.get("id") for i in items if i.get("id")]
        sub["seen"] = sub["seen"][-200:]
        self._save()

    async def _push_one(self, event, sub: Dict, manual=False):
        """推送单个订阅。异步生成器，通过 yield 把结果抛给上层。"""
        typ = sub.get("type")
        max_items = RSS_MAX_ITEMS if typ == "rss" else BUILTIN_MAX_ITEMS
        try:
            items = await self._fetch_items(typ, sub.get("payload", ""), max_items)
        except Exception as e:
            label = self._type_label(typ)
            logger.error(f"[subhub] {label} 抓取失败: {e}")
            yield event.plain_result(f"⚠️ {label} 抓取失败：{e}")
            return

        card = self._build_card(typ, sub, items)
        if not card:
            if manual:
                yield event.plain_result("ℹ️ 最近没有新内容。")
            return

        img = None
        try:
            img = render_image(card)
        except Exception as e:
            logger.error(f"[subhub] 渲染 {typ} 失败: {e}")

        new_for_seen = [i for i in items if i.get("id") not in sub.get("seen", [])] or items
        if img:
            async for r in self._send_image(event, img):
                yield r
            self._mark_seen(sub, [i for i in new_for_seen])
        else:
            yield event.plain_result(
                f"{card.get('title','')}（{len(card.get('items',[]))} 条）：\n"
                + "\n".join(f"  · {i.get('title','-')}" for i in card.get("items", []))
            )

    # ------------------------------------------------------------------
    # 定时调度
    # ------------------------------------------------------------------
    def _record_push_time(self, g: Dict) -> None:
        """记录当前节点时间，作为间隔推送的计时起点（避免订阅后立刻推送）。"""
        try:
            g["last_push"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            self._save()
        except Exception as e:
            logger.error(f"[subhub] 记录推送时间失败: {e}")

    def _ensure_task(self):
        if self._task is None or self._task.done():
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            self._task = loop.create_task(self._scheduler())

    # 全局总开关：配置项 + 命令双重控制，均持久化，默认开启
    def _global_enabled(self) -> bool:
        try:
            cfg = bool(self.config.get("enable_auto_push", True))
        except Exception:
            cfg = True
        try:
            with self._lock:
                g = self._subs.get("_global")
                if isinstance(g, dict) and g.get("enabled") is not None:
                    return cfg and bool(g["enabled"])
        except Exception:
            pass
        return cfg

    def _set_global_enabled(self, on: bool) -> None:
        with self._lock:
            self._subs["_global"] = {"enabled": bool(on)}
        self._save()

    def _safe_interval(self, interval: int) -> int:
        """把间隔钳制到不小于配置的最小间隔。"""
        try:
            floor = int(self.config.get("min_interval_min", MIN_INTERVAL_MIN))
            floor = max(1, floor)
        except Exception:
            floor = MIN_INTERVAL_MIN
        try:
            return max(floor, int(interval))
        except (TypeError, ValueError):
            return max(floor, DEFAULT_INTERVAL_MIN)

    async def _scheduler(self):
        while True:
            try:
                await asyncio.sleep(20)
                # 全局开关：关闭时跳过全部自动推送
                if not self._global_enabled():
                    continue
                now = datetime.now()
                hhmm = f"{now.hour:02d}:{now.minute:02d}"
                today = now.strftime("%Y-%m-%d")
                for gkey, g in list(self._subs.items()):
                    subs = g.get("subs") or []
                    if not subs:
                        continue
                    # 群级暂停：跳过本会话自动推送
                    if g.get("paused"):
                        continue
                    # 时间模式（HH:MM 每天定时）
                    fixed = g.get("time")
                    if fixed:
                        if fixed != hhmm or g.get("last_push_date") == today:
                            continue
                        for sub in subs:
                            await self._push_by_umo(g, sub)
                        g["last_push_date"] = today
                        self._save()
                        continue
                    # 间隔模式
                    last = g.get("last_push")
                    if not last:
                        continue
                    try:
                        last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M")
                    except ValueError:
                        continue
                    gap = (now - last_dt).total_seconds() / 60
                    interval = self._safe_interval(g.get("interval_min", DEFAULT_INTERVAL_MIN))
                    if gap < interval:
                        continue
                    for sub in subs:
                        await self._push_by_umo(g, sub)
                    g["last_push"] = now.strftime("%Y-%m-%d %H:%M")
                    self._save()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[subhub] 调度异常: {e}")

    async def _push_by_umo(self, g: Dict, sub: Dict) -> bool:
        """主动推送：无事件上下文，通过 context.send_message 发送到会话。返回是否推送成功。"""
        umo = g.get("umo")
        if not umo:
            return False
        return await self._send_card_to_umo(umo, sub)

    async def _send_card_to_umo(self, umo: str, sub: Dict) -> bool:
        """抓取、渲染并把订阅卡片发到指定会话。图片优先，失败回退纯文本。"""
        typ = sub.get("type")
        max_items = RSS_MAX_ITEMS if typ == "rss" else BUILTIN_MAX_ITEMS
        try:
            items = await self._fetch_items(typ, sub.get("payload", ""), max_items)
        except Exception as e:
            logger.error(f"[subhub] {self._type_label(typ)} 定时抓取失败: {e}")
            return False

        card = self._build_card(typ, sub, items)
        if not card:
            return False

        img = None
        try:
            img = render_image(card)
        except Exception as e:
            logger.error(f"[subhub] 定时渲染 {typ} 失败: {e}")

        new_for_seen = [i for i in items if i.get("id") not in sub.get("seen", [])] or items
        try:
            if img:
                await self.context.send_message(umo, MessageChain([self._img_comp(img)]))
                self._mark_seen(sub, new_for_seen)
            else:
                await self.context.send_message(
                    umo,
                    MessageChain().message(
                        f"{card.get('title','')}（{len(card.get('items',[]))} 条）：\n"
                        + "\n".join(f"  · {i.get('title','-')}" for i in card["items"])
                    ),
                )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[subhub] 主动发送失败: {e}")
            return False

    async def terminate(self):
        if self._task:
            self._task.cancel()
        self._save()
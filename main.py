"""
统一订阅推送中心 - AstrBot 插件

把多个订阅源统一管理，按设定间隔/时间定时主动推送到群聊 / 会话：
- 热搜榜单：微博、百度、知乎、抖音、腾讯新闻、B 站、36氪
- 信息列表：Steam 特惠、今日更新番剧、GitHub Trending、Hacker News、V2EX
- RSS/Atom：订阅任意链接
支持：
- 关键词过滤（只推命中关键词的条目）
- 每日汇总卡片（digest：多来源合并一张图）
- 订阅级独立调度 / 群级统一调度
- 静默时段（夜间不打扰，可选早晨补发汇总）
- 连续失败自动暂停订阅（自愈），手动 /订阅开关 恢复
- 订阅导入 / 导出
- 全局总开关 + 群级暂停

工程惯例：
- 持久化存放在 AstrBot data 目录的插件子目录 subs.json（原子写 + 线程锁）
- 全程 try/except，任何异常只记录日志，绝不让 AstrBot 崩溃
- 使用 astrbot 的 logger 记录日志
"""

from __future__ import annotations

import asyncio
import base64
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
from .utils import render_image, set_theme

# ---------------------------------------------------------------------------
# 订阅源注册表：新增源在此登记即可，命令/推送/渲染自动适配。
#   fetch: fetchers 里的函数名（签名统一 (payload, max_items)）
#   needs_payload: 是否需要附加参数（目前仅 rss 需要链接）
#   sub_tmpl: 卡片副标题模板
# ---------------------------------------------------------------------------
_SOURCES = {
    "weibo": {
        "label": "微博热搜", "aliases": ["weibo", "微博", "微博热搜", "热搜"],
        "needs_payload": False, "emoji": "🔥", "title": "微博热搜",
        "fetch": "fetch_weibo_hot", "sub_tmpl": "实时 · 共 {n} 条",
    },
    "baidu": {
        "label": "百度热搜", "aliases": ["baidu", "百度"],
        "needs_payload": False, "emoji": "🍃", "title": "百度热搜",
        "fetch": "fetch_baidu_hot", "sub_tmpl": "实时 · 共 {n} 条",
    },
    "zhihu": {
        "label": "知乎热榜", "aliases": ["zhihu", "知乎", "知乎热榜"],
        "needs_payload": False, "emoji": "💙", "title": "知乎热榜",
        "fetch": "fetch_zhihu_hot", "sub_tmpl": "实时 · 共 {n} 条",
    },
    "douyin": {
        "label": "抖音热搜", "aliases": ["douyin", "抖音", "抖音热搜"],
        "needs_payload": False, "emoji": "🎵", "title": "抖音热搜",
        "fetch": "fetch_douyin_hot", "sub_tmpl": "实时 · 共 {n} 条",
    },
    "tencent": {
        "label": "腾讯新闻热榜", "aliases": ["tencent", "腾讯", "腾讯新闻"],
        "needs_payload": False, "emoji": "📰", "title": "腾讯新闻热榜",
        "fetch": "fetch_tencent_news", "sub_tmpl": "实时 · 共 {n} 条",
    },
    "bili": {
        "label": "B站热门", "aliases": ["bili", "b站", "哔哩", "bilibili"],
        "needs_payload": False, "emoji": "🎬", "title": "B站热门",
        "fetch": "fetch_bili_hot", "sub_tmpl": "实时 · 共 {n} 条",
    },
    "kr36": {
        "label": "36氪热榜", "aliases": ["kr36", "36kr", "36氪", "氪"],
        "needs_payload": False, "emoji": "💼", "title": "36氪热榜",
        "fetch": "fetch_36kr", "sub_tmpl": "科技快讯 · 共 {n} 条",
    },
    "steam": {
        "label": "Steam特惠", "aliases": ["steam", "steam特惠", "特惠", "折扣"],
        "needs_payload": False, "emoji": "🎮", "title": "Steam 特惠",
        "fetch": "fetch_steam_specials", "sub_tmpl": "限时特惠 · 共 {n} 款",
    },
    "bangumi": {
        "label": "今日更新番剧", "aliases": ["bangumi", "番剧", "追番", "b站番剧"],
        "needs_payload": False, "emoji": "🍙", "title": "今日更新番剧",
        "fetch": "fetch_bangumi", "sub_tmpl": "今日更新 · 共 {n} 部",
    },
    "github": {
        "label": "GitHub趋势", "aliases": ["github", "gh", "trending", "开源"],
        "needs_payload": False, "emoji": "🐙", "title": "GitHub Trending",
        "fetch": "fetch_github_trending", "sub_tmpl": "今日趋势 · 共 {n} 仓库",
    },
    "hn": {
        "label": "HackerNews", "aliases": ["hn", "hackernews", "hacker news"],
        "needs_payload": False, "emoji": "🟠", "title": "Hacker News",
        "fetch": "fetch_hn", "sub_tmpl": "首页热帖 · 共 {n} 条",
    },
    "v2ex": {
        "label": "V2EX热门", "aliases": ["v2ex", "v站"],
        "needs_payload": False, "emoji": "💬", "title": "V2EX 热门",
        "fetch": "fetch_v2ex", "sub_tmpl": "热门话题 · 共 {n} 条",
    },
    "rss": {
        "label": "RSS订阅", "aliases": ["rss", "RSS"],
        "needs_payload": True, "emoji": "📰", "title": "RSS 订阅",
        "fetch": "fetch_rss", "sub_tmpl": "新更新 {n} 条",
    },
}

# 默认推送间隔（分钟），作为兜底常量；实际优先读配置
DEFAULT_INTERVAL_MIN = 30
MIN_INTERVAL_MIN = 10
# RSS 抓取条数（卡片可显示摘要行，条数略少）
RSS_MAX_ITEMS = 8
# 每个会话最多订阅数（防滥用）
MAX_SUBS_PER_GROUP = 20
# /订阅测试 用于验证 RSS 抓取+渲染链路的演示源
DEMO_RSS = "https://www.ruanyifeng.com/blog/atom.xml"


@register(
    "astrbot_plugin_subhub",
    "xiaowan138",
    "统一订阅推送中心：12 类内容源 + RSS 定时推送",
    "1.1.0",
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
        self._quiet_prev = False  # 上一轮调度是否处于静默时段
        self._apply_theme()
        self._load()

    async def initialize(self):
        """插件加载完毕即启动调度器（重启 AstrBot 后定时推送不失效）。"""
        self._ensure_task()

    async def terminate(self):
        if self._task:
            self._task.cancel()
        self._save()

    # ------------------------------------------------------------------
    # 数据持久化
    # ------------------------------------------------------------------
    def _resolve_data_dir(self) -> Path:
        try:
            mod = __import__(
                "astrbot.core.utils.astrbot_path",
                fromlist=["get_astrbot_plugin_data_path"],
            )
            path = Path(mod.get_astrbot_plugin_data_path()) / "astrbot_plugin_subhub"
            path.mkdir(parents=True, exist_ok=True)
            return path
        except Exception:
            fallback = Path(__file__).resolve().parent / "data"
            fallback.mkdir(parents=True, exist_ok=True)
            return fallback

    def _load(self) -> None:
        try:
            if self.state_file.exists():
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    for k, g in data.items():
                        if k != "_global" and isinstance(g, dict):
                            self._normalize_group(g)
                    self._subs = data
                    return
            self._subs = {}
        except Exception as e:
            logger.error(f"[subhub] 读取订阅配置失败: {e}")
            self._backup_corrupt()
            self._subs = {}

    def _normalize_group(self, g: Dict) -> None:
        """补齐/修正字段，并修复旧版本遗留的坏状态。"""
        g.setdefault("umo", "")
        subs = [s for s in (g.get("subs") or []) if isinstance(s, dict)]
        g["subs"] = subs
        g.setdefault("interval_min", self._default_interval())
        g.setdefault("last_push", "")
        g.setdefault("paused", False)
        # 时间字段统一补零（修复旧数据 "8:00" 永不触发的 bug）
        for key in ("time", "digest_time"):
            t = self._norm_time(g.get(key)) if g.get(key) else None
            if t:
                g[key] = t
            else:
                g.pop(key, None)
        # 修复旧版 off 语义损坏的数据（无 time、间隔>0、无 last_push → 永不推送）
        if not g.get("time") and (g.get("interval_min") or 0) > 0 and not g.get("last_push"):
            g["last_push"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        for s in subs:
            s.setdefault("payload", "")
            s.setdefault("seen", [])
            if not isinstance(s.get("keywords"), list):
                s["keywords"] = []
            s["keywords"] = [str(k) for k in s["keywords"] if str(k).strip()]
            s.setdefault("fail_count", 0)
            s.setdefault("paused", False)
            s.setdefault("auto_paused", False)
            s.setdefault("name", self._sub_name(s.get("type", ""), s.get("payload", "")))
            for key in ("time", "interval_min"):
                if key in s and not s.get(key):
                    s.pop(key, None)
            if s.get("time"):
                t = self._norm_time(s["time"])
                if t:
                    s["time"] = t
                else:
                    s.pop("time", None)

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
            g = self._subs.get(key)
            if g is None:
                g = {
                    "umo": event.unified_msg_origin,
                    "subs": [],
                    "interval_min": self._default_interval(),
                    "last_push": "",
                    "paused": False,
                }
                self._subs[key] = g
            self._normalize_group(g)
            return g

    # ------------------------------------------------------------------
    # 配置读取
    # ------------------------------------------------------------------
    def _apply_theme(self) -> None:
        t = str(self.config.get("theme", "dark") or "dark").strip().lower()
        set_theme(t)

    def _builtin_top_n(self) -> int:
        """内置源卡片条数（weibo_top_n 配置项，1-50）。"""
        try:
            v = int(self.config.get("weibo_top_n", 20))
        except Exception:
            v = 20
        return max(1, min(50, v))

    def _auto_pause_after(self) -> int:
        """连续抓取失败多少次后自动暂停订阅（0 = 关闭自愈）。"""
        try:
            v = int(self.config.get("auto_pause_after_fails", 5))
        except Exception:
            v = 5
        return max(0, v)

    def _digest_top_n(self) -> int:
        try:
            v = int(self.config.get("digest_top_n", 3))
        except Exception:
            v = 3
        return max(1, min(10, v))

    def _quiet_catchup(self) -> bool:
        try:
            return bool(self.config.get("quiet_catchup", True))
        except Exception:
            return True

    def _in_quiet(self, now: datetime) -> bool:
        """静默时段（配置 quiet_hours，如 "23:00-07:00"；空 = 不启用）。"""
        cfg = str(self.config.get("quiet_hours", "") or "").strip()
        m = re.fullmatch(r"(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})", cfg)
        if not m:
            return False
        s = int(m.group(1)) * 60 + int(m.group(2))
        e = int(m.group(3)) * 60 + int(m.group(4))
        t = now.hour * 60 + now.minute
        if s <= e:
            return s <= t < e
        return t >= s or t < e

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------
    @staticmethod
    def _norm_time(s) -> Optional[str]:
        """把 "8:00" 规范化为 "08:00"；非法返回 None。"""
        if not s or not isinstance(s, str):
            return None
        m = re.fullmatch(r"\s*([01]?\d|2[0-3])[:：]([0-5]\d)\s*", s)
        if not m:
            return None
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"

    def _resolve_type(self, raw: str) -> Optional[str]:
        """把用户输入的关键词解析为注册表内的源类型。"""
        key = (raw or "").strip().lower()
        if key in _SOURCES:
            return key
        for t, meta in _SOURCES.items():
            if key in (a.lower() for a in meta.get("aliases", [])):
                return t
        return None

    def _type_label(self, t: str) -> str:
        return _SOURCES.get(t, {}).get("label", t)

    def _all_builtin_cmd(self) -> str:
        """罗列所有内置源标签，用于帮助/订阅提示。"""
        return " / ".join(
            f"{meta['label']}" for t, meta in _SOURCES.items() if t != "rss"
        )

    @staticmethod
    def _img_comp(img_bytes: bytes) -> Image:
        """把图片字节转成 AstrBot Image 组件（Image.file 需要 base64:// 字符串）。"""
        b64 = base64.b64encode(img_bytes).decode("utf-8")
        return Image(file=f"base64://{b64}")

    async def _send_image(self, event: AstrMessageEvent, img_bytes: bytes):
        try:
            yield event.chain_result([self._img_comp(img_bytes)])
        except Exception as e:
            logger.error(f"[subhub] 发送图片失败: {e}")
            yield event.plain_result("⚠️ 图片渲染发送失败。")

    def _sub_name(self, typ: str, payload: str) -> str:
        meta = _SOURCES.get(typ, {})
        if not meta:
            return payload or typ
        if not meta.get("needs_payload"):
            return meta["label"]
        try:
            host = re.sub(r"^https?://", "", payload).split("/")[0]
            return host or payload
        except Exception:
            return payload or "RSS"

    @staticmethod
    def _strip_cmd(event: AstrMessageEvent, *names: str) -> str:
        """去掉命令名前缀，返回剩余参数。"""
        pattern = r"^[/！!]?\s*(?:" + "|".join(re.escape(n) for n in names) + r")\s*"
        return re.sub(pattern, "", event.message_str.strip()).strip()

    # ------------------------------------------------------------------
    # 指令：订阅
    # ------------------------------------------------------------------
    @filter.command("订阅")
    async def add_sub(self, event: AstrMessageEvent):
        """添加订阅。用法：/订阅 <源> [关键词:词1,词2] ｜ /订阅 rss <链接> [关键词:...]"""
        arg = self._strip_cmd(event, "订阅")

        if not arg:
            yield event.plain_result(
                "📡 订阅用法：\n"
                "  内置源：" + self._all_builtin_cmd() + "\n"
                "  /订阅 rss <链接>：任意 RSS/Atom 源\n"
                "  可附加关键词过滤（只推命中标题的条目）：\n"
                "  /订阅 微博热搜 关键词:原神,崩铁\n"
                "例：\n"
                "  /订阅 微博热搜\n"
                "  /订阅 rss https://www.ruanyifeng.com/blog/atom.xml"
            )
            return

        tokens = arg.split()
        raw_type = tokens[0]
        typ = self._resolve_type(raw_type)
        if not typ:
            yield event.plain_result(
                f"❓ 未知订阅类型「{raw_type}」。支持：" + self._all_builtin_cmd() + "、rss。"
            )
            return

        meta = _SOURCES[typ]
        payload = ""
        rest = tokens[1:]
        if meta["needs_payload"]:
            if not rest:
                yield event.plain_result("⚠️ RSS 订阅需要提供链接。用法：/订阅 rss <链接>")
                return
            payload = rest[0].strip()
            if not re.match(r"^https?://", payload):
                yield event.plain_result("⚠️ RSS 链接必须以 http(s):// 开头。")
                return
            rest = rest[1:]

        # 解析关键词：支持 "关键词:词1,词2"、"kw:词" 或裸词（多个用逗号分隔）
        keywords: List[str] = []
        for tok in rest:
            body = re.sub(r"^(?:关键词|kw)[:：]", "", tok, flags=re.IGNORECASE)
            keywords.extend(k.strip() for k in re.split(r"[,，]", body) if k.strip())

        g = self._get_group(event)
        subs = g["subs"]
        if len(subs) >= MAX_SUBS_PER_GROUP:
            yield event.plain_result(f"⚠️ 本会话订阅数已达上限 {MAX_SUBS_PER_GROUP} 条，请先 /退订 部分订阅。")
            return
        # 去重：同一会话内相同类型+相同负载不重复添加
        if any(s.get("type") == typ and s.get("payload") == payload for s in subs):
            yield event.plain_result(
                f"ℹ️ 你已经订阅过「{self._type_label(typ)}」"
                + (f"：{payload}" if payload else "")
                + " 了。"
            )
            return

        sub = {
            "type": typ,
            "payload": payload,
            "seen": [],
            "name": self._sub_name(typ, payload),
            "keywords": keywords,
        }
        subs.append(sub)
        self._save()
        self._ensure_task()
        self._record_push_time(g)
        state = "（⚠️ 当前已暂停自动推送，/推送恢复 后生效）" if g["paused"] else ""
        kw_hint = f"\n🔍 关键词过滤：{'、'.join(keywords)}" if keywords else ""
        yield event.plain_result(
            f"✅ 已订阅「{sub['name']}」。{kw_hint}\n"
            f"之后将按本会话设置自动推送（当前间隔 {g['interval_min']} 分钟）；"
            f"也可用 /查看 {raw_type} 立即查看，或用 /订阅间隔 HH:MM 设置每天定时推送。{state}"
        )

    # ------------------------------------------------------------------
    # 指令：我的订阅 / 退订
    # ------------------------------------------------------------------
    @filter.command("我的订阅", alias={"订阅列表"})
    async def list_subs(self, event: AstrMessageEvent):
        g = self._get_group(event)
        subs = g["subs"]
        if not subs:
            yield event.plain_result(
                "📭 你还没有订阅任何内容。\n可订阅：" + self._all_builtin_cmd() + "、RSS。"
            )
            return
        state = "⏸ 已暂停" if g.get("paused") else "▶ 运行中"
        # 群调度描述
        digest_t = self._norm_time(g.get("digest_time")) if g.get("digest_time") else None
        if digest_t:
            sched = f"每天 {digest_t} 汇总推送"
        elif g.get("time"):
            sched = f"每天 {g['time']} 定时推送"
        elif (g.get("interval_min") or 0) > 0:
            sched = f"每 {g['interval_min']} 分钟轮询"
        else:
            sched = "自动推送已关闭"
        lines = [f"📋 订阅清单（共 {len(subs)} 项 · {sched} · {state}）："]
        for i, s in enumerate(subs, 1):
            name = s.get("name") or s.get("payload") or "?"
            tags = []
            if s.get("keywords"):
                tags.append("关键词:" + "/".join(s["keywords"]))
            if self._sub_has_own(s):
                tags.append(
                    f"独立:每天{s['time']}" if s.get("time") else f"独立:每{s.get('interval_min')}分钟"
                )
            if s.get("auto_paused"):
                tags.append(f"⛔连续失败{s.get('fail_count', 0)}次已暂停")
            elif s.get("paused"):
                tags.append("⏸已暂停")
            lines.append(f"  {i}. {name}" + (f"（{' · '.join(tags)}）" if tags else ""))
        lines.append("管理：/退订 <编号> · /订阅开关 <编号> on|off · /订阅关键词 <编号> 词|off")
        yield event.plain_result("\n".join(lines))

    @filter.command("退订", alias={"取消订阅"})
    async def remove_sub(self, event: AstrMessageEvent):
        arg = self._strip_cmd(event, "退订", "取消订阅")
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
                yield event.plain_result(f"🗑️ 已退订「{rm.get('name') or rm.get('payload', '')}」。")
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
        yield event.plain_result(f"🗑️ 已退订「{rm.get('name') or rm.get('payload', '')}」。")

    # ------------------------------------------------------------------
    # 指令：查看（手动触发）
    # ------------------------------------------------------------------
    @filter.command("查看")
    async def view(self, event: AstrMessageEvent):
        """手动抓取并推送。用法：/查看 <源> ｜ /查看 <订阅编号>"""
        arg = self._strip_cmd(event, "查看")
        if not arg:
            yield event.plain_result(
                "用法：/查看 <源> 或 /查看 <订阅编号>。\n支持：" + self._all_builtin_cmd() + "、rss。"
            )
            return
        g = self._get_group(event)

        # /查看 <订阅编号>
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

        # 已订阅对应用户则用它
        matched = [s for s in g["subs"] if s.get("type") == typ]
        if matched:
            if typ == "rss" and len(matched) > 1:
                yield event.plain_result(
                    f"本会话订阅了 {len(matched)} 个 RSS 源，请用 /查看 <编号> 指定。"
                )
                return
            async for r in self._push_one(event, matched[0], manual=True):
                yield r
            return
        if typ == "rss":
            yield event.plain_result("你还没有订阅 RSS。用 /订阅 rss <链接> 添加。")
            return
        # 内建源未订阅也能直接看一次
        async for r in self._push_one(
            event,
            {"type": typ, "payload": "", "seen": [], "name": self._type_label(typ), "keywords": []},
            manual=True,
        ):
            yield r

    # ------------------------------------------------------------------
    # 指令：推送暂停 / 推送恢复 / 推送总开关
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

    @filter.command("推送总开关", alias={"总开关"})
    @filter.permission_type(PermissionType.ADMIN)
    async def global_switch(self, event: AstrMessageEvent):
        """全局自动推送总开关。用法：/推送总开关 on|off。"""
        arg = self._strip_cmd(event, "推送总开关", "总开关").lower()
        if arg in ("off", "关闭", "关", "0", "false"):
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
            f"🔘 全局自动推送当前为：{state}。\n用法：/推送总开关 on ｜ off（仅管理员）。"
        )

    # ------------------------------------------------------------------
    # 指令：订阅间隔（群级 + 订阅级独立调度）
    # ------------------------------------------------------------------
    @filter.command("订阅间隔")
    @filter.permission_type(PermissionType.ADMIN)
    async def set_interval(self, event: AstrMessageEvent):
        """设置推送调度。用法：
        /订阅间隔 HH:MM|分钟数|off          本会话统一调度
        /订阅间隔 <编号> HH:MM|分钟数|off   某个订阅独立调度（off=恢复跟随群设置）
        """
        arg = self._strip_cmd(event, "订阅间隔")
        g = self._get_group(event)
        if not g["subs"]:
            yield event.plain_result("还没有订阅，先 /订阅 再设置间隔。")
            return
        tokens = arg.split()

        # 订阅级：/订阅间隔 <编号> <值>
        if (
            len(tokens) >= 2
            and tokens[0].isdigit()
            and 1 <= int(tokens[0]) <= len(g["subs"])
        ):
            sub = g["subs"][int(tokens[0]) - 1]
            name = sub.get("name") or self._type_label(sub.get("type", ""))
            msg = self._apply_schedule(sub, tokens[1], own=True)
            self._save()
            self._ensure_task()
            yield event.plain_result(f"⏰ 「{name}」{msg}")
            return

        # 群级
        if not arg:
            digest_t = self._norm_time(g.get("digest_time")) if g.get("digest_time") else None
            if digest_t:
                cur = f"每天 {digest_t} 汇总推送"
            elif g.get("time"):
                cur = f"每天 {g['time']} 定时推送"
            elif (g.get("interval_min") or 0) > 0:
                cur = f"每 {g['interval_min']} 分钟轮询"
            else:
                cur = "已关闭"
            yield event.plain_result(
                f"⏰ 当前本会话调度：{cur}。\n"
                "用法：/订阅间隔 HH:MM（每天定时）｜ /订阅间隔 40（每 40 分钟）｜ "
                "/订阅间隔 off（关闭）；\n"
                "独立调度某个订阅：/订阅间隔 <编号> HH:MM|分钟|off。"
            )
            return
        msg = self._apply_schedule(g, arg, own=False)
        self._save()
        self._ensure_task()
        yield event.plain_result(msg)

    def _apply_schedule(self, target: Dict, val: str, own: bool) -> str:
        """把 HH:MM / 分钟数 / off 应用到群或订阅的调度字段，返回描述文本。"""
        low = val.strip().lower()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        if low in ("off", "关闭", "取消", "0"):
            target.pop("time", None)
            if own:
                # 订阅级 off = 清除独立设置，恢复跟随群调度
                target.pop("interval_min", None)
                target.pop("last_push", None)
                target.pop("last_push_date", None)
                return "已清除独立调度，恢复跟随本会话统一设置。"
            target["interval_min"] = 0
            return "⏰ 已关闭本会话自动推送（手动 /查看 仍可用，重新设置可恢复）。"
        t = self._norm_time(val)
        if t:
            target["time"] = t
            if own:
                target.pop("interval_min", None)
            who = "该订阅" if own else "本会话"
            return f"⏰ 已设置 {who}每天 {t} 定时推送。"
        if val.strip().isdigit():
            n = self._safe_interval(int(val.strip()))
            target["interval_min"] = n
            target.pop("time", None)
            # 切换调度模式时重置计时起点，避免立刻推送一波
            target["last_push"] = now_str
            who = "该订阅" if own else "本会话"
            return f"⏰ 已设置 {who}每 {n} 分钟自动推送。"
        return "⚠️ 时间格式不对：请用 HH:MM（每天定时）、分钟数（轮询间隔）或 off。"

    # ------------------------------------------------------------------
    # 指令：订阅关键词 / 订阅开关 / 订阅汇总
    # ------------------------------------------------------------------
    @filter.command("订阅关键词")
    async def set_keywords(self, event: AstrMessageEvent):
        """设置/清除某个订阅的关键词过滤。用法：/订阅关键词 <编号> 词1,词2 ｜ off"""
        arg = self._strip_cmd(event, "订阅关键词")
        g = self._get_group(event)
        if not g["subs"]:
            yield event.plain_result("📭 没有订阅，先 /订阅 吧。")
            return
        tokens = arg.split(None, 1)
        if not tokens or not tokens[0].isdigit() or not (1 <= int(tokens[0]) <= len(g["subs"])):
            yield event.plain_result(
                "用法：/订阅关键词 <编号> 词1,词2（多个用逗号分隔）\n"
                "清除过滤：/订阅关键词 <编号> off\n查看编号：/我的订阅"
            )
            return
        sub = g["subs"][int(tokens[0]) - 1]
        name = sub.get("name") or self._type_label(sub.get("type", ""))
        rest = tokens[1].strip() if len(tokens) > 1 else ""
        if not rest:
            cur = "、".join(sub.get("keywords") or [])
            yield event.plain_result(f"「{name}」当前关键词：{cur or '（无）'}。")
            return
        if rest.lower() in ("off", "清除", "无", "清空"):
            sub["keywords"] = []
            self._save()
            yield event.plain_result(f"🔍 已清除「{name}」的关键词过滤，恢复推送全部条目。")
            return
        kws = [k.strip() for k in re.split(r"[,，]", rest) if k.strip()]
        sub["keywords"] = kws
        self._save()
        yield event.plain_result(f"🔍 已为「{name}」设置关键词过滤：{'、'.join(kws)}。仅推送标题命中的条目。")

    @filter.command("订阅开关")
    async def sub_switch(self, event: AstrMessageEvent):
        """单独启停某个订阅。用法：/订阅开关 <编号> on|off"""
        arg = self._strip_cmd(event, "订阅开关")
        g = self._get_group(event)
        if not g["subs"]:
            yield event.plain_result("📭 没有订阅，先 /订阅 吧。")
            return
        tokens = arg.split()
        if len(tokens) < 2 or not tokens[0].isdigit() or not (1 <= int(tokens[0]) <= len(g["subs"])):
            yield event.plain_result("用法：/订阅开关 <编号> on|off\n查看编号：/我的订阅")
            return
        sub = g["subs"][int(tokens[0]) - 1]
        name = sub.get("name") or self._type_label(sub.get("type", ""))
        val = tokens[1].lower()
        if val in ("on", "开", "开启", "1", "true"):
            sub["paused"] = False
            sub.pop("auto_paused", None)
            sub["fail_count"] = 0
            self._save()
            yield event.plain_result(f"▶ 已恢复「{name}」的自动推送。")
            return
        if val in ("off", "关", "关闭", "0", "false"):
            sub["paused"] = True
            self._save()
            yield event.plain_result(f"⏸ 已暂停「{name}」的自动推送（手动 /查看 仍可用）。")
            return
        yield event.plain_result("用法：/订阅开关 <编号> on|off")

    @filter.command("订阅汇总")
    @filter.permission_type(PermissionType.ADMIN)
    async def set_digest(self, event: AstrMessageEvent):
        """设置每天一张汇总卡片。用法：/订阅汇总 HH:MM ｜ off"""
        arg = self._strip_cmd(event, "订阅汇总")
        g = self._get_group(event)
        if not g["subs"]:
            yield event.plain_result("还没有订阅，先 /订阅 再设置汇总。")
            return
        if not arg:
            cur = self._norm_time(g.get("digest_time")) if g.get("digest_time") else None
            yield event.plain_result(
                f"📬 当前汇总模式：{'每天 ' + cur if cur else '未开启'}。\n"
                "用法：/订阅汇总 HH:MM（每天定时把所有订阅合并成一张卡片）｜ /订阅汇总 off\n"
                "说明：汇总只包含跟随群调度的订阅；设置了独立调度的订阅仍单独推送。"
            )
            return
        if arg.lower() in ("off", "关闭", "取消"):
            g.pop("digest_time", None)
            g.pop("last_digest_date", None)
            self._save()
            yield event.plain_result("📬 已关闭汇总模式，恢复各订阅按原调度分别推送。")
            return
        t = self._norm_time(arg)
        if not t:
            yield event.plain_result("⚠️ 时间格式不对，请用 24 小时制 HH:MM。")
            return
        g["digest_time"] = t
        self._save()
        self._ensure_task()
        yield event.plain_result(
            f"📬 已设置每天 {t} 推送一张订阅汇总卡片（各来源取前 {self._digest_top_n()} 条）。"
        )

    # ------------------------------------------------------------------
    # 指令：订阅导出 / 导入
    # ------------------------------------------------------------------
    @filter.command("订阅导出")
    async def export_subs(self, event: AstrMessageEvent):
        g = self._get_group(event)
        data = [
            {"type": s.get("type"), "payload": s.get("payload", ""), "keywords": s.get("keywords") or []}
            for s in g["subs"]
            if s.get("type") in _SOURCES
        ]
        if not data:
            yield event.plain_result("📭 没有可导出的订阅。")
            return
        yield event.plain_result(
            "📦 订阅数据如下（迁移到其他会话时用 /订阅导入 <以下JSON>）：\n"
            + json.dumps(data, ensure_ascii=False)
        )

    @filter.command("订阅导入")
    @filter.permission_type(PermissionType.ADMIN)
    async def import_subs(self, event: AstrMessageEvent):
        arg = self._strip_cmd(event, "订阅导入")
        if not arg:
            yield event.plain_result(
                "用法：/订阅导入 <JSON>\n"
                '示例：/订阅导入 [{"type":"weibo","payload":"","keywords":[]}]'
            )
            return
        try:
            data = json.loads(arg)
        except Exception as e:
            yield event.plain_result(f"❌ JSON 解析失败：{e}")
            return
        if not isinstance(data, list):
            yield event.plain_result("❌ 格式不对：需要 JSON 数组。")
            return
        g = self._get_group(event)
        added = skipped = 0
        for it in data:
            if not isinstance(it, dict):
                skipped += 1
                continue
            typ = it.get("type")
            if typ not in _SOURCES:
                typ = self._resolve_type(str(typ or ""))
            if not typ:
                skipped += 1
                continue
            payload = str(it.get("payload") or "").strip()
            if _SOURCES[typ]["needs_payload"] and not re.match(r"^https?://", payload):
                skipped += 1
                continue
            if any(s.get("type") == typ and s.get("payload") == payload for s in g["subs"]):
                skipped += 1
                continue
            if len(g["subs"]) >= MAX_SUBS_PER_GROUP:
                break
            kws = [str(k).strip() for k in (it.get("keywords") or []) if str(k).strip()]
            g["subs"].append(
                {
                    "type": typ,
                    "payload": payload,
                    "seen": [],
                    "name": self._sub_name(typ, payload),
                    "keywords": kws,
                }
            )
            added += 1
        if added:
            self._save()
            self._ensure_task()
            self._record_push_time(g)
        yield event.plain_result(
            f"📥 导入完成：新增 {added} 条，跳过 {skipped} 条（未知类型/缺链接/重复）。"
        )

    # ------------------------------------------------------------------
    # 指令：一键测试所有来源（交付验证用）
    # ------------------------------------------------------------------
    @filter.command("订阅测试")
    async def test_all(self, event: AstrMessageEvent):
        """逐个抓取并推送所有来源，验证各个功能是否可用。用法：/订阅测试。"""
        yield event.plain_result(f"🧪 开始逐个测试全部 {len(_SOURCES)} 类来源，请稍候…")
        umo = event.unified_msg_origin
        subs_todo = []
        for typ, meta in _SOURCES.items():
            if typ == "rss":
                subs_todo.append(
                    {"type": "rss", "payload": DEMO_RSS, "seen": [], "name": "RSS（演示源）", "keywords": []}
                )
            else:
                subs_todo.append(
                    {"type": typ, "payload": "", "seen": [], "name": meta["label"], "keywords": []}
                )
        ok = fail = 0
        detail = []
        for sub in subs_todo:
            label = sub.get("name") or self._type_label(sub["type"])
            try:
                pushed = await self._send_card_to_umo(umo, sub, track=False)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[subhub] 测试 {label} 异常: {e}")
                pushed = False
            if pushed:
                ok += 1
            else:
                fail += 1
            detail.append(f"{'✅' if pushed else '❌'} {label}")
        yield event.plain_result(
            f"🧪 测试完成：成功 {ok} 项 / 失败 {fail} 项\n" + "\n".join(detail)
        )

    # ------------------------------------------------------------------
    # 指令：帮助
    # ------------------------------------------------------------------
    @filter.command("订阅帮助", alias={"订阅说明"})
    async def help(self, event: AstrMessageEvent):
        lines = [
            "📡 统一订阅推送中心 · 12 类来源 + RSS",
            "  热搜榜：微博/百度/知乎/抖音/腾讯/B站/36氪",
            "  资讯列表：Steam特惠/今日番剧/GitHub趋势/HN/V2EX",
            "── 订阅管理 ──",
            "  /订阅 <源> [关键词:词1,词2]   添加订阅",
            "  /订阅 rss <链接> [关键词:...]  订阅任意 RSS/Atom",
            "  /我的订阅     /退订 <编号>",
            "  /订阅关键词 <编号> 词|off      关键词过滤",
            "  /订阅开关 <编号> on|off        单独启停",
            "  /查看 <源>|编号                立即推送一次",
            "── 调度（管理员）──",
            "  /订阅间隔 HH:MM|分钟|off       本会话调度",
            "  /订阅间隔 <编号> HH:MM|分钟    订阅独立调度",
            "  /订阅汇总 HH:MM|off            每天一张汇总卡片",
            "  /推送暂停|恢复    /推送总开关 on|off",
            "── 其他 ──",
            "  /订阅导出      /订阅导入 <json>   /订阅测试",
            "说明：关键词命中标题才推送；订阅连续失败会自动暂停，用 /订阅开关 <编号> on 恢复。",
        ]
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # 抓取与卡片构造
    # ------------------------------------------------------------------
    async def _fetch_filtered(self, sub: Dict, max_items: int) -> List[Dict]:
        """按注册表调用抓取器，并应用关键词过滤。"""
        typ = sub.get("type")
        meta = _SOURCES.get(typ)
        if not meta:
            return []
        fn = getattr(fetchers, meta["fetch"], None)
        if fn is None:
            return []
        payload = sub.get("payload", "") if meta.get("needs_payload") else ""
        res = await fn(payload, max_items=max_items)
        items = res.get("items", []) if isinstance(res, dict) else (res or [])
        kws = sub.get("keywords") or []
        if kws:
            items = [
                i for i in items
                if any(k.lower() in (i.get("title") or "").lower() for k in kws)
            ]
        return items

    def _build_card(self, typ: str, sub: Dict, items: List[Dict]) -> Dict:
        """把抓取结果构造成渲染卡片。"""
        meta = _SOURCES.get(typ, {})
        emoji = meta.get("emoji", "📡")
        if typ == "rss":
            # 仅 RSS 需要按 seen 去重防重推
            new_items = [i for i in items if i.get("id") not in (sub.get("seen") or [])]
            if not new_items:
                return {}
            name = sub.get("name") or "RSS 订阅"
            return {
                "kind": "rss",
                "title": f"📰 {name}",
                "subtitle": f"新更新 {len(new_items)} 条",
                "items": new_items,
            }
        if not items:
            return {}
        title = meta.get("title", f"{emoji} {self._type_label(typ)}")
        return {
            "kind": typ,
            "title": f"{emoji} {title}",
            "subtitle": meta.get("sub_tmpl", "共 {n} 条").format(n=len(items)),
            "items": items,
        }

    def _mark_seen(self, sub: Dict, items: List[Dict]) -> None:
        """仅对 RSS 记录已推送条目的 id，用于去重。"""
        if sub.get("type") != "rss" or not items:
            return
        sub["seen"] = list(sub.get("seen", [])) + [i.get("id") for i in items if i.get("id")]
        sub["seen"] = sub["seen"][-200:]
        self._save()

    async def _push_one(self, event, sub: Dict, manual=False):
        """推送单个订阅（有事件上下文，用于手动 /查看）。"""
        typ = sub.get("type")
        meta = _SOURCES.get(typ)
        if not meta:
            return
        max_items = RSS_MAX_ITEMS if typ == "rss" else self._builtin_top_n()
        try:
            items = await self._fetch_filtered(sub, max_items)
        except Exception as e:
            yield event.plain_result(f"⚠️ {meta['label']} 抓取失败：{e}")
            return

        card = self._build_card(typ, sub, items)
        if not card:
            if manual:
                kws = sub.get("keywords") or []
                hint = "（关键词过滤后暂无命中内容）" if kws else ""
                yield event.plain_result(f"ℹ️ 最近没有新内容。{hint}")
            return

        img = None
        try:
            img = render_image(card)
        except Exception as e:
            logger.error(f"[subhub] 渲染 {typ} 失败: {e}")

        if img:
            async for r in self._send_image(event, img):
                yield r
            self._mark_seen(sub, card.get("items", []))
        else:
            yield event.plain_result(
                f"{card.get('title', '')}（{len(card.get('items', []))} 条）：\n"
                + "\n".join(f"  · {i.get('title', '-')}" for i in card.get("items", []))
            )
            self._mark_seen(sub, card.get("items", []))

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

    @staticmethod
    def _sub_has_own(sub: Dict) -> bool:
        """订阅是否设置了独立调度。"""
        return bool(sub.get("time")) or bool(sub.get("interval_min"))

    def _sub_due(self, sub: Dict, now: datetime, hhmm: str, today: str) -> bool:
        """独立调度订阅是否到期。"""
        t = sub.get("time")
        if t:
            return t == hhmm and sub.get("last_push_date") != today
        iv = sub.get("interval_min") or 0
        if iv <= 0:
            return False
        last = sub.get("last_push")
        if not last:
            return True
        try:
            last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M")
        except ValueError:
            return True
        return (now - last_dt).total_seconds() / 60 >= self._safe_interval(iv)

    def _group_due(self, g: Dict, now: datetime, hhmm: str, today: str) -> bool:
        """跟随群调度的订阅是否到期。"""
        t = g.get("time")
        if t:
            return t == hhmm and g.get("last_push_date") != today
        iv = g.get("interval_min") or 0
        if iv <= 0:
            return False
        last = g.get("last_push")
        if not last:
            return False
        try:
            last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M")
        except ValueError:
            return False
        return (now - last_dt).total_seconds() / 60 >= self._safe_interval(iv)

    async def _scheduler(self):
        while True:
            try:
                await asyncio.sleep(20)
                now = datetime.now()
                in_quiet = self._in_quiet(now)
                quiet_exit = self._quiet_prev and not in_quiet
                self._quiet_prev = in_quiet
                if not self._global_enabled():
                    continue
                if in_quiet:
                    continue  # 静默时段：不推任何内容

                hhmm = f"{now.hour:02d}:{now.minute:02d}"
                today = now.strftime("%Y-%m-%d")
                with self._lock:
                    groups = list(self._subs.items())

                for gkey, g in groups:
                    if gkey == "_global":
                        continue
                    subs = g.get("subs") or []
                    if not subs or g.get("paused"):
                        continue
                    touched = False

                    # 1) 汇总模式：每天一张合并卡片
                    digest_t = self._norm_time(g.get("digest_time")) if g.get("digest_time") else None
                    if digest_t:
                        due = digest_t == hhmm or (
                            quiet_exit and self._quiet_catchup() and digest_t < hhmm
                        )
                        if due and g.get("last_digest_date") != today:
                            await self._push_digest(g)
                            g["last_digest_date"] = today
                            touched = True

                    # 2) 独立调度的订阅单独判断
                    plain_due: List[tuple] = []
                    for idx, sub in enumerate(subs, 1):
                        if sub.get("paused") or sub.get("auto_paused"):
                            continue
                        if self._sub_has_own(sub):
                            if self._sub_due(sub, now, hhmm, today):
                                await self._push_by_umo(g, sub, idx)
                                sub["last_push"] = now.strftime("%Y-%m-%d %H:%M")
                                sub["last_push_date"] = today
                                touched = True
                        elif not digest_t:
                            plain_due.append((idx, sub))

                    # 3) 跟随群调度的订阅统一推送
                    if plain_due and self._group_due(g, now, hhmm, today):
                        for idx, sub in plain_due:
                            await self._push_by_umo(g, sub, idx)
                        g["last_push"] = now.strftime("%Y-%m-%d %H:%M")
                        g["last_push_date"] = today
                        touched = True

                    if touched:
                        self._save()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[subhub] 调度异常: {e}")

    async def _push_by_umo(self, g: Dict, sub: Dict, idx=None) -> bool:
        """主动推送：无事件上下文，通过 context.send_message 发送到会话。"""
        umo = g.get("umo")
        if not umo:
            return False
        return await self._send_card_to_umo(umo, sub, idx=idx)

    async def _send_card_to_umo(self, umo: str, sub: Dict, idx=None, track: bool = True) -> bool:
        """抓取、渲染并把订阅卡片发到指定会话。图片优先，失败回退纯文本。
        track=True 时统计抓取失败次数，达到阈值自动暂停该订阅并通知。"""
        typ = sub.get("type")
        meta = _SOURCES.get(typ)
        if not meta:
            return False
        max_items = RSS_MAX_ITEMS if typ == "rss" else self._builtin_top_n()
        try:
            items = await self._fetch_filtered(sub, max_items)
        except Exception as e:
            logger.error(f"[subhub] {meta['label']} 抓取失败: {e}")
            if track:
                self._note_fail(sub, umo, idx)
            return False
        if track:
            self._reset_fail(sub)

        card = self._build_card(typ, sub, items)
        if not card:
            return False

        img = None
        try:
            img = render_image(card)
        except Exception as e:
            logger.error(f"[subhub] 渲染 {typ} 失败: {e}")

        try:
            if img:
                await self.context.send_message(umo, MessageChain([self._img_comp(img)]))
            else:
                await self.context.send_message(
                    umo,
                    MessageChain().message(
                        f"{card.get('title', '')}（{len(card.get('items', []))} 条）：\n"
                        + "\n".join(f"  · {i.get('title', '-')}" for i in card["items"])
                    ),
                )
            self._mark_seen(sub, card.get("items", []))
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[subhub] 主动发送失败: {e}")
            return False

    # ------------------------------------------------------------------
    # 失败自愈
    # ------------------------------------------------------------------
    def _note_fail(self, sub: Dict, umo: str, idx) -> None:
        """记录一次抓取失败；连续达到阈值自动暂停并通知会话。"""
        try:
            n = int(sub.get("fail_count", 0)) + 1
            sub["fail_count"] = n
            limit = self._auto_pause_after()
            if limit > 0 and n >= limit and not sub.get("auto_paused"):
                sub["auto_paused"] = True
                self._save()
                name = sub.get("name") or self._type_label(sub.get("type", ""))
                pos = f"（编号 {idx}）" if idx else ""
                tip = f"/订阅开关 {idx} on" if idx else "/订阅开关 <编号> on"
                asyncio.get_running_loop().create_task(self._notify_fail(umo, name, n, pos, tip))
            else:
                self._save()
        except Exception as e:
            logger.error(f"[subhub] 记录失败次数异常: {e}")

    async def _notify_fail(self, umo: str, name: str, n: int, pos: str, tip: str) -> None:
        try:
            await self.context.send_message(
                umo,
                MessageChain().message(
                    f"⛔ 订阅「{name}」{pos}已连续失败 {n} 次，自动暂停推送。\n"
                    f"稍后可用 {tip} 恢复（恢复会重置失败计数）。"
                ),
            )
        except Exception as e:
            logger.error(f"[subhub] 发送失败通知异常: {e}")

    def _reset_fail(self, sub: Dict) -> None:
        if sub.get("fail_count"):
            sub["fail_count"] = 0
            self._save()

    # ------------------------------------------------------------------
    # 每日汇总
    # ------------------------------------------------------------------
    async def _push_digest(self, g: Dict) -> None:
        """把本会话所有有效订阅合并成一张汇总卡片推送。"""
        umo = g.get("umo")
        if not umo:
            return
        top_n = self._digest_top_n()
        sections = []
        for sub in g.get("subs") or []:
            if sub.get("paused") or sub.get("auto_paused"):
                continue
            if self._sub_has_own(sub):
                continue  # 独立调度的订阅不进汇总，仍单独推送
            typ = sub.get("type")
            meta = _SOURCES.get(typ)
            if not meta:
                continue
            try:
                items = await self._fetch_filtered(sub, top_n)
            except Exception as e:
                logger.error(f"[subhub] 汇总抓取 {meta['label']} 失败: {e}")
                self._note_fail(sub, umo, None)
                continue
            self._reset_fail(sub)
            if not items:
                continue
            sections.append({"title": f"{meta['emoji']} {meta['label']}", "items": items[:top_n]})
            self._mark_seen(sub, items[:top_n])
        if not sections:
            return
        card = {
            "kind": "digest",
            "title": "📬 订阅汇总",
            "subtitle": f"{len(sections)} 个来源 · " + datetime.now().strftime("%m-%d %H:%M"),
            "sections": sections,
        }
        try:
            img = render_image(card)
        except Exception as e:
            logger.error(f"[subhub] 渲染汇总失败: {e}")
            img = None
        try:
            if img:
                await self.context.send_message(umo, MessageChain([self._img_comp(img)]))
            else:
                lines = [card["title"] + " · " + card["subtitle"]]
                for s in sections:
                    lines.append(f"\n{s['title']}")
                    lines.extend(f"  · {i.get('title', '-')}" for i in s["items"])
                await self.context.send_message(umo, MessageChain().message("\n".join(lines)))
        except Exception as e:
            logger.error(f"[subhub] 发送汇总失败: {e}")

"""
统一订阅推送中心 - 数据抓取器

RSS 订阅抓取与时下热点(微博热搜)抓取。
统一返回条目结构：
    { "title": str, "url": str, "id": str, "time": str opt, "hot": str opt }

网络请求使用 aiohttp 异步库（遵循 AstrBot 官方规范，不使用 requests）。
所有网络异常向上抛出，由调用方兜底。
"""

from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List

try:
    import aiohttp
    _AIOHTTP_OK = True
except Exception:  # pragma: no cover 依赖缺失时插件仍可加载，命令会提示安装
    aiohttp = None
    _AIOHTTP_OK = False

_TIMEOUT = aiohttp.ClientTimeout(total=12) if _AIOHTTP_OK else 12

# 全局限流：避免被打风控，保留最近一次请求时间戳（进程内）
_last_req = {"ts": 0.0}
_req_lock = asyncio.Lock()


async def _throttle(min_interval: float = 1.5) -> None:
    async with _req_lock:
        now = asyncio.get_event_loop().time()
        gap = now - _last_req["ts"]
        if gap < min_interval:
            await asyncio.sleep(min_interval - gap)
        _last_req["ts"] = now


_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36"
)


def _check_deps() -> None:
    if not _AIOHTTP_OK:
        raise RuntimeError(
            "缺少依赖 aiohttp，请先执行：pip install -r requirements.txt"
        )


async def _get_json(url: str, headers: Dict | None = None, timeout: int = 12):
    """GET 并解析 JSON。返回 dict/list 或抛异常。"""
    _check_deps()
    await _throttle()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.get(
            url, headers=headers or {"User-Agent": _UA}, ssl=False
        ) as resp:
            resp.raise_for_status()
            return await resp.json(content_type=None)


async def _get_text(url: str, headers: Dict | None = None, timeout: int = 12) -> str:
    _check_deps()
    await _throttle()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.get(
            url, headers=headers or {"User-Agent": _UA}, ssl=False
        ) as resp:
            resp.raise_for_status()
            return await resp.text(errors="ignore")


# ---------------------------------------------------------------------------
# RSS / Atom 解析
# ---------------------------------------------------------------------------
_ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


async def fetch_rss(url: str, timeout: int = 10, max_items: int = 10) -> List[Dict]:
    """抓取并解析 RSS / Atom 流，返回最新条目列表。"""
    headers = {"User-Agent": "AstrBot-SubHub/1.0 (+RSS Puller)"}

    await _throttle()
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        async with session.get(url, headers=headers, ssl=False) as resp:
            resp.raise_for_status()
            raw = await resp.read()

    if not raw:
        return []

    # RSS/Atom 判定：根标签 <feed> 为 Atom
    head = raw[:512].lower()
    is_atom = b"<feed" in head

    try:
        root = ET.fromstring(raw[: 1 << 22])
    except ET.ParseError:
        return []

    items: List[Dict] = []
    if is_atom:
        for e in root.findall("atom:entry", _ATOM_NS)[:max_items]:
            title = _elem_text(e, "atom:title") or "无标题"
            link = e.find("atom:link", _ATOM_NS)
            href = (link.get("href") if link is not None else "") or ""
            pub = _elem_text(e, "atom:published") or _elem_text(e, "atom:updated")
            items.append(
                {"title": title.strip(), "url": href, "id": title or href, "time": _parse_time(pub)}
            )
    else:
        for e in root.findall(".//item")[:max_items]:
            title = _elem_text(e, "title") or ""
            link = _elem_text(e, "link") or ""
            desc = _elem_text(e, "description")
            pub = _elem_text(e, "pubDate") or _elem_text(e, "dc:date")
            items.append(
                {
                    "title": (title.strip() or "无标题"),
                    "url": link,
                    "id": title or link or desc,
                    "time": _parse_time(pub),
                }
            )
    # 去空 & 按 id 去重
    seen: set = set()
    out: List[Dict] = []
    for it in items:
        key = it["id"]
        if key and key not in seen:
            seen.add(key)
            out.append(it)
    return out


def _elem_text(elem, tag: str) -> str:
    found = elem.find(tag, _ATOM_NS)
    return (found.text or "") if found is not None else ""


def _parse_time(s: str) -> str:
    if not s:
        return ""
    try:
        dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%m-%d %H:%M")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %z")
        return dt.astimezone().strftime("%m-%d %H:%M")
    except ValueError:
        pass
    return ""


# ---------------------------------------------------------------------------
# 微博热搜
# ---------------------------------------------------------------------------
_WEIBO_API = "https://weibo.com/ajax/side/hotSearch"
_LABELS = {"hot": "热", "new": "新", "boom": "爆", "recomm": "荐"}

_WEIBO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
    "Referer": "https://weibo.com/hot/search",
}


async def fetch_weibo_hot(max_items: int = 30) -> Dict:
    """抓取微博实时热搜榜。返回 {items, update} 结构。"""
    await _throttle(min_interval=3.0)
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        async with session.get(_WEIBO_API, headers=_WEIBO_HEADERS, ssl=False) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

    items: List[Dict] = []
    try:
        realtime = data.get("data", {}).get("realtime", []) or []
        for it in realtime[:max_items]:
            title = it.get("word", "")
            if not title:
                continue
            hot = str(it.get("num", ""))
            label = _LABELS.get(it.get("label_name", ""), "")
            if not label and it.get("is_hot") == 1:
                label = "热"
                if not it.get("num"):
                    hot = "1"
            items.append(
                {
                    "rank": len(items) + 1,
                    "title": title,
                    "url": f"https://s.weibo.com/weibo?q=%23{title}%23",
                    "id": f"weibo:{title}",
                    "hot": hot,
                    "hot_label": label,
                }
            )
    except Exception:
        items = []
    return {"items": items, "update": datetime.now().strftime("%H:%M")}


# ---------------------------------------------------------------------------
# 百度热搜
# ---------------------------------------------------------------------------
_BAIDU_API = "https://top.baidu.com/api/board?platform=wise&tab=realtime"


async def fetch_baidu_hot(max_items: int = 30) -> List[Dict]:
    """抓取百度实时热搜。条目：{rank,title,url,id,hot}"""
    data = await _get_json(_BAIDU_API, headers={"User-Agent": _UA})
    # 结构：data.cards[].content[].content[] 才是热搜列表
    cards = (data.get("data", {}) or {}).get("cards", []) or []
    items: List[Dict] = []
    for card in cards:
        for c in (card.get("content", []) or []):
            inner = c.get("content", []) if isinstance(c, dict) else []
            for it_inner in (inner or []):
                if isinstance(it_inner, dict):
                    title = it_inner.get("word") or ""
                    if not title:
                        continue
                    hot = it_inner.get("hotScore") or ""
                    items.append(
                        {
                            "rank": len(items) + 1,
                            "title": title,
                            "url": it_inner.get("url") or "",
                            "id": f"baidu:{title}",
                            "hot": str(hot) if hot else "",
                        }
                    )
                    if len(items) >= max_items:
                        return items[:max_items]
    return items[:max_items]


# ---------------------------------------------------------------------------
# 腾讯新闻热榜
# ---------------------------------------------------------------------------
_TENCENT_API = "https://r.inews.qq.com/gw/event/hot_ranking_list?page_size=30&type=hot"


async def fetch_tencent_news(max_items: int = 30) -> List[Dict]:
    """抓取腾讯新闻热榜。条目：{rank,title,url,id,hot}"""
    data = await _get_json(_TENCENT_API, headers={"User-Agent": _UA})
    items: List[Dict] = []
    for d in (data.get("idlist") or []):
        for n in (d.get("newslist") or []):
            title = n.get("title") or ""
            if not title:
                continue
            # 过滤固定推荐位/广告占位（标题过长且非新闻）
            hot_event = n.get("hotEvent") if isinstance(n.get("hotEvent"), dict) else {}
            score = hot_event.get("hotScore") if hot_event else None
            items.append(
                {
                    "rank": 0,  # 稍后统一编号
                    "title": title,
                    "url": n.get("url") or "",
                    "id": f"tencent:{n.get('id') or title}",
                    "hot": str(score) if score else "",
                }
            )
            if len(items) >= max_items:
                break
        if len(items) >= max_items:
            break
    # 编号 rank
    for i, it in enumerate(items, 1):
        it["rank"] = i
    return items[:max_items]


# ---------------------------------------------------------------------------
# B 站热门视频
# ---------------------------------------------------------------------------
_BILI_API = "https://api.bilibili.com/x/web-interface/popular"


async def fetch_bili_hot(max_items: int = 30) -> List[Dict]:
    """抓取 B 站热门视频。条目：{rank,title,url,id,hot}"""
    data = await _get_json(_BILI_API, headers={"User-Agent": _UA})
    vids = (data.get("data", {}) or {}).get("list", []) or []
    items: List[Dict] = []
    for v in vids[:max_items]:
        title = v.get("title") or ""
        if not title:
            continue
        hot = v.get("stat", {}).get("view") or ""
        items.append(
            {
                "rank": len(items) + 1,
                "title": title,
                "url": f"https://www.bilibili.com/video/{v.get('bvid')}" if v.get("bvid") else "",
                "id": f"bili:{v.get('bvid') or title}",
                "hot": _fmt_count(hot) if isinstance(hot, int) else str(hot),
            }
        )
    return items[:max_items]


def _fmt_count(n) -> str:
    """把整数播放量格式化为 '万' 单位。"""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n)
    if n >= 10000:
        return f"{n / 10000:.1f}万".rstrip("0").rstrip(".")
    return str(n)


# ---------------------------------------------------------------------------
# Steam 特别优惠
# ---------------------------------------------------------------------------
_STEAM_API = "https://store.steampowered.com/api/featuredcategories/"


async def fetch_steam_specials(max_items: int = 20) -> List[Dict]:
    """抓取 Steam 特惠。条目：{title,url,id,meta}"""
    data = await _get_json(
        _STEAM_API,
        headers={"User-Agent": _UA, "Referer": "https://store.steampowered.com/"},
    )
    spec = (data.get("specials") or {}).get("items", []) or []
    items: List[Dict] = []
    for s in spec[:max_items]:
        name = s.get("name") or ""
        if not name:
            continue
        discount = s.get("discount_percent")
        final = s.get("final_price")
        price = f"{final / 100:.2f}" if isinstance(final, (int, float)) else ""
        meta = []
        if discount:
            meta.append(f"-{discount}%")
        if price and price != "0.00":
            meta.append(f"¥{price}")
        items.append(
            {
                "title": name,
                "url": f"https://store.steampowered.com/app/{s.get('id')}" if s.get("id") else "",
                "id": f"steam:{s.get('id') or name}",
                "meta": " ".join(meta),
            }
        )
    return items[:max_items]


# ---------------------------------------------------------------------------
# 番剧更新时间表（B 站番剧）
# ---------------------------------------------------------------------------
_BAN_HEADERS = {
    "User-Agent": _UA,
    "Referer": "https://www.bilibili.com/",
}


async def fetch_bangumi(max_items: int = 30) -> Dict:
    """抓取 B 站番剧当日更新。返回 {items, date} 结构。

    条目：{title,url,id,meta}，meta 为连载/集数信息。
    """
    url = "https://bangumi.bilibili.com/web_api/timeline_global"
    data = await _get_json(url, headers=_BAN_HEADERS)
    result = data.get("result") or []
    # 取今天的更新（is_today=1），若找不到则取第一条
    today_block = next((x for x in result if x.get("is_today") == 1), result[0] if result else {})
    date = today_block.get("date") or ""
    items: List[Dict] = []
    for ep in (today_block.get("seasons") or [])[:max_items]:
        title = ep.get("title") or ""
        if not title:
            continue
        meta = "  ".join(
            x for x in [ep.get("pub_index") or "", ep.get("pub_time") or ""] if x
        )
        items.append(
            {
                "title": title,
                "url": ep.get("url") or "",
                "id": f"bangumi:{ep.get('season_id') or title}",
                "meta": meta,
            }
        )
    return {"items": items, "date": date}
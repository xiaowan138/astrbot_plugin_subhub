"""
统一订阅推送中心 - 数据抓取器

12 类内置源 + 任意 RSS/Atom 抓取：
- 热搜榜：微博、百度、知乎、抖音、腾讯新闻、B站、36氪
- 信息列表：Steam 特惠、B站番剧更新、GitHub Trending、Hacker News、V2EX

统一约定：
- 所有 fetcher 签名为 async def fetch_xxx(payload: str = "", max_items: int = 20)
- 返回 List[Dict]（条目列表）或 {"items": [...], ...}（附带头部信息）
- 条目结构：{ "title", "url", "id", "rank"?, "hot"?, "hot_label"?, "meta"?, "time"?, "desc"? }

网络请求使用 aiohttp 异步库（遵循 AstrBot 官方规范，不使用 requests）。
所有网络异常向上抛出，由调用方兜底。
"""

from __future__ import annotations

import asyncio
import html as _html
import re
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

# 全局限流：避免请求过密触发风控（进程内共享）
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
        raise RuntimeError("缺少依赖 aiohttp，请先执行：pip install -r requirements.txt")


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


async def _post_json(url: str, body: Dict, headers: Dict | None = None, timeout: int = 12):
    """POST JSON 并解析响应。返回 dict/list 或抛异常。"""
    _check_deps()
    await _throttle()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.post(
            url, json=body, headers=headers or {"User-Agent": _UA}, ssl=False
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


def _fmt_count(n) -> str:
    """把整数格式化为 '万' 单位可读文本。"""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n or "")
    if n >= 10000:
        return f"{n / 10000:.1f}万".rstrip("0").rstrip(".")
    return str(n)


def _strip_html(s: str) -> str:
    """去掉 HTML 标签与实体，压平空白。"""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


# ---------------------------------------------------------------------------
# RSS / Atom 解析
# ---------------------------------------------------------------------------
_ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


async def fetch_rss(payload: str = "", max_items: int = 10) -> List[Dict]:
    """抓取并解析 RSS / Atom 流，返回最新条目列表（含摘要 desc）。"""
    url = (payload or "").strip()
    if not re.match(r"^https?://", url):
        raise ValueError("RSS 链接必须以 http(s):// 开头")
    _check_deps()
    headers = {"User-Agent": "AstrBot-SubHub/1.1 (+RSS Puller)"}
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
            desc = _strip_html(_elem_text(e, "atom:summary"))[:90]
            items.append(
                {"title": title.strip(), "url": href, "id": title or href,
                 "time": _parse_time(pub), "desc": desc}
            )
    else:
        for e in root.findall(".//item")[:max_items]:
            title = _elem_text(e, "title") or ""
            link = _elem_text(e, "link") or ""
            desc = _strip_html(_elem_text(e, "description"))[:90]
            pub = _elem_text(e, "pubDate") or _elem_text(e, "dc:date")
            items.append(
                {"title": (title.strip() or "无标题"), "url": link,
                 "id": title or link or desc, "time": _parse_time(pub), "desc": desc}
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
    "User-Agent": _UA,
    "Referer": "https://weibo.com/hot/search",
}


async def fetch_weibo_hot(payload: str = "", max_items: int = 30) -> Dict:
    """抓取微博实时热搜榜。返回 {items, update} 结构。"""
    _check_deps()
    await _throttle(min_interval=3.0)
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        async with session.get(_WEIBO_API, headers=_WEIBO_HEADERS, ssl=False) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

    if not isinstance(data, dict):
        raise RuntimeError("微博接口返回结构异常")
    items: List[Dict] = []
    realtime = (data.get("data") or {}).get("realtime") or []
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
    return {"items": items, "update": datetime.now().strftime("%H:%M")}


# ---------------------------------------------------------------------------
# 百度热搜
# ---------------------------------------------------------------------------
_BAIDU_API = "https://top.baidu.com/api/board?platform=wise&tab=realtime"


async def fetch_baidu_hot(payload: str = "", max_items: int = 30) -> List[Dict]:
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
# 知乎热榜
# ---------------------------------------------------------------------------
_ZHIHU_API = "https://api.zhihu.com/topstory/hot-lists/total?limit=50"


async def fetch_zhihu_hot(payload: str = "", max_items: int = 30) -> List[Dict]:
    """抓取知乎热榜。条目：{rank,title,url,id,hot}"""
    data = await _get_json(_ZHIHU_API, headers={"User-Agent": _UA})
    items: List[Dict] = []
    for it in (data.get("data") or [])[:max_items]:
        target = it.get("target") or {}
        title = (target.get("title") or "").strip()
        if not title:
            continue
        hot = (it.get("detail_text") or "").replace(" ", "")
        hot = hot.replace("万热度", "万").replace("热度", "").strip()
        qid = target.get("id")
        items.append(
            {
                "rank": len(items) + 1,
                "title": title,
                "url": f"https://www.zhihu.com/question/{qid}" if qid else "",
                "id": f"zhihu:{qid or title}",
                "hot": hot,
            }
        )
    return items


# ---------------------------------------------------------------------------
# 抖音热搜
# ---------------------------------------------------------------------------
_DOUYIN_API = "https://www.iesdouyin.com/web/api/v2/hotsearch/billboard/word/"


async def fetch_douyin_hot(payload: str = "", max_items: int = 30) -> List[Dict]:
    """抓取抖音热搜词。条目：{rank,title,url,id,hot}"""
    data = await _get_json(_DOUYIN_API, headers={"User-Agent": _UA})
    items: List[Dict] = []
    for it in (data.get("word_list") or [])[:max_items]:
        title = (it.get("word") or "").strip()
        if not title:
            continue
        hot = it.get("hot_value")
        items.append(
            {
                "rank": len(items) + 1,
                "title": title,
                "url": "",
                "id": f"douyin:{title}",
                "hot": _fmt_count(hot) if isinstance(hot, (int, float)) else str(hot or ""),
            }
        )
    return items


# ---------------------------------------------------------------------------
# 腾讯新闻热榜
# ---------------------------------------------------------------------------
_TENCENT_API = "https://r.inews.qq.com/gw/event/hot_ranking_list?page_size=30&type=hot"


async def fetch_tencent_news(payload: str = "", max_items: int = 30) -> List[Dict]:
    """抓取腾讯新闻热榜。条目：{rank,title,url,id,hot}"""
    data = await _get_json(_TENCENT_API, headers={"User-Agent": _UA})
    items: List[Dict] = []
    for d in (data.get("idlist") or []):
        for n in (d.get("newslist") or []):
            # 过滤占位横幅（如"每10分钟更新一次"的 TIP 条目，无 url 字段）
            if not n.get("url") or str(n.get("id", "")).startswith("TIP"):
                continue
            title = n.get("title") or ""
            if not title:
                continue
            hot_event = n.get("hotEvent") if isinstance(n.get("hotEvent"), dict) else {}
            score = hot_event.get("hotScore") if hot_event else None
            items.append(
                {
                    "rank": 0,
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
    for i, it in enumerate(items, 1):
        it["rank"] = i
    return items[:max_items]


# ---------------------------------------------------------------------------
# B 站热门视频
# ---------------------------------------------------------------------------
_BILI_API = "https://api.bilibili.com/x/web-interface/popular"


async def fetch_bili_hot(payload: str = "", max_items: int = 30) -> List[Dict]:
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


# ---------------------------------------------------------------------------
# 36氪热榜
# ---------------------------------------------------------------------------
_KR36_API = "https://gateway.36kr.com/api/mis/nav/home/nav/rank/hot"


async def fetch_36kr(payload: str = "", max_items: int = 20) -> List[Dict]:
    """抓取 36氪热榜（阅读数为热度）。条目：{rank,title,url,id,hot}"""
    body = {
        "partner_id": "wap",
        "param": {"siteId": 1, "platformId": 2},
        "timestamp": int(datetime.now().timestamp() * 1000),
    }
    data = await _post_json(_KR36_API, body, headers={"User-Agent": _UA})
    rank = ((data.get("data") or {}).get("hotRankList")) or []
    items: List[Dict] = []
    for it in rank[:max_items]:
        mat = it.get("templateMaterial") or {}
        title = (mat.get("widgetTitle") or "").strip()
        if not title:
            continue
        item_id = mat.get("itemId") or it.get("itemId")
        read = mat.get("statRead")
        items.append(
            {
                "rank": len(items) + 1,
                "title": title,
                "url": f"https://www.36kr.com/p/{item_id}" if item_id else "",
                "id": f"kr36:{item_id or title}",
                "hot": _fmt_count(read) if isinstance(read, (int, float)) else "",
            }
        )
    return items


# ---------------------------------------------------------------------------
# Steam 特别优惠
# ---------------------------------------------------------------------------
_STEAM_API = "https://store.steampowered.com/api/featuredcategories/"

# 常见货币符号（跟随接口返回的 currency 字段，随机器人所在地区自动适配）
_CUR_SYMBOLS = {
    "usd": "$", "cny": "¥", "jpy": "¥", "eur": "€", "gbp": "£",
    "krw": "₩", "rub": "₽", "twd": "NT$", "hkd": "HK$", "brl": "R$",
    "inr": "₹", "aud": "A$", "cad": "C$", "sgd": "S$", "thb": "฿",
}


def _price_text(final, currency: str) -> str:
    """把 Steam 分单位价格格式化为带货币符号的文本。"""
    if not isinstance(final, (int, float)):
        return ""
    cur = (currency or "").lower()
    symbol = _CUR_SYMBOLS.get(cur, f"{currency} " if currency else "")
    cents = int(final)
    if cents % 100 == 0:
        return f"{symbol}{cents // 100:,}"
    return f"{symbol}{cents / 100:.2f}"


async def fetch_steam_specials(payload: str = "", max_items: int = 20) -> List[Dict]:
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
        meta = []
        if discount:
            meta.append(f"-{discount}%")
        price = _price_text(s.get("final_price"), s.get("currency") or "")
        if price:
            meta.append(price)
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


async def fetch_bangumi(payload: str = "", max_items: int = 30) -> Dict:
    """抓取 B 站番剧当日更新。返回 {items, date} 结构。"""
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


# ---------------------------------------------------------------------------
# GitHub Trending
# ---------------------------------------------------------------------------
_GH_TRENDING = "https://github.com/trending"


async def fetch_github_trending(payload: str = "", max_items: int = 15) -> List[Dict]:
    """抓取 GitHub Trending 仓库（解析 trending 页面）。条目：{title,url,id,meta}"""
    html = await _get_text(_GH_TRENDING, headers={"User-Agent": _UA})
    repos = re.findall(r'<h2[^>]*>\s*<a[^>]+href="/([^"]+)"', html)
    stars = re.findall(r"([\d,]+)\s+stars today", html)
    items: List[Dict] = []
    for repo, st in zip(repos, stars):
        if "/" not in repo:
            continue
        items.append(
            {
                "title": repo,
                "url": f"https://github.com/{repo}",
                "id": f"gh:{repo}",
                "meta": f"+{st.replace(',', '')}★",
            }
        )
        if len(items) >= max_items:
            break
    return items


# ---------------------------------------------------------------------------
# Hacker News
# ---------------------------------------------------------------------------
async def fetch_hn(payload: str = "", max_items: int = 20) -> List[Dict]:
    """抓取 Hacker News 首页热帖（Algolia API）。条目：{title,url,id,meta}"""
    url = f"https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage={max_items}"
    data = await _get_json(url, headers={"User-Agent": _UA})
    items: List[Dict] = []
    for h in (data.get("hits") or [])[:max_items]:
        title = (h.get("title") or "").strip()
        if not title:
            continue
        pts = h.get("points") or 0
        oid = h.get("objectID")
        items.append(
            {
                "title": title,
                "url": h.get("url") or (f"https://news.ycombinator.com/item?id={oid}" if oid else ""),
                "id": f"hn:{oid or title}",
                "meta": f"{pts}分",
            }
        )
    return items


# ---------------------------------------------------------------------------
# V2EX 热门
# ---------------------------------------------------------------------------
_V2EX_API = "https://www.v2ex.com/api/topics/hot.json"


async def fetch_v2ex(payload: str = "", max_items: int = 20) -> List[Dict]:
    """抓取 V2EX 热门话题。条目：{title,url,id,meta}"""
    data = await _get_json(_V2EX_API, headers={"User-Agent": _UA})
    items: List[Dict] = []
    for t in (data or [])[:max_items]:
        title = (t.get("title") or "").strip()
        if not title:
            continue
        items.append(
            {
                "title": title,
                "url": t.get("url") or "",
                "id": f"v2ex:{t.get('id') or title}",
                "meta": f"{t.get('replies') or 0}回复",
            }
        )
    return items

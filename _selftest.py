"""
全功能自测脚本（不依赖 AstrBot，直接在插件目录运行）：

    python _selftest.py

覆盖：
- 12 类内置源 + RSS 演示源的真实抓取与数据结构校验
- 每类源渲染成图片卡片并落盘到 out/
- 订阅汇总（digest）卡片渲染
- 明暗双主题渲染
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import fetchers
from utils import render_image, set_theme

OUT = Path(__file__).resolve().parent / "out"
OUT.mkdir(exist_ok=True)

DEMO_RSS = "https://www.ruanyifeng.com/blog/atom.xml"

# kind, 显示名, emoji, 副标题模板, fetcher 函数, 渲染风格(board/list)
KINDS = [
    ("weibo", "微博热搜", "🔥", "实时 · 共 {n} 条", "fetch_weibo_hot"),
    ("baidu", "百度热搜", "🍃", "实时 · 共 {n} 条", "fetch_baidu_hot"),
    ("zhihu", "知乎热榜", "💙", "实时 · 共 {n} 条", "fetch_zhihu_hot"),
    ("douyin", "抖音热搜", "🎵", "实时 · 共 {n} 条", "fetch_douyin_hot"),
    ("tencent", "腾讯新闻热榜", "📰", "实时 · 共 {n} 条", "fetch_tencent_news"),
    ("bili", "B站热门", "🎬", "实时 · 共 {n} 条", "fetch_bili_hot"),
    ("kr36", "36氪热榜", "💼", "科技快讯 · 共 {n} 条", "fetch_36kr"),
    ("steam", "Steam 特惠", "🎮", "限时特惠 · 共 {n} 款", "fetch_steam_specials"),
    ("bangumi", "今日更新番剧", "🍙", "今日更新 · 共 {n} 部", "fetch_bangumi"),
    ("github", "GitHub Trending", "🐙", "今日趋势 · 共 {n} 仓库", "fetch_github_trending"),
    ("hn", "Hacker News", "🟠", "首页热帖 · 共 {n} 条", "fetch_hn"),
    ("v2ex", "V2EX 热门", "💬", "热门话题 · 共 {n} 条", "fetch_v2ex"),
]

REQUIRED_FIELDS = ("title", "url", "id")


def check_items(kind: str, items) -> str:
    if not isinstance(items, list) or not items:
        return "EMPTY"
    for i, it in enumerate(items[:3]):
        for f in REQUIRED_FIELDS:
            if f not in it:
                return f"第{i + 1}条缺字段 {f}"
        if not it.get("title"):
            return f"第{i + 1}条 title 为空"
    return "OK"


async def main():
    ok = fail = 0
    sections = []

    for kind, title, emoji, tmpl, fn_name in KINDS:
        fn = getattr(fetchers, fn_name)
        try:
            res = await fn("", max_items=12)
            items = res.get("items", []) if isinstance(res, dict) else res
            status = check_items(kind, items)
            if status != "OK":
                raise RuntimeError(f"数据校验失败: {status}")
            card = {
                "kind": kind,
                "title": f"{emoji} {title}",
                "subtitle": tmpl.format(n=len(items)),
                "items": items,
            }
            img = render_image(card)
            if not img:
                raise RuntimeError("渲染返回空")
            (OUT / f"{kind}.png").write_bytes(img)
            first = items[0]
            print(f"[PASS] {title}: {len(items)} 条 · 首条={first.get('title', '')[:28]} "
                  f"hot={first.get('hot', first.get('meta', ''))}")
            if items:
                sections.append({"title": f"{emoji} {title}", "items": items[:3]})
            ok += 1
        except Exception as e:
            print(f"[FAIL] {title}: {type(e).__name__}: {e}")
            fail += 1

    # RSS 演示源
    try:
        items = await fetchers.fetch_rss(DEMO_RSS, max_items=8)
        status = check_items("rss", items)
        if status != "OK":
            raise RuntimeError(f"数据校验失败: {status}")
        card = {
            "kind": "rss",
            "title": "📰 Ruan Yifeng's Blog",
            "subtitle": f"新更新 {len(items)} 条",
            "items": items,
        }
        img = render_image(card)
        if not img:
            raise RuntimeError("渲染返回空")
        (OUT / "rss.png").write_bytes(img)
        desc_ok = any(it.get("desc") for it in items)
        print(f"[PASS] RSS（演示源）: {len(items)} 条 · 摘要行={'有' if desc_ok else '无'} · "
              f"首条={items[0].get('title', '')[:28]}")
        if items:
            sections.append({"title": "📰 RSS（演示源）", "items": items[:3]})
        ok += 1
    except Exception as e:
        print(f"[FAIL] RSS（演示源）: {type(e).__name__}: {e}")
        fail += 1

    # 汇总卡片
    if sections:
        try:
            card = {
                "kind": "digest",
                "title": "📬 订阅汇总",
                "subtitle": f"{len(sections)} 个来源",
                "sections": sections,
            }
            img = render_image(card)
            if not img:
                raise RuntimeError("渲染返回空")
            (OUT / "digest.png").write_bytes(img)
            print(f"[PASS] 汇总卡片: {len(sections)} 个分区")
            ok += 1
        except Exception as e:
            print(f"[FAIL] 汇总卡片: {type(e).__name__}: {e}")
            fail += 1

    # 浅色主题抽查
    try:
        set_theme("light")
        if sections:
            card = {
                "kind": "digest",
                "title": "📬 订阅汇总",
                "subtitle": "light theme",
                "sections": sections,
            }
            img = render_image(card)
            if not img:
                raise RuntimeError("渲染返回空")
            (OUT / "theme_light.png").write_bytes(img)
            set_theme("dark")
            print("[PASS] 浅色主题渲染")
            ok += 1
    except Exception as e:
        set_theme("dark")
        print(f"[FAIL] 浅色主题渲染: {type(e).__name__}: {e}")
        fail += 1

    print(f"\n===== 自测结果：成功 {ok} / 失败 {fail} =====")
    print(f"渲染图片输出目录: {OUT}")
    raise SystemExit(0 if fail == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())

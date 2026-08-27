"""
统一订阅推送中心 - 全功能自测脚本（交付前验证用）

逐个抓取所有来源 -> 构造卡片 -> 渲染成 PNG 图片，验证抓取与渲染链路，
并把产物保存到 out/ 目录供人工核对。

运行：python _selftest.py
"""

import asyncio
import json
import os
from pathlib import Path

from fetchers import (
    fetch_weibo_hot,
    fetch_baidu_hot,
    fetch_tencent_news,
    fetch_bili_hot,
    fetch_steam_specials,
    fetch_bangumi,
    fetch_rss,
)
from utils import render_image

# 与 main.py _SOURCES 一致：kind -> (标题, 是否榜型)
KINDS = [
    ("weibo", "微博热搜", True),
    ("baidu", "百度热搜", True),
    ("tencent", "腾讯新闻热榜", True),
    ("bili", "B站热门", True),
    ("steam", "Steam 特惠", False),
    ("bangumi", "今日更新番剧", False),
]
DEMO_RSS = "https://www.ruanyifeng.com/blog/atom.xml"

OUT = Path(__file__).resolve().parent / "out"
OUT.mkdir(exist_ok=True)


def _board_card(kind, title, items):
    return {
        "kind": kind,
        "title": f"🔖 {title}",
        "subtitle": f"实时 · 共 {len(items)} 条",
        "items": items,
    }


def _list_card(kind, title, items, subtitle):
    return {
        "kind": kind,
        "title": f"🔖 {title}",
        "subtitle": subtitle,
        "items": items,
    }


async def fetch_for(kind):
    if kind == "weibo":
        return (await fetch_weibo_hot(12)).get("items", [])
    if kind == "baidu":
        return await fetch_baidu_hot(12)
    if kind == "tencent":
        return await fetch_tencent_news(12)
    if kind == "bili":
        return await fetch_bili_hot(12)
    if kind == "steam":
        return await fetch_steam_specials(12)
    if kind == "bangumi":
        return (await fetch_bangumi(12)).get("items", [])
    return []


async def main():
    results = []
    # 1) 内置榜型/列表源
    for kind, title, is_board in KINDS:
        try:
            items = await fetch_for(kind)
            if is_board:
                card = _board_card(kind, title, items)
            elif kind == "steam":
                card = _list_card(kind, title, items, f"限时特惠 · 共 {len(items)} 款")
            else:
                card = _list_card(kind, title, items, f"今日更新 · 共 {len(items)} 部")
            img = render_image(card)
            path = OUT / f"{kind}.png"
            if img:
                path.write_bytes(img)
            ok = bool(img) and len(items) > 0
            results.append((title, ok, len(items), path.name if img else "镜像失败"))
        except Exception as e:
            results.append((title, False, 0, f"{type(e).__name__}: {e}"))

    # 2) RSS 抓取 + 渲染
    try:
        rss_items = await fetch_rss(DEMO_RSS, max_items=8)
        card = _list_card("rss", "RSS（演示源）", rss_items, f"新更新 {len(rss_items)} 条")
        img = render_image(card)
        path = OUT / "rss.png"
        if img:
            path.write_bytes(img)
        results.append(("RSS订阅", bool(img) and len(rss_items) > 0, len(rss_items), path.name if img else "镜像失败"))
    except Exception as e:
        results.append(("RSS订阅", False, 0, f"{type(e).__name__}: {e}"))

    # 汇总
    print("\n" + "=" * 56)
    print(f"{'来源':<14}{'结果':<5}{'条数':<6}产物")
    print("-" * 56)
    ok_cnt = 0
    for title, ok, n, prod in results:
        mark = "✅" if ok else "❌"
        if ok:
            ok_cnt += 1
        print(f"{title:<14}{mark:<6}{n:<6}{prod}")
    print("-" * 56)
    print(f"共 {len(results)} 项，成功 {ok_cnt} 项，失败 {len(results) - ok_cnt} 项。")
    print(f"卡片产物已保存到：{OUT}")


asyncio.run(main())
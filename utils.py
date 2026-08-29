"""
统一订阅推送中心 - 卡片渲染工具

用 Pillow 将订阅内容渲染成美观的图片卡片：
- 热搜榜单卡片（微博/百度/知乎/抖音/腾讯/B站/36氪，榜样式带热度）
- 信息列表卡片（RSS 更新 / Steam 折扣 / 番剧更新 / GitHub / HN / V2EX，可带摘要行）
- 订阅汇总卡片（digest：多来源合并成一张图）

支持明暗两套主题（set_theme 切换）。
纯标准库 + Pillow，中文字体自动探测，找不到字体则回退，保证不崩溃。
"""

from __future__ import annotations

import io
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_OK = True
except Exception:  # pragma: no cover
    _PIL_OK = False


# ---------------------------------------------------------------------------
# 字体
# ---------------------------------------------------------------------------
# 候选中文字体路径（按优先级）
_FONT_CANDIDATES = [
    "C:/Windows/Fonts/msyh.ttc",  # 微软雅黑
    "C:/Windows/Fonts/msyhbd.ttc",
    "C:/Windows/Fonts/simhei.ttf",  # 黑体
    "C:/Windows/Fonts/simsun.ttc",  # 宋体
    "/System/Library/Fonts/PingFang.ttc",  # macOS 苹方
    "/System/Library/Fonts/STHeiti Light.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",  # Linux 文泉驿
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
]
_FONT_CACHE: Dict[int, "ImageFont.FreeTypeFont"] = {}


def _find_font_path() -> Optional[str]:
    for p in _FONT_CANDIDATES:
        if os.path.exists(p):
            return p
    return None


def _get_font(size: int):
    """获取指定字号的中文字体，带缓存。找不到中文字体时回退默认字体。"""
    key = size
    if key in _FONT_CACHE:
        return _FONT_CACHE[key]
    font = None
    path = _find_font_path()
    if path:
        try:
            font = ImageFont.truetype(path, size)
        except Exception:
            font = None
    if font is None:
        try:
            font = ImageFont.load_default()
        except Exception:
            font = None
    _FONT_CACHE[key] = font
    return font


def _truncate(text: str, max_len: int) -> str:
    """按字符数截断，超长加省略号。"""
    text = text.strip() or "-"
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _fit_text_width(font, text: str, max_width) -> str:
    """把文本截断到不超过最大像素宽度。"""
    if font is None or max_width <= 0:
        return text
    if font.getlength(text) <= max_width:
        return text
    if font.getlength("…") > max_width:
        return ""
    while text and font.getlength(text + "…") > max_width:
        text = text[:-1]
    return text + "…"


# ---------------------------------------------------------------------------
# 主题
# ---------------------------------------------------------------------------
_PALETTES = {
    "dark": {
        "bg": (18, 22, 34),
        "panel": (28, 34, 52),
        "title": (235, 240, 248),
        "text": (214, 220, 232),
        "muted": (140, 150, 170),
        "top3": (255, 94, 94),
        "index": (120, 130, 150),
        "accent": (255, 168, 90),
        "accent2": (90, 168, 255),
        "divider": (44, 52, 74),
    },
    "light": {
        "bg": (244, 246, 250),
        "panel": (255, 255, 255),
        "title": (28, 32, 46),
        "text": (52, 58, 74),
        "muted": (128, 136, 155),
        "top3": (225, 55, 55),
        "index": (150, 158, 175),
        "accent": (240, 140, 50),
        "accent2": (60, 130, 230),
        "divider": (225, 229, 238),
    },
}
_THEME = "dark"


def set_theme(name: str) -> None:
    """切换明暗主题：'dark'（默认）或 'light'。非法值回退 dark。"""
    global _THEME
    _THEME = name if name in _PALETTES else "dark"


def _pal() -> Dict:
    return _PALETTES[_THEME]


# 渲染类型分派
_BOARD_KINDS = {"weibo", "baidu", "zhihu", "douyin", "tencent", "bili", "kr36"}
_LIST_KINDS = {"rss", "steam", "bangumi", "github", "hn", "v2ex"}

# 中文字体普遍不含彩色 emoji 字形，渲染会变成"豆腐块"，统一剥掉
# （范围 U+1F000-U+1FAFF 及变体选择符/零宽连接符；保留 ★ 等常用符号）
_EMOJI_RE = re.compile("[\U0001F000-\U0001FAFF\uFE0F\u200D]")


def _strip_emoji(s) -> str:
    return _EMOJI_RE.sub("", s or "").strip()


def _clean_card(card: Dict) -> Dict:
    """渲染前清洗卡片：剥掉文本字段里的 emoji，避免豆腐块。"""
    def clean_item(it):
        if not isinstance(it, dict):
            return it
        out = dict(it)
        for k in ("title", "desc", "hot", "hot_label", "meta", "time"):
            if isinstance(out.get(k), str):
                out[k] = _strip_emoji(out[k])
        return out

    c = dict(card)
    for k in ("title", "subtitle"):
        if isinstance(c.get(k), str):
            c[k] = _strip_emoji(c[k])
    if isinstance(c.get("items"), list):
        c["items"] = [clean_item(i) for i in c["items"]]
    if isinstance(c.get("sections"), list):
        secs = []
        for s in c["sections"]:
            if isinstance(s, dict):
                secs.append(
                    {
                        **s,
                        "title": _strip_emoji(s.get("title", "")),
                        "items": [clean_item(i) for i in s.get("items", [])],
                    }
                )
        c["sections"] = secs
    return c


def render_image(card: Dict) -> Optional[bytes]:
    """给定卡片数据，渲染成 PNG 字节流。card 结构见各 render_* 函数。"""
    if not _PIL_OK:
        return None
    card = _clean_card(card)
    kind = card.get("kind")
    if kind in _BOARD_KINDS:
        return _render_board(card)
    if kind in _LIST_KINDS:
        return _render_list(card)
    if kind == "digest":
        return _render_digest(card)
    return None


# ---------------------------------------------------------------------------
# 基础块
# ---------------------------------------------------------------------------
_CANVAS_W = 680
_PAD = 30
_HEAD_H = 96
_FOOTER_H = 40


def _begin_canvas(content_h: int = 0):
    """创建画布并返回 (img, draw, ctx)。高度 = 头部 + 内容 + 底部，按内容动态计算。"""
    height = _HEAD_H + max(0, content_h) + _FOOTER_H
    img = Image.new("RGBA", (_CANVAS_W, height), _pal()["bg"])
    draw = ImageDraw.Draw(img)
    ctx = {
        "w": _CANVAS_W,
        "pad": _PAD,
        "head_h": _HEAD_H,
        "footer_h": _FOOTER_H,
        "max_w": _CANVAS_W - 2 * _PAD,
    }
    return img, draw, ctx


def _draw_footer(img, draw, ctx):
    """在画布最底部绘制时间戳，绝不叠在内容上。"""
    sbf = _get_font(13)
    bottom_y = img.height - (sbf.size if sbf else 13) - 8
    draw.text((ctx["pad"], bottom_y), _now_str(), font=sbf, fill=_pal()["muted"])


def _draw_head(img, draw, ctx, title: str, subtitle: str, accent):
    """绘制顶部色条 + 标题 + 副标题。"""
    draw.rectangle([0, 0, ctx["w"], 6], fill=accent)
    tf = _get_font(26)
    draw.text((ctx["pad"], 20), _truncate(title, 24), font=tf, fill=_pal()["title"])
    sub = subtitle or _now_str()
    sbf = _get_font(14)
    if sbf and sbf.getlength(sub) > ctx["max_w"]:
        sub = _truncate(sub, 40)
    draw.text((ctx["pad"], ctx["head_h"] - 26), sub, font=sbf, fill=_pal()["muted"])


def _finish_canvas(img) -> bytes:
    _byte = io.BytesIO()
    img.save(_byte, format="PNG")
    return _byte.getvalue()


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _format_hot(num) -> str:
    """热度格式化：纯数字转 '万' 单位；已带 '万' 的文本原样返回（避免二次破坏）。"""
    if not num:
        return ""
    if isinstance(num, str):
        s = num.strip()
        if "万" in s:
            return s
        try:
            n = float(s.replace(",", ""))
        except ValueError:
            return s
    else:
        try:
            n = float(num)
        except (TypeError, ValueError):
            return str(num)
    if n >= 10000:
        t = f"{n / 10000:.1f}".rstrip("0").rstrip(".")
        return f"{t}万"
    return str(int(n))


def _label_color(label: str) -> tuple:
    """热度标签对应颜色：爆=红，热=橙，新=蓝，荐=绿。"""
    return {"爆": (255, 82, 82), "热": (255, 168, 90), "新": (90, 168, 255), "荐": (96, 188, 120)}.get(label, _pal()["text"])


# ---------------------------------------------------------------------------
# 热搜榜单卡片
# ---------------------------------------------------------------------------
def _render_board(card: Dict) -> Optional[bytes]:
    """渲染热搜榜类卡片。items: [{rank,title,hot,hot_label}]"""
    pal = _pal()
    list_items = card.get("items", [])
    title = card.get("title", "热搜榜")
    subtitle = card.get("subtitle", "")

    row_font = _get_font(16)
    line_h = row_font.size + 26
    content_h = line_h * len(list_items) + 18

    img, draw, ctx = _begin_canvas(content_h)
    _draw_head(img, draw, ctx, title, subtitle, pal["accent"])

    # 内容面板
    top = ctx["head_h"]
    panel_h = line_h * len(list_items) + 18
    draw.rounded_rectangle([ctx["pad"], top, ctx["w"] - ctx["pad"], top + panel_h], radius=12, fill=pal["panel"])

    hf = _get_font(14)  # 热度
    lf = _get_font(12)  # 标签
    tf16 = _get_font(16)  # 标题
    right_anchor = ctx["w"] - ctx["pad"] - 14

    y = top + 12
    for idx, it in enumerate(list_items, 1):
        rank = it.get("rank", idx)
        hot_text = _format_hot(it.get("hot"))
        label = it.get("hot_label", "") if isinstance(it.get("hot_label"), str) else ""
        is_top3 = rank <= 3
        rank_x = ctx["pad"] + 14
        rank_font = _get_font(18)
        rcol = pal["top3"] if is_top3 else pal["index"]
        rank_col_w = 30
        draw.text((rank_x, y), f"{rank}", font=rank_font, fill=rcol)
        tx = rank_x + rank_col_w
        right_w = hf.getlength(hot_text) if (hot_text and hf) else 0
        label_w = lf.getlength(f"[{label}]") if (label and lf) else 0
        avail_w = ctx["w"] - ctx["pad"] - 14 - right_w - label_w - tx - 24
        title_ = _truncate(it.get("title", "-"), 24)
        if avail_w > 60 and tf16 and tf16.getlength(title_) > avail_w:
            title_ = _fit_text_width(tf16, title_, avail_w)
        draw.text((tx, y + 2), title_, font=tf16, fill=pal["title"] if is_top3 else pal["text"])
        if hot_text and hf:
            draw.text((right_anchor - hf.getlength(hot_text), y + 4), hot_text, font=hf, fill=pal["muted"])
        if label and lf:
            lcol = _label_color(label)
            lx = right_anchor - label_w
            draw.text((lx, y + 5), f"[{label}]", font=lf, fill=lcol)
        y += line_h

    _draw_footer(img, draw, ctx)
    return _finish_canvas(img)


# ---------------------------------------------------------------------------
# 信息列表卡片（可带摘要行）
# ---------------------------------------------------------------------------
def _render_list(card: Dict) -> Optional[bytes]:
    """渲染「信息列表」类卡片。items: [{title, time?, meta?, desc?}]"""
    pal = _pal()
    items = card.get("items", [])
    title = card.get("title", "订阅更新")
    subtitle = card.get("subtitle", "")

    row_font = _get_font(16)
    base_line_h = row_font.size + 26
    desc_extra = 20
    descs = [((it.get("desc") or "").strip()) for it in items]
    heights = [base_line_h + (desc_extra if d else 0) for d in descs]
    content_h = sum(heights) + 18

    img, draw, ctx = _begin_canvas(content_h)
    _draw_head(img, draw, ctx, title, subtitle, pal["accent2"])

    top = ctx["head_h"]
    panel_h = content_h
    draw.rounded_rectangle([ctx["pad"], top, ctx["w"] - ctx["pad"], top + panel_h], radius=12, fill=pal["panel"])

    hf = _get_font(13)
    df = _get_font(12)
    tk = _get_font(16)
    y = top + 12
    for it, desc, h in zip(items, descs, heights):
        tinfo = it.get("meta", "") or it.get("time", "") or ""
        time_w = hf.getlength(tinfo) if (tinfo and hf) else 0
        tx = ctx["pad"] + 14
        avail_w = ctx["max_w"] - 28 - time_w - 20
        t = _truncate(it.get("title", "-"), 26)
        if avail_w > 60 and tk and tk.getlength(t) > avail_w:
            t = _fit_text_width(tk, t, avail_w)
        draw.text((tx, y + 2), t, font=tk, fill=pal["text"])
        if tinfo and hf:
            draw.text((ctx["w"] - ctx["pad"] - 14 - hf.getlength(tinfo), y + 3), tinfo, font=hf, fill=pal["muted"])
        if desc and df:
            dtxt = _fit_text_width(df, desc, ctx["max_w"] - 28)
            draw.text((tx, y + base_line_h - 8), dtxt, font=df, fill=pal["muted"])
        y += h
    _draw_footer(img, draw, ctx)
    return _finish_canvas(img)


# ---------------------------------------------------------------------------
# 订阅汇总卡片（digest）
# ---------------------------------------------------------------------------
def _render_digest(card: Dict) -> Optional[bytes]:
    """渲染订阅汇总卡片。sections: [{title, items:[{title, hot?, meta?, time?}]}]"""
    pal = _pal()
    sections = card.get("sections", [])
    title = card.get("title", "订阅汇总")
    subtitle = card.get("subtitle", "")

    row_font = _get_font(15)
    line_h = row_font.size + 22
    sec_head_h = 38
    gap = 10

    total = 0
    for s in sections:
        total += sec_head_h + line_h * len(s.get("items", [])) + gap
    content_h = total + 14

    img, draw, ctx = _begin_canvas(content_h)
    _draw_head(img, draw, ctx, title, subtitle, pal["accent2"])

    top = ctx["head_h"]
    panel_h = content_h
    draw.rounded_rectangle([ctx["pad"], top, ctx["w"] - ctx["pad"], top + panel_h], radius=12, fill=pal["panel"])

    hf = _get_font(12)
    sf = _get_font(17)
    tf15 = _get_font(15)
    y = top + 14
    for s in sections:
        # 分区标题
        draw.text((ctx["pad"] + 14, y), _truncate(s.get("title", ""), 20), font=sf, fill=pal["accent2"])
        y += sec_head_h
        for it in s.get("items", []):
            tinfo = _format_hot(it.get("hot")) or it.get("meta", "") or it.get("time", "") or ""
            time_w = hf.getlength(tinfo) if (tinfo and hf) else 0
            tx = ctx["pad"] + 26
            avail_w = ctx["max_w"] - 40 - time_w - 16
            t = _truncate(it.get("title", "-"), 26)
            if avail_w > 60 and tf15 and tf15.getlength(t) > avail_w:
                t = _fit_text_width(tf15, t, avail_w)
            draw.text((tx, y + 2), t, font=tf15, fill=pal["text"])
            if tinfo and hf:
                draw.text((ctx["w"] - ctx["pad"] - 14 - hf.getlength(tinfo), y + 4), tinfo, font=hf, fill=pal["muted"])
            y += line_h
        # 分区分隔线
        if s is not sections[-1]:
            draw.line(
                [(ctx["pad"] + 14, y + gap // 2), (ctx["w"] - ctx["pad"] - 14, y + gap // 2)],
                fill=pal["divider"], width=1,
            )
        y += gap
    _draw_footer(img, draw, ctx)
    return _finish_canvas(img)

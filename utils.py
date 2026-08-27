"""
统一订阅推送中心 - 卡片渲染工具

用 Pillow 将订阅内容渲染成美观的图片卡片：
- 热搜榜单卡片（微博/百度/腾讯/B站 等，榜样式带热度）
- 信息列表卡片（RSS 更新 / Steam 折扣 / 番剧更新 等）

纯标准库 + Pillow，中文字体自动探测，找不到字体则回退，保证不崩溃。
"""

from __future__ import annotations

import io
import os
from datetime import datetime
from typing import Dict, List, Optional

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_OK = True
except Exception:  # pragma: no cover
    _PIL_OK = False


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


def render_image(card: Dict) -> Optional[bytes]:
    """给定卡片数据，渲染成 PNG 字节流。card 结构见各 render_* 函数。"""
    if not _PIL_OK:
        return None
    kind = card.get("kind")
    if kind in ("weibo", "baidu", "tencent", "bili"):
        return _render_board(card)
    if kind in ("rss", "steam", "bangumi"):
        return _render_list(card)
    return None


# ---------------------------------------------------------------------------
# 颜色与基础块
# ---------------------------------------------------------------------------
_BG = (18, 22, 34)
_PANEL = (28, 34, 52)
_TITLE = (235, 240, 248)
_TEXT = (214, 220, 232)
_MUTED = (140, 150, 170)
_TOP3 = (255, 94, 94)  # 前三名红
_NORMAL_INDEX = (120, 130, 150)

_CANVAS_W = 680
_PAD = 30
_HEAD_H = 96
_FOOTER_H = 40


def _begin_canvas(content_h: int = 0):
    """创建画布并返回 (img, draw, ctx)。高度 = 头部 + 内容 + 底部，按内容动态计算。"""
    height = _HEAD_H + max(0, content_h) + _FOOTER_H
    img = Image.new("RGBA", (_CANVAS_W, height), _BG)
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
    bottom_y = img.height - sbf.size - 8
    draw.text((ctx["pad"], bottom_y), _now_str(), font=sbf, fill=_MUTED)


def _finish_canvas(img) -> bytes:
    _byte = io.BytesIO()
    img.save(_byte, format="PNG")
    return _byte.getvalue()


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _format_hot(num) -> str:
    """把热度数字格式化为 '万' 单位可读文本。"""
    if not num:
        return ""
    try:
        n = float(str(num).replace(",", ""))
    except ValueError:
        return str(num)
    if n >= 10000:
        return f"{n / 10000:.1f}万".rstrip("0").rstrip(".")
    return str(int(n))


def _label_color(label: str) -> tuple:
    """热度标签对应颜色：爆=红，热=橙，新=蓝，荐=绿。"""
    return {"爆": (255, 82, 82), "热": (255, 168, 90), "新": (90, 168, 255), "荐": (96, 188, 120)}.get(label, _TEXT)


def _render_board(card: Dict) -> Optional[bytes]:
    """渲染热搜榜类卡片（微博/百度/腾讯/B站）。
    items: [{rank,title,hot,hot_label}]"""
    list_items = card.get("items", [])
    title = card.get("title", "热搜榜")
    subtitle = card.get("subtitle", "")

    row_font = _get_font(16)
    line_h = row_font.size + 26
    content_h = line_h * len(list_items) + 18

    img, draw, ctx = _begin_canvas(content_h)
    draw.rectangle([0, 0, ctx["w"], 6], fill=(255, 168, 90))

    # 标题
    tf = _get_font(26)
    draw.text((ctx["pad"], 20), title, font=tf, fill=_TITLE)
    sub = subtitle or _now_str()
    sbf = _get_font(14)
    if sbf.getlength(sub) > ctx["max_w"]:
        sub = _truncate(sub, 40)
    draw.text((ctx["pad"], ctx["head_h"] - 26), sub, font=sbf, fill=_MUTED)

    # 内容面板
    row_font = _get_font(16)
    line_h = row_font.size + 26
    top = ctx["head_h"]
    panel_h = line_h * len(list_items) + 18
    draw.rounded_rectangle([ctx["pad"], top, ctx["w"] - ctx["pad"], top + panel_h], radius=12, fill=_PANEL)

    # 表单行复用的字体
    hf = _get_font(14)  # 热度
    lf = _get_font(12)  # 标签
    tf16 = _get_font(16)  # 标题
    right_anchor = ctx["w"] - ctx["pad"] - 14

    y = top + 12
    for idx, it in enumerate(list_items, 1):
        rank = it.get("rank", idx)
        hot_text = _format_hot(it.get("hot"))
        # 标签（爆/热/新/荐）
        label = it.get("hot_label", "") if isinstance(it.get("hot_label"), str) else ""
        # 排名 + 标题
        is_top3 = rank <= 3
        rank_x = ctx["pad"] + 14
        rank_font = _get_font(18)
        rcol = _TOP3 if is_top3 else _NORMAL_INDEX
        # 预定排名右侧对齐的固定栏宽，保证标题起点稳定
        rank_col_w = 30
        rank_str = f"{rank}"
        draw.text((rank_x, y), rank_str, font=rank_font, fill=rcol)
        tx = rank_x + rank_col_w
        # 可用标题宽度：右侧预留热度+标签区
        right_w = hf.getlength(hot_text) if (hot_text and hf) else 0
        label_w = lf.getlength(f"[{label}]") if (label and lf) else 0
        avail_w = ctx["w"] - ctx["pad"] - 14 - right_w - label_w - tx - 24
        title_ = _truncate(it.get("title", "-"), 24)
        # 若标题过长，且文字超宽，则截断到可用宽度
        if avail_w > 60 and tf16 and tf16.getlength(title_) > avail_w:
            title_ = _fit_text_width(tf16, title_, avail_w)
        draw.text((tx, y + 2), title_, font=tf16, fill=_TITLE if is_top3 else _TEXT)
        # 热度（靠右）
        if hot_text and hf:
            draw.text((right_anchor - hf.getlength(hot_text), y + 4), hot_text, font=hf, fill=_MUTED)
        # 标签（紧跟热度）
        if label and lf:
            lcol = _label_color(label)
            lx = right_anchor - label_w
            draw.text((lx, y + 5), f"[{label}]", font=lf, fill=lcol)
        y += line_h

    _draw_footer(img, draw, ctx)
    img = _finish_canvas(img)
    return img


def _render_list(card: Dict) -> Optional[bytes]:
    """渲染「信息列表」类卡片（RSS 更新 / Steam 折扣 / 番剧更新）。
    items: [{title, time?, meta?}]，右侧显示 time 或 meta。"""
    items = card.get("items", [])
    title = card.get("title", "订阅更新")
    subtitle = card.get("subtitle", "")

    row_font = _get_font(16)
    line_h = row_font.size + 26
    content_h = line_h * len(items) + 18

    img, draw, ctx = _begin_canvas(content_h)
    draw.rectangle([0, 0, ctx["w"], 6], fill=(90, 168, 255))
    tf = _get_font(26)
    draw.text((ctx["pad"], 20), title, font=tf, fill=_TITLE)
    sbf = _get_font(14)
    draw.text((ctx["pad"], ctx["head_h"] - 26), subtitle or _now_str(), font=sbf, fill=_MUTED)

    top = ctx["head_h"]
    panel_h = content_h
    draw.rounded_rectangle([ctx["pad"], top, ctx["w"] - ctx["pad"], top + panel_h], radius=12, fill=_PANEL)
    y = top + 12
    for it in items:
        # 右侧信息：优先 meta（Steam 折扣/番剧），其次 time（RSS）
        tinfo = it.get("meta", "") or it.get("time", "") or ""
        hf = _get_font(13)
        time_w = hf.getlength(tinfo) if (tinfo and hf) else 0
        tk = _get_font(16)
        tx = ctx["pad"] + 14
        avail_w = ctx["max_w"] - 28 - time_w - 20
        t = _truncate(it.get("title", "-"), 26)
        if avail_w > 60 and tk and tk.getlength(t) > avail_w:
            t = _fit_text_width(tk, t, avail_w)
        draw.text((tx, y + 2), t, font=tk, fill=_TEXT)
        if tinfo and hf:
            draw.text((ctx["w"] - ctx["pad"] - 14 - hf.getlength(tinfo), y + 3), tinfo, font=hf, fill=_MUTED)
        y += line_h
    _draw_footer(img, draw, ctx)
    return _finish_canvas(img)
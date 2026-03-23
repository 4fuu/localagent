#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "playwright==1.52.0",
#   "jinja2",
# ]
# ///

import argparse
import atexit
import json
import os
from datetime import date, datetime
from html import escape
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_SKILL_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _SKILL_DIR / "templates"
_PROJECT_ROOT = _SKILL_DIR.parent.parent

# Prefer project-local browsers when available; otherwise fall back to the
# container/system Playwright installation (for sandbox images).
_project_playwright = _PROJECT_ROOT / ".playwright"
if _project_playwright.is_dir():
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(_project_playwright))

# Chromium shared libs — pixi environment's lib/
_PIXI_ENV = _PROJECT_ROOT / ".pixi" / "envs" / "localagent"
_pixi_lib = _PIXI_ENV / "lib"
if _pixi_lib.is_dir():
    _ld = os.environ.get("LD_LIBRARY_PATH", "")
    _pixi_lib_str = str(_pixi_lib)
    if _pixi_lib_str not in _ld:
        os.environ["LD_LIBRARY_PATH"] = f"{_pixi_lib_str}:{_ld}" if _ld else _pixi_lib_str

# Fontconfig — let Chromium discover pixi-managed fonts
_fontconfig_conf = _PIXI_ENV / "etc" / "fonts" / "fonts.conf"
if _fontconfig_conf.is_file():
    os.environ.setdefault("FONTCONFIG_FILE", str(_fontconfig_conf))

from playwright.sync_api import Playwright, sync_playwright  # noqa: E402

# ---------------------------------------------------------------------------
# Lazy browser singleton
# ---------------------------------------------------------------------------
_pw: Playwright | None = None
_browser: Any = None


def _get_browser() -> Any:
    global _pw, _browser
    if _browser is None:
        _pw = sync_playwright().start()
        _browser = _pw.chromium.launch()
    return _browser


@atexit.register
def _cleanup() -> None:
    global _pw, _browser
    if _browser:
        _browser.close()
        _browser = None
    if _pw:
        _pw.stop()
        _pw = None


# ---------------------------------------------------------------------------
# Theme per content_type
# ---------------------------------------------------------------------------
_THEMES: dict[str, dict[str, str]] = {
    "weather_china": {"icon": "\U0001f324\ufe0f", "primary": "#3b82f6", "secondary": "#1d4ed8", "name": "天气"},
    "weather_international": {"icon": "\U0001f30d", "primary": "#3b82f6", "secondary": "#1d4ed8", "name": "国际天气"},
    "exchangerate": {"icon": "\U0001f4b1", "primary": "#f59e0b", "secondary": "#d97706", "name": "汇率"},
    "stock": {"icon": "\U0001f4c8", "primary": "#10b981", "secondary": "#059669", "name": "股票"},
    "baike_pro": {"icon": "\U0001f4da", "primary": "#14b8a6", "secondary": "#0d9488", "name": "百科"},
    "medical_common": {"icon": "\U0001f3e5", "primary": "#ef4444", "secondary": "#b91c1c", "name": "医疗"},
    "medical_pro": {"icon": "\U0001f3e5", "primary": "#ef4444", "secondary": "#b91c1c", "name": "医疗"},
    "train_line": {"icon": "\U0001f684", "primary": "#3b82f6", "secondary": "#1e40af", "name": "火车"},
    "train_station_common": {"icon": "\U0001f684", "primary": "#3b82f6", "secondary": "#1e40af", "name": "火车"},
    "train_station_pro": {"icon": "\U0001f684", "primary": "#3b82f6", "secondary": "#1e40af", "name": "火车"},
    "star_chinese_zodiac_animal": {"icon": "\u2b50", "primary": "#6366f1", "secondary": "#4338ca", "name": "属相"},
    "star_chinese_zodiac": {"icon": "\u2b50", "primary": "#6366f1", "secondary": "#4338ca", "name": "属相年份"},
    "star_western_zodiac_sign": {"icon": "\u2b50", "primary": "#6366f1", "secondary": "#4338ca", "name": "星座"},
    "star_western_zodiac": {"icon": "\u2b50", "primary": "#6366f1", "secondary": "#4338ca", "name": "星座日期"},
    "gold_price": {"icon": "\U0001f4b0", "primary": "#f59e0b", "secondary": "#b45309", "name": "金价"},
    "precious_metal": {"icon": "\U0001f947", "primary": "#f59e0b", "secondary": "#b45309", "name": "贵金属"},
    "gold_price_trend": {"icon": "\U0001f4b0", "primary": "#f59e0b", "secondary": "#b45309", "name": "金价"},
    "gold_price_futures_trend": {"icon": "\U0001f4b0", "primary": "#f59e0b", "secondary": "#b45309", "name": "金价期货"},
    "oil_price": {"icon": "\u26fd", "primary": "#f97316", "secondary": "#c2410c", "name": "油价"},
    "calendar": {"icon": "\U0001f4c5", "primary": "#8b5cf6", "secondary": "#6d28d9", "name": "万年历"},
    "constellation": {"icon": "\u2b50", "primary": "#6366f1", "secondary": "#4338ca", "name": "星座"},
    "baike": {"icon": "\U0001f4da", "primary": "#14b8a6", "secondary": "#0d9488", "name": "百科"},
    "medical": {"icon": "\U0001f3e5", "primary": "#ef4444", "secondary": "#b91c1c", "name": "医疗"},
    "phone": {"icon": "\U0001f4f1", "primary": "#6366f1", "secondary": "#4f46e5", "name": "手机"},
    "train": {"icon": "\U0001f684", "primary": "#3b82f6", "secondary": "#1e40af", "name": "火车"},
    "car_common": {"icon": "\U0001f697", "primary": "#64748b", "secondary": "#475569", "name": "汽车"},
    "car_pro": {"icon": "\U0001f697", "primary": "#64748b", "secondary": "#475569", "name": "汽车"},
    "car": {"icon": "\U0001f697", "primary": "#64748b", "secondary": "#475569", "name": "汽车"},
    "mobile": {"icon": "\U0001f4f1", "primary": "#6366f1", "secondary": "#4f46e5", "name": "手机"},
}
_DEFAULT_THEME: dict[str, str] = {"icon": "\U0001f50d", "primary": "#6b7280", "secondary": "#4b5563", "name": "搜索结果"}

# Template name mapping
_TEMPLATE_MAP: dict[str, str] = {
    "weather_china": "weather.html.jinja",
    "weather_international": "weather.html.jinja",
    "stock": "stock.html.jinja",
    "baike_pro": "baike_pro.html.jinja",
    "medical_common": "medical_common.html.jinja",
    "medical_pro": "medical_pro.html.jinja",
    "train_line": "train_line.html.jinja",
    "train_station_common": "train_station_common.html.jinja",
    "train_station_pro": "train_station_pro.html.jinja",
    "star_chinese_zodiac_animal": "star_chinese_zodiac_animal.html.jinja",
    "star_chinese_zodiac": "star_chinese_zodiac.html.jinja",
    "star_western_zodiac_sign": "star_western_zodiac_sign.html.jinja",
    "star_western_zodiac": "star_western_zodiac.html.jinja",
    "gold_price": "gold_price.html.jinja",
    "exchangerate": "exchangerate.html.jinja",
    "gold_price_futures_trend": "gold_price_futures_trend.html.jinja",
    "oil_price": "oil_price.html.jinja",
    "calendar": "calendar.html.jinja",
    "constellation": "constellation.html.jinja",
    "precious_metal": "precious_metal.html.jinja",
    "gold_price_trend": "gold_price_trend.html.jinja",
    "phone": "phone.html.jinja",
    "train": "train.html.jinja",
    "baike": "baike.html.jinja",
    "medical": "medical.html.jinja",
    "car_common": "car_common.html.jinja",
    "car_pro": "car_pro.html.jinja",
    "car": "car.html.jinja",
    "mobile": "mobile.html.jinja",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, list):
        if all(not isinstance(item, (dict, list)) for item in value):
            return "、".join(str(item) for item in value)
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _parse_iso_date(value: Any) -> date | None:
    text = _stringify(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _weather_label_for_day(day_date: date | None, *, base_date: date | None, fallback_index: int) -> str:
    if day_date is not None and base_date is not None:
        delta = (day_date - base_date).days
        if delta == 0:
            return "今天"
        if delta == 1:
            return "明天"
        if delta == 2:
            return "后天"
        if delta >= 0:
            return f"第{delta + 1}天"
    return ["今天", "明天", "后天"][fallback_index] if fallback_index < 3 else f"第{fallback_index + 1}天"


# ---------------------------------------------------------------------------
# Per-type data extraction
# ---------------------------------------------------------------------------


def _extract_weather(content: dict[str, Any]) -> dict[str, Any]:
    city = _stringify(content.get("city", content.get("name", "")))
    raw_days = [day for day in content.get("day", []) if isinstance(day, dict)]
    today = datetime.now().date()
    start_index = 0

    for idx, day in enumerate(raw_days):
        other_week = _stringify(day.get("other_week")).strip()
        day_date = _parse_iso_date(day.get("date"))
        if other_week == "今天" or day_date == today:
            start_index = idx
            break
        if day_date is not None and day_date > today:
            start_index = idx
            break

    selected_days = raw_days[start_index:start_index + 5]
    base_date = _parse_iso_date(selected_days[0].get("date")) if selected_days else today
    days: list[dict[str, str]] = []
    for idx, day in enumerate(selected_days):
        day_date = _parse_iso_date(day.get("date"))
        label = _weather_label_for_day(day_date, base_date=base_date, fallback_index=idx)
        date = _stringify(day.get("date"))
        week = _stringify(day.get("week"))
        summary = _stringify(day.get("summary"))
        high = _stringify(day.get("number_high"))
        low = _stringify(day.get("number_low"))
        wind = _stringify(day.get("day_wind"))
        wind_level = _stringify(day.get("day_windother"))
        humidity = _stringify(day.get("humidity"))

        days.append(
            {
                "label": label,
                "date": f"{week} {date}".strip(),
                "summary": summary,
                "temp": f"{low}℃ ~ {high}℃" if low and high else "",
                "wind": f"{wind} {wind_level}".strip() if wind else "",
                "humidity": f"{humidity}%" if humidity else "",
            }
        )
    return {
        "subtitle": city,
        "meta_location": city,
        "meta_time": _now_str(),
        "weather_days": days,
        "footer_left": f"更新于 {_now_str()}",
    }


def _extract_stock(content: dict[str, Any]) -> dict[str, Any]:
    stock_data = content
    group = content.get("group")
    if isinstance(group, list):
        for item in group:
            if isinstance(item, dict):
                stock_data = item
                break

    name = _stringify(stock_data.get("name", stock_data.get("stock_name", "")))
    code = _stringify(stock_data.get("code_stock", stock_data.get("code", stock_data.get("stock_code", ""))))
    price = _stringify(
        stock_data.get(
            "price",
            stock_data.get("current_price", stock_data.get("number_closed", stock_data.get("close", ""))),
        )
    )
    change = _stringify(stock_data.get("change_percent", stock_data.get("change", stock_data.get("number_per", ""))))

    color = "stock-flat"
    if change:
        if change.startswith("-"):
            color = "stock-down"
        elif change.startswith("+") or (change and change[0].isdigit() and float(change.replace("%", "")) > 0):
            color = "stock-up"

    # Collect remaining fields as rows, skipping already-used ones.
    skip = {
        "alias",
        "change",
        "change_percent",
        "close",
        "code",
        "code_stock",
        "current_price",
        "group",
        "name",
        "name_exchange",
        "number_closed",
        "number_high",
        "number_low",
        "number_open",
        "number_per",
        "price",
        "stock_code",
        "stock_name",
    }
    label_map = {
        "key_status": "状态",
        "number_closed": "昨收",
        "number_high": "最高",
        "number_low": "最低",
        "number_open": "开盘",
        "time": "时间",
        "type": "市场",
    }
    rows = []
    for key in ("type", "key_status", "number_open", "number_high", "number_low", "number_closed", "time", "alias"):
        if key in stock_data and _stringify(stock_data.get(key)):
            rows.append((label_map.get(key, key), _stringify(stock_data.get(key))))

    for k, v in stock_data.items():
        if k.startswith("_") or k in skip:
            continue
        text = _stringify(v)
        if text:
            rows.append((label_map.get(k, k), text))

    subtitle = f"{name} ({code})" if code else name
    return {
        "subtitle": subtitle,
        "meta_time": _now_str(),
        "stock_name": f"{name} {code}".strip(),
        "stock_price": price,
        "stock_change": change,
        "stock_color": color,
        "rows": rows,
        "footer_left": f"更新于 {_now_str()}",
    }


def _extract_exchangerate(content: dict[str, Any]) -> dict[str, Any]:
    rate = _stringify(content.get("rate", content.get("exchange_rate", content.get("result", ""))))
    from_cur = _stringify(content.get("from", content.get("from_currency", content.get("source", ""))))
    to_cur = _stringify(content.get("to", content.get("to_currency", content.get("target", ""))))
    pair = f"{from_cur} → {to_cur}" if from_cur and to_cur else ""
    unit = _stringify(content.get("unit", ""))

    skip = {"rate", "exchange_rate", "result", "from", "to", "from_currency", "to_currency",
            "source", "target", "unit"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": pair,
        "meta_time": _now_str(),
        "rate_pair": escape(pair),
        "rate_value": escape(rate),
        "rate_unit": escape(unit),
        "rows": rows,
        "footer_left": f"更新于 {_now_str()}",
    }


def _extract_oil_price(content: dict[str, Any]) -> dict[str, Any]:
    province = _stringify(content.get("province", content.get("city", content.get("name", ""))))
    items: list[dict[str, str]] = []

    # Try to find oil price fields by common patterns
    oil_types = ["0", "92", "95", "98", "89"]
    type_labels = {"0": "0号柴油", "89": "89号汽油", "92": "92号汽油", "95": "95号汽油", "98": "98号汽油"}
    for t in oil_types:
        for key_pattern in [t, f"h{t}", f"oil_{t}", f"price_{t}", f"gasoline_{t}"]:
            val = content.get(key_pattern)
            if val is not None:
                items.append({"name": type_labels.get(t, f"{t}号"), "price": _stringify(val)})
                break

    # If no structured items found, fall back to rows
    skip = set(oil_types) | {f"h{t}" for t in oil_types} | {"province", "city", "name"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)] if not items else []

    return {
        "subtitle": province,
        "meta_location": escape(province),
        "meta_time": _now_str(),
        "oil_items": items,
        "rows": rows,
        "footer_left": f"更新于 {_now_str()}",
    }


def _extract_calendar(content: dict[str, Any]) -> dict[str, Any]:
    solar = _stringify(content.get("solar", content.get("date", content.get("gregorian", ""))))
    lunar = _stringify(content.get("lunar", content.get("lunar_date", content.get("nongli", ""))))
    week = _stringify(content.get("week", content.get("weekday", "")))

    skip = {"solar", "date", "gregorian", "lunar", "lunar_date", "nongli", "week", "weekday"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": solar or _now_str().split(" ")[0],
        "meta_time": _now_str(),
        "cal_solar": escape(solar),
        "cal_lunar": escape(lunar),
        "cal_week": escape(week),
        "rows": rows,
    }


def _extract_constellation(content: dict[str, Any]) -> dict[str, Any]:
    name = _stringify(content.get("name", content.get("star", content.get("constellation", ""))))
    date_range = _stringify(content.get("date_range", content.get("date", "")))

    skip = {"name", "star", "constellation", "date_range", "date"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": name,
        "meta_time": _now_str(),
        "star_name": escape(name),
        "star_date_range": escape(date_range),
        "rows": rows,
    }


def _extract_precious_metal(content: dict[str, Any]) -> dict[str, Any]:
    name = _stringify(content.get("name", content.get("metal_name", "")))
    price = _stringify(content.get("price", content.get("current_price", "")))
    unit = _stringify(content.get("unit", ""))
    change = _stringify(content.get("change", content.get("change_percent", "")))

    color = "metal-flat"
    if change:
        if change.startswith("-"):
            color = "metal-down"
        elif change.startswith("+") or (change and change[0].isdigit()):
            color = "metal-up"

    skip = {"name", "metal_name", "price", "current_price", "unit", "change", "change_percent"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": name,
        "meta_time": _now_str(),
        "metal_name": escape(name),
        "metal_price": escape(price),
        "metal_unit": escape(unit),
        "metal_change": escape(change),
        "metal_color": color,
        "rows": rows,
        "footer_left": f"更新于 {_now_str()}",
    }


def _metric_color(status: str) -> str:
    if status == "up":
        return "metric-up"
    if status == "down":
        return "metric-down"
    return "metric-flat"


def _extract_gold_price_trend(content: dict[str, Any]) -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    section_titles = ["国内金价", "国际金价"]
    for idx, entry in enumerate(content.get("tab_location", [])):
        if not isinstance(entry, dict):
            continue

        current = next(
            (item for item in entry.get("current", []) if isinstance(item, dict)),
            {},
        )
        metrics = []
        for item in entry.get("list", []):
            if not isinstance(item, dict):
                continue
            name = _stringify(item.get("name"))
            value = _stringify(item.get("value"))
            if not name or not value:
                continue
            metrics.append(
                {
                    "name": name,
                    "value": value,
                    "color": _metric_color(_stringify(item.get("key_status"))),
                }
            )

        price = _stringify(current.get("number_info"))
        unit = _stringify(current.get("unit"))
        change_percent = _stringify(current.get("number_per"))
        change_range = _stringify(current.get("range"))
        change_text = " ".join(part for part in [change_percent, change_range] if part).strip()

        sections.append(
            {
                "title": section_titles[idx] if idx < len(section_titles) else f"分项 {idx + 1}",
                "price": price,
                "unit": unit,
                "change": change_text,
                "change_color": _metric_color(_stringify(current.get("key_status"))),
                "metrics": metrics,
            }
        )

    rows = []
    source = _stringify(content.get("source"))
    if source:
        rows.append(("来源", source))

    return {
        "subtitle": source or "黄金价格趋势",
        "meta_time": _now_str(),
        "gold_sections": sections,
        "rows": rows,
        "footer_left": f"更新于 {_now_str()}",
    }


def _extract_train(content: dict[str, Any]) -> dict[str, Any]:
    items: list[dict[str, str]] = []
    # Try list-style data
    for entry in content.get("list", content.get("trains", content.get("data", []))):
        if not isinstance(entry, dict):
            continue
        items.append({
            "train_no": _stringify(entry.get("train_no", entry.get("trainNo", entry.get("no", "")))),
            "from_station": _stringify(entry.get("from_station", entry.get("fromStation", entry.get("start", "")))),
            "to_station": _stringify(entry.get("to_station", entry.get("toStation", entry.get("end", "")))),
            "depart_time": _stringify(entry.get("depart_time", entry.get("departTime", entry.get("start_time", "")))),
            "arrive_time": _stringify(entry.get("arrive_time", entry.get("arriveTime", entry.get("end_time", "")))),
            "duration": _stringify(entry.get("duration", entry.get("run_time", ""))),
            "price": _stringify(entry.get("price", entry.get("seat_price", ""))),
        })

    from_city = _stringify(content.get("from", content.get("from_city", "")))
    to_city = _stringify(content.get("to", content.get("to_city", "")))
    subtitle = f"{from_city} → {to_city}" if from_city and to_city else ""

    skip = {"list", "trains", "data", "from", "to", "from_city", "to_city"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)] if not items else []

    return {
        "subtitle": subtitle,
        "meta_time": _now_str(),
        "train_items": items[:6],
        "rows": rows,
    }


def _extract_baike(content: dict[str, Any]) -> dict[str, Any]:
    title = _stringify(content.get("title", content.get("name", "")))
    desc = _stringify(content.get("desc", content.get("description", content.get("summary", content.get("abstract", "")))))

    skip = {"title", "name", "desc", "description", "summary", "abstract"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": title,
        "baike_title": escape(title),
        "baike_desc": escape(desc),
        "rows": rows,
    }


def _extract_baike_pro(content: dict[str, Any]) -> dict[str, Any]:
    first_item = {}
    for module in content.get("module_list", []):
        if not isinstance(module, dict):
            continue
        for item in module.get("item_list", []):
            if not isinstance(item, dict):
                continue
            data = item.get("data", {})
            if isinstance(data, dict):
                first_item = data
                break
        if first_item:
            break

    card = first_item.get("card", {}) if isinstance(first_item.get("card"), dict) else {}
    title = _stringify(card.get("title", first_item.get("title", "")))
    desc = _stringify(card.get("dynAbstract", card.get("abstract_info", card.get("subTitle", ""))))
    skip = {"baikeURL", "card"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in first_item.items() if k not in skip and _stringify(v)]

    return {
        "subtitle": title,
        "baike_title": escape(title),
        "baike_desc": escape(desc),
        "rows": rows,
    }


def _extract_medical(content: dict[str, Any]) -> dict[str, Any]:
    title = _stringify(content.get("title", content.get("name", content.get("disease", ""))))
    desc = _stringify(content.get("desc", content.get("description", content.get("summary", ""))))

    skip = {"title", "name", "disease", "desc", "description", "summary"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": title,
        "med_title": escape(title),
        "med_desc": escape(desc),
        "rows": rows,
    }


def _extract_medical_common(content: dict[str, Any]) -> dict[str, Any]:
    item = content.get("subitem", {})
    if not isinstance(item, dict):
        item = {}
    title = _stringify(item.get("title"))
    desc = _stringify(item.get("content"))
    rows = []
    for key in ["doctorName", "doctorLevel", "hospital", "hospitalLv", "department", "wapUrl4Resin"]:
        value = _stringify(item.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": title,
        "med_title": escape(title),
        "med_desc": escape(desc),
        "rows": rows,
    }


def _extract_medical_pro(content: dict[str, Any]) -> dict[str, Any]:
    title = _stringify(content.get("title"))
    details = content.get("contents", {}) if isinstance(content.get("contents"), dict) else {}
    detail = details.get("detail", {}) if isinstance(details.get("detail"), dict) else {}
    desc = _stringify(detail.get("content_detail"))
    rows = []
    for key in ["doctor", "doctor_title", "hospital", "hospital_level", "department"]:
        value = _stringify(details.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": title,
        "med_title": escape(title),
        "med_desc": escape(desc),
        "rows": rows,
    }


def _extract_car(content: dict[str, Any]) -> dict[str, Any]:
    name = _stringify(content.get("name", content.get("car_name", content.get("model", ""))))
    price = _stringify(content.get("price", content.get("guide_price", "")))

    skip = {"name", "car_name", "model", "price", "guide_price"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": name,
        "car_name": escape(name),
        "car_price": escape(price),
        "rows": rows,
    }


def _extract_car_common(content: dict[str, Any]) -> dict[str, Any]:
    item = {}
    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict):
            subitems = first.get("subitem", [])
            if isinstance(subitems, list) and subitems and isinstance(subitems[0], dict):
                item = subitems[0]
    elif isinstance(content, dict):
        subitems = content.get("subitem", [])
        if isinstance(subitems, list) and subitems and isinstance(subitems[0], dict):
            item = subitems[0]

    sub = item.get("subdisplay", {}) if isinstance(item.get("subdisplay"), dict) else {}
    name = _stringify(sub.get("carSeries", sub.get("brand", item.get("key", ""))))
    price = _stringify(sub.get("guidedPrice"))
    rows = []
    for key in ["brand", "carLevel", "nation", "wx_carurl"]:
        value = _stringify(sub.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": name,
        "car_name": escape(name),
        "car_price": escape(price),
        "rows": rows,
    }


def _extract_car_pro(content: dict[str, Any]) -> dict[str, Any]:
    info_sub = content.get("info_sub", {}) if isinstance(content.get("info_sub"), dict) else {}
    display = content.get("info_subdisplay", {}) if isinstance(content.get("info_subdisplay"), dict) else {}
    name = _stringify(display.get("name", info_sub.get("name_car", "")))
    price = _stringify(display.get("price_guide", info_sub.get("price_manual_guide", "")))
    rows = []
    for key in ["key_brand", "sub_brand", "name_series", "displacement", "oilwear", "type_year", "url", "url_conf", "url_min_price"]:
        value = _stringify(display.get(key, info_sub.get(key)))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": name,
        "car_name": escape(name),
        "car_price": escape(price),
        "rows": rows,
    }


def _extract_phone(content: dict[str, Any]) -> dict[str, Any]:
    subdisplay = content.get("subdisplay", {}) if isinstance(content.get("subdisplay"), dict) else {}
    name = " vs ".join(
        part for part in [_stringify(subdisplay.get("modelname1")), _stringify(subdisplay.get("modelname2"))] if part
    )
    price = " / ".join(
        part for part in [_stringify(subdisplay.get("modelprice1")), _stringify(subdisplay.get("modelprice2"))] if part
    )
    rows = []
    for key in [
        "date", "modelbrand1", "modelbrand2", "modelpcurl1", "modelpcurl2",
        "modelwapurl1", "modelwapurl2",
    ]:
        value = _stringify(subdisplay.get(key))
        if value:
            rows.append((escape(key), escape(value)))

    for model_key, prefix in [("model1_parameter", "机型A"), ("model2_parameter", "机型B")]:
        model_params = subdisplay.get(model_key, {})
        if not isinstance(model_params, dict):
            continue
        for group_key in ["show_parameter1", "show_parameter2"]:
            items = model_params.get(group_key, [])
            if not isinstance(items, list):
                continue
            for item in items[:8]:
                if not isinstance(item, dict):
                    continue
                label = _stringify(item.get("par_name1", item.get("par_name")))
                value = _stringify(item.get("val1", item.get("val")))
                if label and value:
                    rows.append((escape(f"{prefix}{label}"), escape(value)))

    return {
        "subtitle": name,
        "phone_name": escape(name),
        "phone_price": escape(price),
        "rows": rows,
    }


def _extract_mobile(content: dict[str, Any]) -> dict[str, Any]:
    name = _stringify(content.get("name", content.get("phone_name", content.get("model", ""))))
    price = _stringify(content.get("price", ""))

    skip = {"name", "phone_name", "model", "price"}
    rows = [(escape(k), escape(_stringify(v))) for k, v in content.items()
            if not k.startswith("_") and k not in skip and _stringify(v)]

    return {
        "subtitle": name,
        "phone_name": escape(name),
        "phone_price": escape(price),
        "rows": rows,
    }


def _extract_train_line(content: dict[str, Any]) -> dict[str, Any]:
    items: list[dict[str, str]] = []
    for entry in content if isinstance(content, list) else []:
        if not isinstance(entry, dict):
            continue
        sub = entry.get("subdisplay", {})
        if not isinstance(sub, dict):
            continue
        ticket = sub.get("ticket", {})
        if isinstance(ticket, dict):
            price = " / ".join(f"{k}:{_stringify(v)}" for k, v in list(ticket.items())[:3] if _stringify(v))
        else:
            price = _stringify(ticket)
        items.append(
            {
                "train_no": _stringify(sub.get("num", entry.get("key"))),
                "from_station": _stringify(sub.get("start_station", sub.get("start_city"))),
                "to_station": _stringify(sub.get("end_station", sub.get("end_city"))),
                "depart_time": _stringify(sub.get("start_time")),
                "arrive_time": _stringify(sub.get("end_time")),
                "duration": _stringify(sub.get("alltime")),
                "price": price,
            }
        )
    return {
        "subtitle": "火车线路",
        "meta_time": _now_str(),
        "train_items": items[:6],
        "rows": [],
    }


def _extract_train_station_common(content: dict[str, Any]) -> dict[str, Any]:
    sub = content.get("subdisplay", {}) if isinstance(content.get("subdisplay"), dict) else {}
    items: list[dict[str, str]] = []
    stations = sub.get("station", [])
    if isinstance(stations, list) and stations:
        for item in stations[:6]:
            if not isinstance(item, dict):
                continue
            items.append(
                {
                    "train_no": _stringify(sub.get("checi_num", content.get("key"))),
                    "from_station": _stringify(item.get("station_name", item.get("name", ""))),
                    "to_station": _stringify(item.get("arrive_station", item.get("next_station", ""))),
                    "depart_time": _stringify(item.get("start_time", item.get("depart_time", ""))),
                    "arrive_time": _stringify(item.get("arrive_time", "")),
                    "duration": _stringify(sub.get("alltime")),
                    "price": "",
                }
            )
    rows = []
    for key in ["date", "type", "url", "H5url", "start_station", "end_station", "start_time", "end_time"]:
        value = _stringify(sub.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": _stringify(sub.get("checi_num", content.get("key"))),
        "meta_time": _now_str(),
        "train_items": items,
        "rows": rows if not items else rows[:4],
    }


def _extract_train_station_pro(content: dict[str, Any]) -> dict[str, Any]:
    return _extract_train_station_common(content)


def _extract_star_chinese_zodiac_animal(content: dict[str, Any]) -> dict[str, Any]:
    sub = content.get("subdisplay", {}) if isinstance(content.get("subdisplay"), dict) else {}
    name = _stringify(sub.get("name", content.get("key", "")))
    rows = []
    for key in ["othername", "year1", "character", "yearfortune", "good", "bad", "fortune", "caiyun", "hunyin", "jiankang", "shiye"]:
        value = _stringify(sub.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": name,
        "meta_time": _now_str(),
        "star_name": escape(name),
        "star_date_range": "",
        "rows": rows,
    }


def _extract_star_chinese_zodiac(content: dict[str, Any]) -> dict[str, Any]:
    sub = content.get("subdisplay", {}) if isinstance(content.get("subdisplay"), dict) else {}
    name = _stringify(sub.get("nianfen", content.get("key", "")))
    date_range = " / ".join(part for part in [_stringify(sub.get("date1")), _stringify(sub.get("date2"))] if part)
    rows = []
    for key in ["nongli", "name1", "name2", "name1link", "name2url"]:
        value = _stringify(sub.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": name,
        "meta_time": _now_str(),
        "star_name": escape(name),
        "star_date_range": escape(date_range),
        "rows": rows,
    }


def _extract_star_western_zodiac_sign(content: dict[str, Any]) -> dict[str, Any]:
    items = content.get("info_subdisplay", [])
    item = items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else {}
    name = _stringify(item.get("name"))
    date_range = _stringify(item.get("date"))
    rows = []
    for key in ["name_english", "attribute", "color", "diamond", "summary", "female", "male", "number_info", "tag_star"]:
        value = _stringify(item.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": name,
        "meta_time": _now_str(),
        "star_name": escape(name),
        "star_date_range": escape(date_range),
        "rows": rows,
    }


def _extract_star_western_zodiac(content: dict[str, Any]) -> dict[str, Any]:
    accur = _stringify(content.get("accurContent"))
    month = accur.split("&", 1)[0] if "&" in accur else accur[:20]
    rows = []
    for key in ["accurContent", "voiceinfo"]:
        value = _stringify(content.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    return {
        "subtitle": month,
        "meta_time": _now_str(),
        "star_name": escape("星座日期"),
        "star_date_range": "",
        "rows": rows,
    }


def _extract_gold_price(content: dict[str, Any]) -> dict[str, Any]:
    sub = content.get("subdisplay", {}) if isinstance(content.get("subdisplay"), dict) else {}
    tabs = sub.get("tab", [])
    item = {}
    tab_type = ""
    if isinstance(tabs, list) and tabs:
        first_tab = tabs[0] if isinstance(tabs[0], dict) else {}
        tab_type = _stringify(first_tab.get("type"))
        symbols = first_tab.get("symbol", [])
        if isinstance(symbols, list) and symbols and isinstance(symbols[0], dict):
            item = symbols[0]
    name = _stringify(item.get("name", item.get("shopname", "黄金价格")))
    price = _stringify(item.get("price"))
    change = _stringify(item.get("updown"))
    rows = []
    for key in ["date"]:
        value = _stringify(sub.get(key))
        if value:
            rows.append((escape(key), escape(value)))
    for key in ["shopname", "pcurl", "mpcurl", "type"]:
        value = _stringify(item.get(key, tab_type if key == "type" else ""))
        if value:
            rows.append((escape(key), escape(value)))
    color = "metal-flat"
    if change.startswith("-"):
        color = "metal-down"
    elif change:
        color = "metal-up"
    return {
        "subtitle": tab_type,
        "meta_time": _now_str(),
        "metal_name": escape(name),
        "metal_price": escape(price),
        "metal_unit": "",
        "metal_change": escape(change),
        "metal_color": color,
        "rows": rows,
        "footer_left": f"更新于 {_now_str()}",
    }


def _extract_generic(content: dict[str, Any]) -> dict[str, Any]:
    subtitle = _stringify(content.get("city", content.get("name", content.get("title", ""))))
    rows: list[tuple[str, str]] = []
    for key, value in content.items():
        if str(key).startswith("_"):
            continue
        text = _stringify(value)
        if not text:
            continue
        rows.append((escape(str(key)), escape(text)))
    if not rows:
        rows = [("content", escape(_stringify(content)))]
    return {
        "subtitle": escape(subtitle),
        "meta_time": _now_str(),
        "rows": rows,
    }


_EXTRACTORS: dict[str, Any] = {
    "weather_china": _extract_weather,
    "weather_international": _extract_weather,
    "stock": _extract_stock,
    "baike_pro": _extract_baike_pro,
    "medical_common": _extract_medical_common,
    "medical_pro": _extract_medical_pro,
    "train_line": _extract_train_line,
    "train_station_common": _extract_train_station_common,
    "train_station_pro": _extract_train_station_pro,
    "star_chinese_zodiac_animal": _extract_star_chinese_zodiac_animal,
    "star_chinese_zodiac": _extract_star_chinese_zodiac,
    "star_western_zodiac_sign": _extract_star_western_zodiac_sign,
    "star_western_zodiac": _extract_star_western_zodiac,
    "gold_price": _extract_gold_price,
    "exchangerate": _extract_exchangerate,
    "gold_price_futures_trend": _extract_gold_price_trend,
    "oil_price": _extract_oil_price,
    "calendar": _extract_calendar,
    "constellation": _extract_constellation,
    "precious_metal": _extract_precious_metal,
    "gold_price_trend": _extract_gold_price_trend,
    "phone": _extract_phone,
    "train": _extract_train,
    "baike": _extract_baike,
    "medical": _extract_medical,
    "car_common": _extract_car_common,
    "car_pro": _extract_car_pro,
    "car": _extract_car,
    "mobile": _extract_mobile,
}

# ---------------------------------------------------------------------------
# Jinja2 environment
# ---------------------------------------------------------------------------
_jinja_env = Environment(loader=FileSystemLoader(str(_TEMPLATES_DIR)), autoescape=True)


# ---------------------------------------------------------------------------
# Core render function
# ---------------------------------------------------------------------------


def render_model_card_to_image(
    content_type: str,
    content: dict[str, Any],
    output_path: str | Path,
    width: int = 480,
) -> Path:
    canvas_padding_left = 20
    canvas_padding_right = 18
    canvas_padding_top = 20
    canvas_padding_bottom = 20

    theme = _THEMES.get(content_type, _DEFAULT_THEME)
    extractor = _EXTRACTORS.get(content_type, _extract_generic)
    template_name = _TEMPLATE_MAP.get(content_type, "base.html.jinja")

    ctx = extractor(content)
    ctx.setdefault("width", width)
    ctx.setdefault("canvas_padding_left", canvas_padding_left)
    ctx.setdefault("canvas_padding_right", canvas_padding_right)
    ctx.setdefault("canvas_padding_top", canvas_padding_top)
    ctx.setdefault("canvas_padding_bottom", canvas_padding_bottom)
    ctx.setdefault("theme", theme)
    ctx.setdefault("rows", [])

    template = _jinja_env.get_template(template_name)
    html = template.render(**ctx)

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    viewport_width = width + canvas_padding_left + canvas_padding_right
    browser = _get_browser()
    page = browser.new_page(
        viewport={"width": viewport_width, "height": 800},
        device_scale_factor=2,
    )
    try:
        page.set_content(html, wait_until="load")
        height = page.evaluate("document.body.scrollHeight")
        page.set_viewport_size({"width": viewport_width, "height": height})
        page.screenshot(path=str(target), full_page=True, scale="device")
    finally:
        page.close()

    return target


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="将博查模态卡 JSON 渲染为 PNG")
    parser.add_argument("--content-type", required=True, help="模态卡 content_type")
    parser.add_argument("--input", required=True, help="modelCard JSON 文件路径")
    parser.add_argument("--output", required=True, help="输出 PNG 路径")
    parser.add_argument("--width", type=int, default=480, help="卡片宽度（默认 480）")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    input_path = Path(args.input)
    content = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(content, dict):
        raise ValueError("输入 JSON 必须是对象（modelCard）")
    out = render_model_card_to_image(args.content_type, content, args.output, width=args.width)
    print(out)


if __name__ == "__main__":
    main()

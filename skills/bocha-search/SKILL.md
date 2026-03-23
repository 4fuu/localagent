---
name: bocha-search
description: 基于博查 AI 搜索 API 的中文综合检索技能，侧重网页/图片结果与结构化模态卡（天气、汇率、股票等）解析展示，并可将 modelCard 渲染为 PNG 卡片；适合国内信息与结构化答案场景。默认搜索，如果有特殊情况请考虑使用别的搜索技能。
---

# 博查 AI 搜索技能

本技能用于网络搜索与结构化信息展示，覆盖网页结果解析、模态卡内容提取和卡片图片渲染。它由 `search.py`（搜索与结果解析）和 `render_card.py`（模态卡渲染）组成，适用于需要同时返回文本结果和可视化卡片的查询场景。`search.py` 默认会渲染模态卡 PNG，并在输出末尾明确说明是哪一段模态卡生成了几张图、图片路径在哪里。

使用博查 AI 搜索 API 进行智能网络搜索。包含两个脚本：
- `search.py`：执行搜索，输出网页结果与模态卡文本；默认渲染模态卡 PNG，并输出图片位置与张数提示
- `render_card.py`：将单个 `modelCard` JSON 渲染为 PNG（可独立调用）

## 前置条件

- 已设置环境变量 `BOCHA_API_KEY`
- 已安装 `uv` 工具（当前环境已经安装）

## 使用方法

常规搜索（网页结果/文本，默认自动渲染模态卡），模态卡请发送消息时携带：

```bash
uv run skills/bocha-search/search.py <搜索关键词> [时间范围] [结果数量] [--cards-output-dir <目录>] [--no-render-cards]
```

## 参数说明

- **搜索关键词**（必需）：要搜索的内容
- **时间范围**（可选）：筛选结果的时间范围
  - `noLimit`：不限时间（默认）
  - `oneDay`：一天内
  - `oneWeek`：一周内
  - `oneMonth`：一月内
  - `oneYear`：一年内
- **结果数量**（可选）：返回结果数量，1-50，默认 10
- 默认会将检测到的模态卡渲染为 PNG
- `--no-render-cards`（可选）：关闭模态卡 PNG 渲染
- `--cards-output-dir`（可选）：图片输出目录；默认在当前会话 workspace 下创建 `bocha-cards/`，普通仓库内手动运行时回退到项目根 `workspace/bocha-cards`

## 示例

```bash
# 搜索天气并渲染模态卡图片（默认开启）
uv run skills/bocha-search/search.py "北京天气"

# 搜索汇率（会触发汇率模态卡，默认开启）
uv run skills/bocha-search/search.py "美元对人民币汇率"

# 限定时间范围和结果数量
uv run skills/bocha-search/search.py "人工智能最新进展" oneWeek 5

# 自定义图片输出目录
uv run skills/bocha-search/search.py "今日油价" --cards-output-dir ./out/cards

# 基本搜索，仅返回文本，不渲染模态卡图片
uv run skills/bocha-search/search.py "天空为什么是蓝色的" --no-render-cards
```

独立渲染单个模态卡 JSON：

```bash
uv run skills/bocha-search/render_card.py \
  --content-type weather_china \
  --input ./modelCard.json \
  --output ./out/weather.png
```

## 输出格式

### 模态卡

如果搜索结果包含模态卡（结构化数据），`search.py` 会输出 `modelCard` 原始 JSON（pretty print）。
支持的 `content_type` 见下方“模态卡类型”表。

默认渲染开启时，会在终端输出每张图片路径，例如：

```text
🖼️ 模态卡图片：bocha-cards/weather_china_a1b2c3d4e5f6.png（类型：weather_china，内容 #1）
```

同一次搜索中如果返回多个 `modelCard`，会全部渲染为独立图片。图片文件名使用随机后缀，避免覆盖旧文件。输出末尾会逐行列出“文件路径 -> 卡片类型（模态卡内容 #N）”，方便模型在发送多张图片时准确对应每个路径的卡片类型。发送完成后，请删除本次生成的临时图片文件。

### 网页搜索结果

每条结果包含：
- 标题（title）
- 链接（url）
- 摘要（summary）

## 环境变量

- `BOCHA_API_KEY`: 博查 AI 搜索 API 密钥，从系统密钥存储中读取

## 依赖管理

脚本使用 PEP 723 内联元数据声明依赖，`uv run` 会自动管理：

- `search.py` 依赖 `httpx`
- `render_card.py` 依赖 `playwright` + `jinja2`

Chromium 浏览器和 CJK 字体等系统级依赖已经由系统前置处理，无需手动安装。

## 模态卡类型

博查 API 支持的模态卡类型包括：

| 类型 | content_type | 示例搜索词 |
|------|-------------|-----------|
| 国内天气 | weather_china | 北京天气、杭州天气 |
| 国际天气 | weather_international | 巴黎天气、纽约天气 |
| 百科专业版 | baike_pro | 西瓜的功效与作用 |
| 医疗普通版 | medical_common | 站着和坐着哪个对腰椎伤害大 |
| 医疗专业版 | medical_pro | 站着和坐着哪个对腰椎伤害大 |
| 万年历 | calendar | 今天是什么日子、农历正月初一 |
| 火车线路 | train_line | 长春到白城火车时刻表 |
| 火车站点信息 | train_station_common | K84次列车途经站点 |
| 火车站点信息专业版 | train_station_pro | G2556高铁时刻表停靠站 |
| 中国属相 | star_chinese_zodiac_animal | 属虎男最佳婚配属相 |
| 中国属相年份 | star_chinese_zodiac | 2024年是什么年 |
| 星座 | star_western_zodiac_sign | 水瓶座、射手座性格 |
| 星座日期 | star_western_zodiac | 12月是什么星座 |
| 金价 | gold_price | 今日金价、现在黄金多少钱一克 |
| 金价趋势 | gold_price_trend | 黄金今日价格 |
| 金价期货趋势 | gold_price_futures_trend | 黄金期货实时行情 |
| 汇率 | exchangerate | 美元对人民币汇率、一美元等于多少人民币 |
| 油价 | oil_price | 今日油价、92号汽油价格 |
| 手机参数对比 | phone | 华为Pura70和Mate60哪个好 |
| 股票 | stock | 腾讯股票、苹果股价 |
| 汽车普通版 | car_common | 长安汽车 |
| 汽车专业版 | car_pro | 宝马X3 |

## 注意事项
1. **模态卡功能**：默认开启模态卡功能
2. **发送图片**：将生成的模态卡图片随消息发送
3. **结果数量**：最多返回 50 条结果，超过会报错
4. **超时设置**：默认超时 60 秒，复杂搜索可能需要更长时间
5. **检查以往图片**：如果有以往生成的图片，需要删除

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 百炼模型API前端 v6.8-LOCKED-CLOUD（2026-07-28）
- 只渲染最近10轮对话（20条消息），大幅减少闪烁
- 文件上传改为系统原生文件选择器（弹窗选择），交互与桌面软件一致
- 导出为公众号发布级格式：微软雅黑、字号层级、首行缩进2字符、自动清理标记
- 所有功能：记忆10轮、摘要复制、时间精确、熔断、WAL
- 底部布局：输入框 + 📎文件按钮同行，紧凑
- 输出直接展开，无折叠
- 【云端版】在调用模型前，自动从 RDS 加载历史对话到 messages
"""

import os
import re
import json
import time
import logging
import functools
import base64
import sqlite3
import io
import shutil
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Optional, Tuple

import streamlit as st
import dashscope
import tushare as ts
import pytz
import pymysql
from PIL import Image
from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from http import HTTPStatus
from dotenv import load_dotenv

load_dotenv()

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
if not DASHSCOPE_API_KEY:
    raise RuntimeError("⛔ 请配置环境变量 DASHSCOPE_API_KEY")

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")
MODEL_NAME = os.getenv("MODEL_NAME", "qwen-plus")

BEIJING_TZ = pytz.timezone('Asia/Shanghai')
ZHI_SUAN_FILE = os.getenv("ZHI_SUAN_FILE", "./智算池完整版_同花顺.txt")
HISTORY_FILE = os.getenv("HISTORY_FILE", "./chat_history.json")
MEMORY_DB_PATH = os.getenv("MEMORY_DB_PATH", "./chat_memory.db")
LOCAL_DB_PATH = os.getenv("LOCAL_DB_PATH", "./stock_data.db")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename='app.log', filemode='a')
logger = logging.getLogger(__name__)

_FAIL_COUNTER = 0
_MODEL_HEALTHY = True
MAX_CONSECUTIVE_FAILURES = 3

def reset_health_status():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    _FAIL_COUNTER = 0
    _MODEL_HEALTHY = True

def mark_failure():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    _FAIL_COUNTER += 1
    if _FAIL_COUNTER >= MAX_CONSECUTIVE_FAILURES:
        _MODEL_HEALTHY = False
        logger.warning("🚨 模型服务熔断已触发，连续失败 %d 次", _FAIL_COUNTER)

def is_model_healthy() -> bool:
    return _MODEL_HEALTHY

def estimate_tokens(text: str) -> int:
    if not text: return 0
    ch = len(re.findall(r'[\u4e00-\u9fff]', text))
    return int(ch / 1.5 + (len(text) - ch) / 4)

def estimate_messages_tokens(messages: List[Dict[str, Any]]) -> int:
    total = 0
    for msg in messages:
        c = msg.get("content", "")
        if isinstance(c, list):
            for p in c: total += estimate_tokens(p.get("text", "") if isinstance(p, dict) else str(p))
        else:
            total += estimate_tokens(str(c))
        total += len(msg.get("images", [])) * 500
        total += sum(estimate_tokens(tf.get("data", "")) for tf in msg.get("txt_files", []))
    return total

CONTEXT_LIMIT = 1_000_000
CONTEXT_WARN_THRESHOLD = 0.80
TOKEN_DISPLAY_THRESHOLD = 5000

# ================= RDS 连接函数 =================
def get_rds_connection():
    """从 st.secrets 读取 RDS 连接信息"""
    return pymysql.connect(
        host=st.secrets["connections"]["rds"]["host"],
        port=st.secrets["connections"]["rds"]["port"],
        user=st.secrets["connections"]["rds"]["username"],
        password=st.secrets["connections"]["rds"]["password"],
        database=st.secrets["connections"]["rds"]["database"],
        charset='utf8mb4'
    )

# ================= 记忆库（本地 SQLite，用于本地运行） =================
def init_memory_db():
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS memories (id INTEGER PRIMARY KEY AUTOINCREMENT, user_msg TEXT, assistant_msg TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_memories_user ON memories(user_msg)")
        conn.commit()
        conn.close()
        logger.info("✅ 记忆库初始化完成 (WAL模式)")
    except Exception as e:
        logger.error(f"记忆库初始化失败: {e}")

def save_memory(u: str, a: str):
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("INSERT INTO memories(user_msg, assistant_msg) VALUES(?, ?)", (u, a))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"记忆保存失败: {e}")

def search_memory(keyword: str = "", limit: int = 2) -> List[Tuple[str, str]]:
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA busy_timeout=5000")
        if keyword:
            rows = conn.execute("SELECT user_msg, assistant_msg FROM memories WHERE user_msg LIKE ? OR assistant_msg LIKE ? ORDER BY ts DESC LIMIT ?", (f"%{keyword}%", f"%{keyword}%", limit)).fetchall()
        else:
            rows = conn.execute("SELECT user_msg, assistant_msg FROM memories ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"记忆检索失败: {e}")
        return []

# ================= 从 RDS 加载历史对话（云端专用） =================
def load_history_from_rds(limit: int = 10) -> List[Dict[str, str]]:
    """从 RDS chat_memory 表加载最近的历史对话，返回 messages 格式列表"""
    try:
        conn = get_rds_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT user_msg, assistant_msg FROM chat_memory ORDER BY ts DESC LIMIT %s",
                (limit,)
            )
            rows = cursor.fetchall()
        conn.close()
        # 反转成时间正序（从早到晚）
        rows.reverse()
        history_messages = []
        for user_msg, assistant_msg in rows:
            if user_msg and user_msg.strip():
                history_messages.append({"role": "user", "content": user_msg})
            if assistant_msg and assistant_msg.strip():
                history_messages.append({"role": "assistant", "content": assistant_msg})
        return history_messages
    except Exception as e:
        logger.warning(f"从 RDS 加载历史对话失败: {e}")
        return []

@functools.lru_cache(maxsize=1)
def load_zhi_suan_mapping() -> Dict[str, Dict[str, str]]:
    mapping = {}
    try:
        if not os.path.exists(ZHI_SUAN_FILE):
            return mapping
        with open(ZHI_SUAN_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 5 and not line.startswith('子类') and '|---' not in line:
                    code, name = parts[4].strip(), parts[3].strip()
                    mapping[code] = {"name": name, "plate": parts[0].strip(), "sub": parts[1].strip()}
                    mapping[name] = mapping[code]
    except Exception as e:
        logger.error(f"智算池加载失败: {e}")
    return mapping

ZHI_SUAN_MAP = load_zhi_suan_mapping()

def save_current_session():
    if "messages" not in st.session_state or not st.session_state.messages:
        return
    history = load_history_cached()
    sid = st.session_state.get("current_session_id")
    user_msgs = [m for m in st.session_state.messages if m["role"] == "user"]
    if not user_msgs:
        return
    last_txt = user_msgs[-1]["content"]
    if isinstance(last_txt, list):
        last_txt = " ".join(p.get("text", "") for p in last_txt if isinstance(p, dict))
    stock_name = next((ZHI_SUAN_MAP[k]["name"] for k in ZHI_SUAN_MAP if k in last_txt), last_txt[:8])
    title = f"{stock_name} ({datetime.now(BEIJING_TZ).strftime('%Y%m%d')})"

    if sid:
        for s in history:
            if s.get("id") == sid:
                s.update({"messages": st.session_state.messages, "title": title, "updated_at": datetime.now(BEIJING_TZ).isoformat()})
                break
    else:
        sid = datetime.now(BEIJING_TZ).strftime("%Y%m%d%H%M%S")
        history.insert(0, {"id": sid, "title": title, "created_at": datetime.now(BEIJING_TZ).isoformat(), "updated_at": datetime.now(BEIJING_TZ).isoformat(), "messages": st.session_state.messages})
        st.session_state.current_session_id = sid

    try:
        if os.path.exists(HISTORY_FILE):
            shutil.copy2(HISTORY_FILE, HISTORY_FILE + ".bak")
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"保存历史会话失败: {e}")

def load_history_cached() -> List[Dict[str, Any]]:
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"加载历史失败: {e}")
    return []

def new_session():
    if st.session_state.messages:
        save_current_session()
    st.session_state.messages = []
    st.session_state.current_session_id = None
    st.session_state.uploaded_files = []
    st.session_state.processed_files = set()
    st.session_state.compressed_summary = ""
    st.rerun()

def delete_message_pair(idx: int):
    msgs = st.session_state.messages
    if 0 <= idx < len(msgs):
        del msgs[idx:]
        save_current_session()
        st.rerun()

def compress_history_by_date(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(messages) <= 20:
        st.session_state.compressed_summary = ""
        return messages
    today_str = datetime.now(BEIJING_TZ).strftime("%Y%m%d")
    today_m, early_m = [], []
    for m in messages:
        (today_m if m.get("date", today_str) == today_str else early_m).append(m)
    if early_m:
        summary_parts = ["📋 旧对话摘要（已压缩，可复制到新对话使用）：\n"]
        for m in early_m[-5:]:
            role = "用户" if m["role"] == "user" else "助手"
            content = str(m.get("content", ""))[:200]
            summary_parts.append(f"{role}：{content}...")
        st.session_state.compressed_summary = "\n".join(summary_parts)
    else:
        st.session_state.compressed_summary = ""
    return today_m + early_m[-5:]

_MARKET_CACHE = {"vars": {}, "ts": 0}

def get_market_vars() -> Dict[str, str]:
    if time.time() - _MARKET_CACHE["ts"] < 300 and _MARKET_CACHE["vars"]:
        return _MARKET_CACHE["vars"]
    now = datetime.now(BEIJING_TZ)
    wd = now.weekday()
    wds = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    ct = now.time()
    sess = "休市" if wd >= 5 else ("盘前" if ct < dt_time(9, 15) else ("盘中" if (dt_time(9, 15) <= ct <= dt_time(11, 30) or dt_time(13, 0) <= ct <= dt_time(15, 0)) else ("午休" if dt_time(11, 30) < ct < dt_time(13, 0) else "收盘")))
    vars = {"CURRENT_DATE": now.strftime("%Y-%m-%d %H:%M:%S"), "WEEKDAY": wds[wd], "MARKET_SESSION": sess, "INDEX_STATUS": "数据获取中", "VOLUME_TREND": "数据获取中", "MARKET_PHASE": "震荡"}
    if TUSHARE_TOKEN:
        try:
            pro = ts.pro_api(TUSHARE_TOKEN)
            df_sh = pro.index_daily(ts_code='000001.SH', limit=1)
            df_sz = pro.index_daily(ts_code='399001.SZ', limit=1)
            if not df_sh.empty and not df_sz.empty:
                sc, sp = float(df_sh['close'].iloc[0]), float(df_sh['pct_chg'].iloc[0])
                zc, zp = float(df_sz['close'].iloc[0]), float(df_sz['pct_chg'].iloc[0])
                vol = (float(df_sh['amount'].iloc[0]) + float(df_sz['amount'].iloc[0])) / 1e8
                vars.update({"INDEX_STATUS": f"上证{int(sc)}({sp:+.2f}%) 深证{int(zc)}({zp:+.2f}%)"})
                vars["MARKET_PHASE"] = "调整" if sp < -1 else ("主升" if sp > 1 else "震荡")
                vars["VOLUME_TREND"] = f"{'缩量' if vol < 1.5 else '放量'}至{vol:.1f}万亿"
        except Exception as e:
            logger.warning(f"Tushare拉取失败: {e}")
    _MARKET_CACHE["vars"], _MARKET_CACHE["ts"] = vars, time.time()
    return vars

def sanitize_text(text: str, max_len: int = 800) -> str:
    if not text:
        return ""
    cleaned = text.replace('\n', ' ').replace('\r', ' ')
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len] + "..."
    return cleaned

def inject_memory(query: str, force_latest: bool = False) -> str:
    if force_latest:
        res = search_memory("", limit=10)
    else:
        if not query:
            return ""
        keyword = query.split()[0] if query.split() else query
        res = search_memory(keyword, limit=2)
    if not res:
        return ""
    mem_lines = ["\n【历史记忆参考】"]
    for u, a in res:
        mem_lines.append(f"问: {sanitize_text(u, 200)}")
        mem_lines.append(f"答: {sanitize_text(a, 200)}")
    return "\n".join(mem_lines) + "\n"

def call_bailian_once(messages: List[Dict], scheme: str, mvars: Dict, hint: str) -> Tuple[str, int]:
    global _MODEL_HEALTHY
    if not is_model_healthy():
        raise RuntimeError("🔴 服务暂时不可用，请稍后重试（熔断已触发）")
    dashscope.api_key = DASHSCOPE_API_KEY

    current_time_str = mvars['CURRENT_DATE']
    weekday_str = mvars['WEEKDAY']
    session_str = mvars['MARKET_SESSION']
    sys_p = f"""你是智飞投研助手。当前时间:{current_time_str} | 时段:{session_str} | 指数:{mvars['INDEX_STATUS']} | 量能:{mvars['VOLUME_TREND']}
分析方案:{scheme if scheme else '日常对话'}
规则:直接输出结论+关键数据+风险提示。不展示工具调用过程。
报告开头必须使用以下准确时间戳，且不得修改：{current_time_str} 周{weekday_str} {session_str}
附加参考: {hint}"""

    full_msgs = [{"role": "system", "content": sys_p}]
    for m in messages:
        role = m["role"]
        content = str(m.get("content", ""))
        if role == "user":
            content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content)
        full_msgs.append({"role": role, "content": content})

    base_tok = estimate_tokens(sys_p) + estimate_messages_tokens(messages)
    retries, delay = 3, 2

    for attempt in range(retries):
        try:
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=full_msgs, result_format="message", stream=False)
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                full_text = resp.output.choices[0].message.content
                if not full_text or not full_text.strip():
                    raise RuntimeError("模型返回内容为空")
                reset_health_status()
                total_tok = base_tok + estimate_tokens(full_text)
                return full_text, total_tok
            else:
                raise RuntimeError(f"API Error: {resp.code} {resp.message}")
        except Exception as e:
            logger.warning(f"第{attempt+1}次调用失败: {e}")
            if attempt == retries - 1:
                mark_failure()
                raise RuntimeError(f"❌ 模型连续{retries}次调用失败，服务已熔断")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("❌ 未知错误：无法获取有效响应")

def export_md(msgs):
    """纯内容拼接导出（无问答标签）"""
    contents = []
    for m in msgs:
        if m["role"] == "assistant":
            c = str(m.get("content", "")).replace("data:image/png;base64,", "").strip()
            if c:
                contents.append(c)
    if not contents:
        return "# 暂无内容"
    return "\n\n---\n\n".join(contents)

def export_docx(msgs):
    """
    公众号发布级导出
    - 清理所有 Markdown 标记（# * - 1. 等）
    - 微软雅黑、字号层级、首行缩进2字符、两端对齐
    - 自动识别标题/小标题/正文/声明
    """
    doc = Document()

    # 设置默认字体（微软雅黑）
    style = doc.styles['Normal']
    style.font.name = '微软雅黑'
    style._element.rPr.rFonts.set(qn('w:eastAsia'), '微软雅黑')

    # 收集所有模型生成内容（只取 assistant）
    contents = []
    for m in msgs:
        if m["role"] == "assistant":
            c = str(m.get("content", "")).strip()
            if c:
                contents.append(c)

    if not contents:
        doc.add_paragraph("暂无内容")
        buf = io.BytesIO()
        doc.save(buf)
        buf.seek(0)
        return buf.getvalue()

    full_text = "\n\n".join(contents)
    lines = full_text.split('\n')

    # 状态变量
    is_first_line = True
    article_title = None

    # 第一遍：识别文章标题（第一行非空内容）
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith('#'):
            stripped = re.sub(r'^#+\s*', '', stripped)
        article_title = stripped
        break

    # 第二遍：逐行处理
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # 清理 Markdown 标记
        cleaned = stripped
        cleaned = re.sub(r'^#+\s*', '', cleaned)
        cleaned = re.sub(r'^[-*]\s+', '', cleaned)
        cleaned = re.sub(r'^\d+\.\s+', '', cleaned)
        cleaned = re.sub(r'\*\*(.*?)\*\*', r'\1', cleaned)

        # ---- 判断行类型 ----
        # 小标题检测（中文序号 一、二、 或 1、2、；或短句不含句号）
        is_subtitle = False
        if re.match(r'^[一二三四五六七八九十]+[、．]\s*', cleaned) or re.match(r'^\(?\d+\)?[、．]\s*', cleaned):
            is_subtitle = True
        if len(cleaned) < 25 and '。' not in cleaned and '，' not in cleaned and '：' not in cleaned:
            is_subtitle = True

        # 结尾声明检测
        is_disclaimer = False
        disclaimer_keywords = ['不构成投资建议', '投资有风险', '风险提示', '股市有风险', '智飞整理', '仅供参考', '谨慎操作']
        if any(kw in cleaned for kw in disclaimer_keywords):
            is_disclaimer = True

        # ---- 应用样式 ----
        p = doc.add_paragraph()

        # 文章标题
        if is_first_line and article_title and cleaned == article_title:
            run = p.add_run(cleaned)
            run.font.size = Pt(22)
            run.font.bold = True
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            p.paragraph_format.space_after = Pt(18)
            is_first_line = False
            continue

        # 小标题
        if is_subtitle and not is_disclaimer:
            run = p.add_run(cleaned)
            run.font.size = Pt(18)
            run.font.bold = True
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(14)
            p.paragraph_format.space_after = Pt(8)
            is_first_line = False
            continue

        # 结尾声明
        if is_disclaimer:
            run = p.add_run(cleaned)
            run.font.size = Pt(14)
            run.font.bold = False
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(12)
            p.paragraph_format.space_after = Pt(4)
            is_first_line = False
            continue

        # 正文
        run = p.add_run(cleaned)
        run.font.size = Pt(16)
        run.font.bold = False
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        p.paragraph_format.first_line_indent = Pt(32)
        p.paragraph_format.line_spacing = 1.5
        p.paragraph_format.space_after = Pt(6)
        is_first_line = False

    # 如果没有识别到文章标题，补一个默认
    if not article_title and contents:
        p = doc.add_paragraph("智飞行情研判")
        p.runs[0].font.size = Pt(22)
        p.runs[0].font.bold = True
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.getvalue()

# ================= Streamlit UI =================
st.set_page_config(page_title="智飞投研系统", layout="wide", page_icon="📈")

# 防闪 CSS
st.markdown("""
<style>
    .stApp, section.main, .main, [data-testid="stAppViewContainer"] {
        background: #ffffff !important;
        transition: none !important;
        animation: none !important;
        will-change: auto !important;
    }
    [aria-label="Loading..."], [data-testid="stLoadingIndicator"] {
        opacity: 0 !important;
        pointer-events: none !important;
    }
    .stChatInputContainer {
        position: sticky !important;
        bottom: 0 !important;
        background: #ffffff !important;
        padding: 12px 0 8px 0 !important;
        z-index: 999 !important;
        border-top: 1px solid #e5e7eb !important;
        box-shadow: 0 -4px 10px rgba(0,0,0,0.03) !important;
    }
    .stChatMessage { margin-bottom: 8px; }
    [data-testid="stChatFloatingInputContainer"] {
        background: #ffffff !important;
        border-radius: 12px !important;
        border: 1px solid #d1d5db !important;
        padding: 4px 12px !important;
    }
    img { max-width: 100% !important; height: auto !important; border-radius: 4px !important; }
    .file-status {
        font-size: 13px;
        color: #64748b;
        margin-top: 4px;
    }
    /* 缩窄文件上传器的列宽 */
    .stFileUploader > div:first-child {
        width: 40px !important;
        min-width: 40px !important;
    }
    .stFileUploader button {
        padding: 4px 8px !important;
        font-size: 18px !important;
        height: 38px !important;
        width: 40px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    .stFileUploader [data-testid="stFileUploadDropzone"] {
        width: 40px !important;
        min-width: 40px !important;
        padding: 2px !important;
    }
    .stFileUploader [data-testid="stFileUploadDropzone"] > div:first-child {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

init_memory_db()

if "messages" not in st.session_state:
    st.session_state.messages = []
if "current_session_id" not in st.session_state:
    st.session_state.current_session_id = None
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = []
if "processed_files" not in st.session_state:
    st.session_state.processed_files = set()
if "editing_msg_idx" not in st.session_state:
    st.session_state.editing_msg_idx = -1
if "edit_content" not in st.session_state:
    st.session_state.edit_content = ""
if "pending_generation" not in st.session_state:
    st.session_state.pending_generation = False
if "quick_prompt" not in st.session_state:
    st.session_state.quick_prompt = None
if "quick_scheme" not in st.session_state:
    st.session_state.quick_scheme = None
if "last_token_usage" not in st.session_state:
    st.session_state.last_token_usage = 0
if "history_page" not in st.session_state:
    st.session_state.history_page = 1
if "compressed_summary" not in st.session_state:
    st.session_state.compressed_summary = ""

with st.sidebar:
    st.markdown("### ⚡ 快捷分析")
    for lbl, txt, sch in [("📊 盘前分析", "盘前分析", "方案 A-盘前"), ("🔗 产业链扫描", "扫描产业链", "方案 B-产业链"), ("📈 行情判断", "现在市场什么阶段", "方案 D-行情"), ("💰 资金监控", "资金在往哪去", "方案 E-资金")]:
        if st.button(lbl, key=f"q_{sch}", use_container_width=True):
            st.session_state.quick_prompt, st.session_state.quick_scheme = txt, sch
            st.rerun()

    st.divider()
    st.subheader("📊 分析方案")
    schemes = {"方案 A-盘前": "方案 A-盘前", "方案 B-产业链": "方案 B-产业链", "方案 C-卡脖子推演": "方案 C-卡脖子", "方案 D-行情判断": "方案 D-行情", "方案 E-资金流转": "方案 E-资金", "方案 F-个股深度": "方案 F-个股"}
    selected_scheme = None
    for l, v in schemes.items():
        if st.checkbox(l, key=f"s_{v}", value=(st.session_state.quick_scheme == v)):
            selected_scheme = v
    st.info("💬 日常对话" if not selected_scheme else f"📊 {selected_scheme}")

    st.divider()
    st.subheader("📋 历史会话")
    history = load_history_cached()
    if history:
        page_size = 10
        start_idx = (st.session_state.history_page - 1) * page_size
        end_idx = start_idx + page_size
        for s in history[start_idx:end_idx]:
            active = s.get("id") == st.session_state.current_session_id
            c1, c2 = st.columns([5, 1])
            with c1:
                if st.button(s.get("title", "未命名"), key=f"h_{s.get('id')}", use_container_width=True):
                    if not active:
                        st.session_state.messages = s.get("messages", [])
                        st.session_state.current_session_id = s.get('id')
                        st.rerun()
            with c2:
                if st.button("✕", key=f"d_{s.get('id')}"):
                    st.session_state.messages = []
                    st.session_state.current_session_id = None
                    st.rerun()
        total_pages = (len(history) + page_size - 1) // page_size
        if st.session_state.history_page < total_pages:
            if st.button("📥 加载更多历史", key="load_more_hist", use_container_width=True):
                st.session_state.history_page += 1
                st.rerun()
    else:
        st.caption("暂无历史会话")
    if st.button("➕ 新建会话", use_container_width=True):
        new_session()

# ================= 渲染最近10轮（即最近20条消息） =================
MAX_RENDER_ROUNDS = 10
render_limit = MAX_RENDER_ROUNDS * 2

render_messages = st.session_state.messages[-render_limit:] if len(st.session_state.messages) > render_limit else st.session_state.messages

for idx, msg in enumerate(render_messages):
    with st.chat_message(msg["role"]):
        if msg.get("txt_files"):
            for tf in msg["txt_files"]:
                st.caption(f"📎 {tf['name']}")
        content = msg.get("content", "")
        if isinstance(content, list):
            for p in content:
                if isinstance(p, dict) and p.get("text"):
                    st.markdown(p["text"])
        else:
            st.markdown(str(content).replace("data:image/png;base64,", ""))

        col_btns = st.columns([2, 1, 1, 1])
        with col_btns[0]:
            if msg["role"] == "user" and st.button("✏️ 编辑", key=f"e_{idx}"):
                st.session_state.editing_msg_idx = idx
                st.session_state.edit_content = str(content) if not isinstance(content, list) else ""
                st.rerun()
        with col_btns[1]:
            if st.button("🗑️ 删除", key=f"d_{idx}"):
                delete_message_pair(idx)
        with col_btns[2]:
            if msg["role"] == "assistant":
                md_data = export_md([msg])
                st.download_button("📥 MD", data=md_data, file_name=f"分析_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.md", mime="text/markdown", key=f"md_{idx}", use_container_width=True)
        with col_btns[3]:
            if msg["role"] == "assistant":
                docx_data = export_docx([msg])
                st.download_button("📄 DOCX", data=docx_data, file_name=f"分析_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", key=f"docx_{idx}", use_container_width=True)

# 显示压缩摘要
if st.session_state.compressed_summary:
    with st.chat_message("assistant"):
        st.markdown("📋 以下为已压缩的旧对话摘要，可复制后在新对话中粘贴使用：")
        st.text_area("摘要内容", value=st.session_state.compressed_summary, height=150, key="compressed_summary_display", disabled=True)
        st.caption("💡 选中上方文本后 Ctrl+C 复制，即可在新对话中粘贴使用")

# 编辑消息
if st.session_state.editing_msg_idx >= 0:
    idx = st.session_state.editing_msg_idx
    if idx < len(st.session_state.messages):
        with st.chat_message("user"):
            nc = st.text_area("编辑消息", value=st.session_state.edit_content, key="edit_ta", height=80)
            b1, b2 = st.columns(2)
            with b1:
                if st.button("✅ 重新发送", key="send_e"):
                    if nc.strip():
                        del st.session_state.messages[idx:]
                        st.session_state.editing_msg_idx = -1
                        st.session_state.messages.append({"role": "user", "content": nc.strip(), "date": datetime.now(BEIJING_TZ).strftime("%Y%m%d")})
                        st.rerun()
            with b2:
                if st.button("取消", key="cancel_e"):
                    st.session_state.editing_msg_idx = -1
                    st.rerun()

# ================= 底部输入区（紧凑布局：输入框 + 📎文件按钮同行） =================
st.divider()

# ---- 已挂载文件显示 ----
if st.session_state.uploaded_files:
    file_names = [f["name"] for f in st.session_state.uploaded_files]
    st.caption("📎 " + " | ".join(file_names))
    if st.button("🗑️ 清空附件", key="clear_attachments", use_container_width=False):
        st.session_state.uploaded_files = []
        st.session_state.processed_files = set()
        st.rerun()

# ---- 输入行：输入框 + 文件上传按钮（同行） ----
col_input, col_file, _ = st.columns([8, 1, 1])

with col_input:
    prompt = st.chat_input("输入股票/行业/事件，或描述你的需求...", key="main_input_fixed")
    if not prompt and st.session_state.quick_prompt:
        prompt = st.session_state.quick_prompt
        st.session_state.quick_prompt = None
        st.session_state.quick_scheme = None

with col_file:
    # 极简文件上传器（只显示小图标按钮）
    uploaded_file = st.file_uploader(
        "📎",
        type=['txt', 'log', 'csv', 'md', 'py', 'json'],
        key="file_uploader_widget",
        label_visibility="collapsed",
        help="选择本地文件（txt/log/csv/md/py/json）"
    )
    if uploaded_file is not None:
        file_id = uploaded_file.name + str(uploaded_file.size)
        if file_id not in st.session_state.processed_files:
            try:
                content = uploaded_file.read().decode('utf-8', errors='ignore')
                if len(content) > 100000:
                    content = content[:100000] + "\n... (文件过大，已截断)"
                st.session_state.uploaded_files.append({
                    "type": "txt",
                    "data": content,
                    "name": uploaded_file.name
                })
                st.session_state.processed_files.add(file_id)
                st.success(f"✅ 已挂载: {uploaded_file.name}")
                st.rerun()
            except Exception as e:
                st.error(f"❌ 读取文件失败: {e}")

# ================= 核心执行逻辑 =================
if prompt:
    if not st.session_state.messages:
        mh = inject_memory(prompt, force_latest=True)
    else:
        mh = inject_memory(prompt, force_latest=False)

    uc = prompt
    if st.session_state.uploaded_files:
        file_contents = []
        for f in st.session_state.uploaded_files:
            if f["type"] == "txt":
                content = sanitize_text(f["data"], 800)
                file_contents.append(f"[{f['name']}]\n{content}")
        if file_contents:
            uc = uc + "\n\n【文件内容】\n" + "\n".join(file_contents)

    umsg = {"role": "user", "content": uc, "date": datetime.now(BEIJING_TZ).strftime("%Y%m%d"), "txt_files": [f for f in st.session_state.uploaded_files if f["type"] == "txt"]}
    st.session_state.messages.append(umsg)
    st.session_state.uploaded_files = []
    st.session_state.processed_files = set()
    save_current_session()
    st.session_state.pending_generation = True
    st.rerun()

if st.session_state.pending_generation:
    st.session_state.pending_generation = False
    if not is_model_healthy():
        st.error("🔴 服务暂时不可用，请稍后重试（模型服务已熔断）")
        st.stop()

    # ================= 云端：从 RDS 加载历史对话到 messages =================
    rds_history = load_history_from_rds(limit=10)
    if rds_history:
        # 把 RDS 历史插入到当前 messages 前面（但保留当前 session 的最新消息）
        st.session_state.messages = rds_history + st.session_state.messages

    comp_msgs = compress_history_by_date(st.session_state.messages)
    if not st.session_state.messages:
        mh = inject_memory("", force_latest=True)
    else:
        mh = inject_memory(st.session_state.messages[-1].get("content", ""), force_latest=False)

    with st.chat_message("assistant"):
        try:
            mvars = get_market_vars()
            full_txt, tok_used = call_bailian_once(comp_msgs, selected_scheme, mvars, mh)
            st.markdown(full_txt)
            st.session_state.last_token_usage = tok_used
            if comp_msgs and comp_msgs[-1].get("content"):
                save_memory(comp_msgs[-1]["content"], full_txt)
            if selected_scheme and tok_used >= TOKEN_DISPLAY_THRESHOLD:
                st.caption(f"🔢 本次消耗: ~{tok_used:,} tokens")
            st.session_state.messages.append({"role": "assistant", "content": full_txt, "date": datetime.now(BEIJING_TZ).strftime("%Y%m%d")})
            save_current_session()
        except RuntimeError as e:
            st.error(str(e))
            if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
                st.session_state.messages.pop()
        except Exception as e:
            st.error(f"❌ 未知错误: {e}")
            logger.error(f"未知错误: {e}", exc_info=True)

# ================= 底部全局导出 =================
if st.session_state.messages:
    st.divider()
    st.caption("💡 提示：每条AI回复下方有单条导出按钮。下方为全部对话导出。")
    col_all_md, col_all_docx = st.columns(2)
    with col_all_md:
        all_md = export_md(st.session_state.messages)
        st.download_button("📥 导出全部对话为MD", data=all_md, file_name=f"全部对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.md", mime="text/markdown", key="all_md", use_container_width=True)
    with col_all_docx:
        all_docx = export_docx(st.session_state.messages)
        st.download_button("📄 导出全部对话为DOCX", data=all_docx, file_name=f"全部对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", key="all_docx", use_container_width=True)
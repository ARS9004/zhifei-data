#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 百炼模型API前端 v6.8-CLOUD-RDS（2026-07-28）
- 只渲染最近10轮对话（20条消息）
- 文件上传：系统原生文件选择器
- 导出：公众号发布级格式
- 【云端专版】所有记忆读写走 RDS（chat_memory 表）
- 底部布局：输入框 + 📎文件按钮同行
- 历史对话加载：最近10轮（20条消息）
- 调用百炼应用 API（自动携带控制台配置的所有工具）
- 调试信息直接显示在界面
"""

import os
import re
import json
import time
import logging
import functools
import sqlite3
import io
import shutil
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Optional, Tuple

import streamlit as st
import dashscope
import requests
import tushare as ts
import pytz
import pymysql
from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from http import HTTPStatus
from dotenv import load_dotenv

load_dotenv()

# ================= 环境变量读取 =================
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
if not DASHSCOPE_API_KEY:
    raise RuntimeError("⛔ 请配置环境变量 DASHSCOPE_API_KEY")

BAILIAN_APP_ID = os.getenv("BAILIAN_APP_ID")  # 可选，如果没有则用 dashscope 直接调用
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")
MODEL_NAME = os.getenv("MODEL_NAME", "qwen-plus")

BEIJING_TZ = pytz.timezone('Asia/Shanghai')
ZHI_SUAN_FILE = os.getenv("ZHI_SUAN_FILE", "./智算池完整版_同花顺.txt")
HISTORY_FILE = os.getenv("HISTORY_FILE", "./chat_history.json")

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

# ================= 云端版记忆库（纯 RDS） =================
def init_memory_db():
    logger.info("☁️ 云端模式：记忆库使用 RDS")
    pass

def save_memory(u: str, a: str):
    try:
        conn = get_rds_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO chat_memory (user_msg, assistant_msg) VALUES (%s, %s)",
                (u, a)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"RDS 记忆保存失败: {e}")

def search_memory(keyword: str = "", limit: int = 2) -> List[Tuple[str, str]]:
    try:
        conn = get_rds_connection()
        with conn.cursor() as cursor:
            if keyword:
                cursor.execute(
                    "SELECT user_msg, assistant_msg FROM chat_memory "
                    "WHERE user_msg LIKE %s OR assistant_msg LIKE %s "
                    "ORDER BY id DESC LIMIT %s",
                    (f"%{keyword}%", f"%{keyword}%", limit)
                )
            else:
                cursor.execute(
                    "SELECT user_msg, assistant_msg FROM chat_memory "
                    "ORDER BY id DESC LIMIT %s",
                    (limit,)
                )
            rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"RDS 记忆检索失败: {e}")
        return []

def load_history_from_rds(limit: int = 10) -> List[Dict[str, str]]:
    try:
        conn = get_rds_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT user_msg, assistant_msg FROM chat_memory ORDER BY id DESC LIMIT %s",
                (limit,)
            )
            rows = cursor.fetchall()
        conn.close()
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

# ================= 百炼 API 调用（优先应用API，降级dashscope） =================
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
            content = re.sub(r'
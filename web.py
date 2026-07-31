#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 本地完整版 v7.0（2026-07-31）
- 本地端主力：RDS 主存储 + SQLite 本地备份
- 手机端：OSS 同步，摘要 + 3轮对话恢复
- 双写：每轮对话实时写入 RDS + SQLite，每10轮同步 OSS
- 上下文恢复：摘要 + 最后5轮，启动时 RDS → OSS → SQLite 三级回退
"""

import os
import re
import json
import time
import uuid
import logging
import functools
import sqlite3
import io
import shutil
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Optional, Tuple

import streamlit as st
import dashscope
import tushare as ts
import pytz
import oss2
import pymysql
from PIL import Image
from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from http import HTTPStatus
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

load_dotenv()

# ================= 环境变量 =================
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
if not DASHSCOPE_API_KEY:
    raise RuntimeError("⛔ 请配置环境变量 DASHSCOPE_API_KEY")

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")
MODEL_NAME = os.getenv("MODEL_NAME", "qwen-plus")
BEIJING_TZ = pytz.timezone('Asia/Shanghai')
ZHI_SUAN_FILE = os.getenv("ZHI_SUAN_FILE", "./智算池完整版_同花顺.txt")
HISTORY_FILE = os.getenv("HISTORY_FILE", "./chat_history.json")
MEMORY_DB_PATH = os.getenv("MEMORY_DB_PATH", "./chat_memory.db")

# ================= RDS 配置 =================
RDS_HOST = "rm-2zeli1or40iqt7vq66o.mysql.rds.aliyuncs.com"
RDS_PORT = 3306
RDS_USER = "zhuanz1"
RDS_PASSWORD = "zhuanz1_2026"
RDS_DATABASE = "stock_db"
RDS_CHAT_TABLE = "chat_memory"

# ================= OSS 配置 =================
OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"
OSS_FILENAME = "chat_history.jsonl"
OSS_SUMMARY_FILE = "chat_summary.json"

# ================= 日志 =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('app.log', mode='a', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ================= 熔断 =================
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

# ================= 辅助函数 =================
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

def sanitize_text(text: str, max_len: int = 800) -> str:
    if not text: return ""
    cleaned = text.replace('\n', ' ').replace('\r', ' ')
    if len(cleaned) > max_len: cleaned = cleaned[:max_len] + "..."
    return cleaned

CONTEXT_LIMIT = 1_000_000
TOKEN_DISPLAY_THRESHOLD = 5000
SESSION_GAP_SECONDS = 3600  # 1小时

# ================= RDS 操作 =================
def get_rds_connection():
    return pymysql.connect(
        host=RDS_HOST, port=RDS_PORT, user=RDS_USER,
        password=RDS_PASSWORD, database=RDS_DATABASE, charset='utf8mb4'
    )

def get_or_create_session() -> str:
    """获取或创建当前会话 ID，距上次 > 1小时自动新建"""
    if "session_id" not in st.session_state:
        try:
            conn = get_rds_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT session_id, ts FROM chat_memory ORDER BY ts DESC LIMIT 1")
            row = cursor.fetchone()
            conn.close()
            if row:
                last_session_id, last_ts = row
                last_dt = datetime.strptime(str(last_ts), '%Y-%m-%d %H:%M:%S')
                last_dt = BEIJING_TZ.localize(last_dt) if last_dt.tzinfo is None else last_dt
                now = datetime.now(BEIJING_TZ)
                if (now - last_dt).total_seconds() < SESSION_GAP_SECONDS:
                    st.session_state.session_id = last_session_id
                else:
                    st.session_state.session_id = str(uuid.uuid4())
            else:
                st.session_state.session_id = str(uuid.uuid4())
        except Exception as e:
            logger.warning(f"session 获取失败: {e}")
            st.session_state.session_id = str(uuid.uuid4())
    return st.session_state.session_id

def load_context_from_rds() -> dict:
    """
    从 RDS 恢复上下文：摘要 + 最后5轮完整对话。
    返回 {"summary": str, "recent_messages": list, "session_id": str}
    """
    result = {"summary": "", "recent_messages": [], "session_id": None}
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT session_id, ts FROM chat_memory ORDER BY ts DESC LIMIT 1")
        row = cursor.fetchone()
        if not row:
            conn.close()
            return result

        session_id, last_ts = row
        last_dt = datetime.strptime(str(last_ts), '%Y-%m-%d %H:%M:%S')
        last_dt = BEIJING_TZ.localize(last_dt) if last_dt.tzinfo is None else last_dt
        now = datetime.now(BEIJING_TZ)

        if (now - last_dt).total_seconds() >= SESSION_GAP_SECONDS:
            result["session_id"] = str(uuid.uuid4())
            restore_session_id = session_id
        else:
            result["session_id"] = session_id
            restore_session_id = session_id

        # 取摘要
        cursor.execute(
            "SELECT summary FROM chat_summary WHERE session_id = %s ORDER BY created_at DESC LIMIT 1",
            (restore_session_id,)
        )
        summary_row = cursor.fetchone()
        if summary_row:
            result["summary"] = summary_row[0]

        # 取最后5轮完整对话
        cursor.execute(
            "SELECT messages, ts FROM chat_memory WHERE session_id = %s ORDER BY round_num DESC LIMIT 5",
            (restore_session_id,)
        )
        rows = cursor.fetchall()
        conn.close()

        for messages_json, ts in reversed(rows):
            data = json.loads(messages_json)
            for msg in data.get("messages", []):
                result["recent_messages"].append({
                    "role": msg.get("role"),
                    "content": msg.get("content"),
                    "timestamp": str(ts),
                    "session_id": restore_session_id
                })
        return result
    except Exception as e:
        logger.warning(f"RDS 上下文恢复失败: {e}")
        return result

def save_to_rds(session_id: str, round_num: int, messages: dict, ts: str):
    """写入 RDS"""
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        messages_json = json.dumps(messages, ensure_ascii=False)
        cursor.execute(
            f"INSERT INTO {RDS_CHAT_TABLE} (session_id, round_num, messages, ts) VALUES (%s, %s, %s, %s)",
            (session_id, round_num, messages_json, ts)
        )
        conn.commit()
        conn.close()
        logger.info(f"✅ RDS 写入成功: session={session_id}, round={round_num}")
    except Exception as e:
        logger.warning(f"RDS 写入失败: {e}")

# ================= OSS 操作 =================
def get_oss_client():
    access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
    if not access_key_id or not access_key_secret:
        raise RuntimeError("⛔ 请配置环境变量 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
    client = AcsClient(access_key_id, access_key_secret, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

def sync_to_oss():
    """
    增量同步到 OSS：读取现有 JSONL → 追加新记录 → 写回。
    固定文件名 chat_history.jsonl，按 (session_id, round_num) 去重。
    """
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + OSS_FILENAME

        existing_rounds = set()
        existing_lines = []
        try:
            result = bucket.get_object(remote_path)
            content = result.read().decode('utf-8')
            for line in content.strip().split('\n'):
                if line.strip():
                    data = json.loads(line)
                    existing_rounds.add((data.get("session_id"), data.get("round_num")))
                    existing_lines.append(line)
        except:
            pass

        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT session_id, round_num, messages, ts FROM chat_memory_new ORDER BY ts ASC, round_num ASC"
        )
        rows = cursor.fetchall()
        conn.close()

        new_count = 0
        for session_id, round_num, messages_json, ts in rows:
            if (session_id, round_num) in existing_rounds:
                continue
            line = {
                "session_id": session_id,
                "round_num": round_num,
                "messages": json.loads(messages_json),
                "ts": ts
            }
            existing_lines.append(json.dumps(line, ensure_ascii=False))
            existing_rounds.add((session_id, round_num))
            new_count += 1

        if new_count == 0:
            logger.info("📭 OSS 无需同步")
            return

        content = "\n".join(existing_lines) + "\n"
        bucket.put_object(remote_path, content.encode('utf-8'))
        logger.info(f"✅ OSS 同步成功: 新增 {new_count} 条，总计 {len(existing_lines)} 条")

        # 同步摘要
        sync_summary_to_oss()
    except Exception as e:
        logger.warning(f"OSS 同步失败: {e}")

def sync_summary_to_oss():
    """同步最新摘要到 OSS，供手机端恢复上下文"""
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT summary FROM chat_summary ORDER BY created_at DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            data = {"summary": row[0], "updated_at": datetime.now(BEIJING_TZ).isoformat()}
            bucket = get_oss_client()
            remote = OSS_PREFIX + OSS_SUMMARY_FILE
            bucket.put_object(remote, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            logger.info("✅ 摘要同步到 OSS 成功")
    except Exception as e:
        logger.warning(f"摘要同步 OSS 失败: {e}")

def sync_from_oss():
    """
    从 OSS 读取增量数据，合并到 RDS 和本地 SQLite。
    按 (session_id, round_num) 去重。
    """
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + OSS_FILENAME

        try:
            result = bucket.get_object(remote_path)
            content = result.read().decode('utf-8')
        except:
            logger.info("📭 OSS 远程文件不存在")
            return

        lines = content.strip().split('\n')

        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT session_id, round_num FROM chat_memory_new")
        existing = {(row[0], row[1]) for row in cursor.fetchall()}

        rds_conn = get_rds_connection()
        rds_cursor = rds_conn.cursor()
        rds_cursor.execute("SELECT session_id, round_num FROM chat_memory")
        rds_existing = {(row[0], row[1]) for row in rds_cursor.fetchall()}

        new_count = 0
        for line in lines:
            if not line.strip():
                continue
            data = json.loads(line)
            sid = data.get("session_id")
            rn = data.get("round_num")
            key = (sid, rn)

            if key in existing and key in rds_existing:
                continue

            messages_json = json.dumps(data.get("messages"), ensure_ascii=False)
            ts = data.get("ts", datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S'))

            if key not in existing:
                cursor.execute(
                    "INSERT INTO chat_memory_new (session_id, round_num, messages, ts) VALUES (?, ?, ?, ?)",
                    (sid, rn, messages_json, ts)
                )
                existing.add(key)

            if key not in rds_existing:
                rds_cursor.execute(
                    "INSERT INTO chat_memory (session_id, round_num, messages, ts) VALUES (%s, %s, %s, %s)",
                    (sid, rn, messages_json, ts)
                )
                rds_existing.add(key)

            new_count += 1

        conn.commit()
        conn.close()
        rds_conn.commit()
        rds_conn.close()

        logger.info(f"✅ OSS 导入成功: 新增 {new_count} 条")
    except Exception as e:
        logger.warning(f"OSS 导入失败: {e}")

# ================= SQLite 操作 =================
def init_memory_db():
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS chat_memory_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                round_num INTEGER NOT NULL,
                messages TEXT NOT NULL,
                ts TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_session_local ON chat_memory_new(session_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ts_local ON chat_memory_new(ts)")
        conn.commit()
        conn.close()
        logger.info("✅ SQLite 初始化完成")
    except Exception as e:
        logger.error(f"SQLite 初始化失败: {e}")

def save_to_sqlite(session_id: str, round_num: int, messages: dict, ts: str):
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        messages_json = json.dumps(messages, ensure_ascii=False)
        cursor.execute(
            "INSERT INTO chat_memory_new (session_id, round_num, messages, ts) VALUES (?, ?, ?, ?)",
            (session_id, round_num, messages_json, ts)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"SQLite 写入失败: {e}")

def load_from_sqlite(limit: int = 5) -> List[Dict]:
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT session_id, round_num, messages, ts FROM chat_memory_new ORDER BY ts DESC, round_num DESC LIMIT ?",
            (limit,)
        )
        rows = cursor.fetchall()
        conn.close()
        result = []
        for session_id, round_num, messages_json, ts in reversed(rows):
            data = json.loads(messages_json)
            for msg in data.get("messages", []):
                result.append({
                    "role": msg.get("role"),
                    "content": msg.get("content"),
                    "timestamp": ts,
                    "session_id": session_id,
                    "round_num": round_num
                })
        return result
    except Exception as e:
        logger.warning(f"SQLite 读取失败: {e}")
        return []

def get_sqlite_round_count() -> int:
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM chat_memory_new")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except:
        return 0

# ================= 上下文注入 =================
def inject_memory() -> str:
    """
    从 RDS 恢复上下文：摘要 + 最后5轮。
    用于模型启动时注入 system prompt。
    """
    ctx = load_context_from_rds()
    parts = []

    if ctx["summary"]:
        parts.append(f"【历史对话摘要】\n{ctx['summary']}")

    if ctx["recent_messages"]:
        parts.append("\n【最近对话】")
        for m in ctx["recent_messages"]:
            role = "用户" if m["role"] == "user" else "助手"
            content = sanitize_text(m.get("content", ""), 300)
            parts.append(f"{role}：{content}")

    return "\n".join(parts) if parts else ""

# ================= 启动初始化 =================
def init_session_on_startup() -> list:
    """
    启动时恢复上下文：RDS → OSS → SQLite 三级回退。
    返回恢复的 messages 列表。
    """
    # 1. 尝试 RDS
    ctx = load_context_from_rds()
    if ctx["recent_messages"]:
        st.session_state.session_id = ctx["session_id"]
        st.session_state.rds_summary = ctx["summary"]
        return ctx["recent_messages"]

    # 2. 尝试 OSS 同步后再试 RDS
    try:
        sync_from_oss()
        ctx = load_context_from_rds()
        if ctx["recent_messages"]:
            st.session_state.session_id = ctx["session_id"]
            st.session_state.rds_summary = ctx["summary"]
            return ctx["recent_messages"]
    except:
        pass

    # 3. 回退 SQLite
    sqlite_msgs = load_from_sqlite(limit=10)
    if sqlite_msgs:
        st.session_state.session_id = sqlite_msgs[0].get("session_id")
        return sqlite_msgs

    return []

# ================= 百炼调用 =================
def call_bailian_once(messages: List[Dict], scheme: str, mvars: Dict, hint: str) -> Tuple[str, int]:
    global _MODEL_HEALTHY
    if not is_model_healthy():
        raise RuntimeError("🔴 服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY

    current_time_str = mvars['CURRENT_DATE']
    weekday_str = mvars['WEEKDAY']
    session_str = mvars['MARKET_SESSION']
    sys_p = f"""你是智飞投研助手。当前时间:{current_time_str} | 时段:{session_str} | 指数:{mvars['INDEX_STATUS']} | 量能:{mvars['VOLUME_TREND']}
分析方案:{scheme if scheme else '日常对话'}
规则:直接输出结论+关键数据+风险提示。不展示工具调用过程。"""

    if hint:
        sys_p = sys_p + "\n\n" + hint

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
                raise RuntimeError(f"❌ 模型连续{retries}次调用失败")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("❌ 未知错误")

# ================= 智算池 =================
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

# ================= 会话管理 =================
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
    st.session_state.sync_status = ""
    st.rerun()

def delete_message_pair(idx: int):
    msgs = st.session_state.messages
    if 0 <= idx < len(msgs):
        del msgs[idx:]
        save_current_session()
        st.rerun()

# ================= 行情变量 =================
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

# ================= 导出 =================
def export_md(msgs):
    contents = []
    for m in msgs:
        if m["role"] == "assistant":
            c = str(m.get("content", "")).replace("data:image/png;base64,", "").strip()
            if c: contents.append(c)
    if not contents: return "# 暂无内容"
    return "\n\n---\n\n".join(contents)

def export_docx(msgs):
    doc = Document()
    style = doc.styles['Normal']
    style.font.name = '微软雅黑'
    style._element.rPr.rFonts.set(qn('w:eastAsia'), '微软雅黑')

    contents = []
    for m in msgs:
        if m["role"] == "assistant":
            c = str(m.get("content", "")).strip()
            if c: contents.append(c)

    if not contents:
        doc.add_paragraph("暂无内容")
        buf = io.BytesIO(); doc.save(buf); buf.seek(0)
        return buf.getvalue()

    full_text = "\n\n".join(contents)
    lines = full_text.split('\n')
    is_first_line = True
    article_title = None

    for line in lines:
        stripped = line.strip()
        if not stripped: continue
        if stripped.startswith('#'):
            stripped = re.sub(r'^#+\s*', '', stripped)
        article_title = stripped
        break

    for line in lines:
        stripped = line.strip()
        if not stripped: continue
        cleaned = stripped
        cleaned = re.sub(r'^#+\s*', '', cleaned)
        cleaned = re.sub(r'^[-*]\s+', '', cleaned)
        cleaned = re.sub(r'^\d+\.\s+', '', cleaned)
        cleaned = re.sub(r'\*\*(.*?)\*\*', r'\1', cleaned)

        is_subtitle = False
        if re.match(r'^[一二三四五六七八九十]+[、．]\s*', cleaned) or re.match(r'^\(?\d+\)?[、．]\s*', cleaned):
            is_subtitle = True
        if len(cleaned) < 25 and '。' not in cleaned and '，' not in cleaned and '：' not in cleaned:
            is_subtitle = True

        is_disclaimer = False
        disclaimer_keywords = ['不构成投资建议', '投资有风险', '风险提示', '股市有风险', '智飞整理', '仅供参考', '谨慎操作']
        if any(kw in cleaned for kw in disclaimer_keywords):
            is_disclaimer = True

        p = doc.add_paragraph()

        if is_first_line and article_title and cleaned == article_title:
            run = p.add_run(cleaned)
            run.font.size = Pt(22); run.font.bold = True
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            p.paragraph_format.space_after = Pt(18)
            is_first_line = False; continue

        if is_subtitle and not is_disclaimer:
            run = p.add_run(cleaned)
            run.font.size = Pt(18); run.font.bold = True
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(14)
            p.paragraph_format.space_after = Pt(8)
            is_first_line = False; continue

        if is_disclaimer:
            run = p.add_run(cleaned)
            run.font.size = Pt(14); run.font.bold = False
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(12)
            p.paragraph_format.space_after = Pt(4)
            is_first_line = False; continue

        run = p.add_run(cleaned)
        run.font.size = Pt(16); run.font.bold = False
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        p.paragraph_format.first_line_indent = Pt(32)
        p.paragraph_format.line_spacing = 1.5
        p.paragraph_format.space_after = Pt(6)
        is_first_line = False

    if not article_title and contents:
        p = doc.add_paragraph("智飞行情研判")
        p.runs[0].font.size = Pt(22); p.runs[0].font.bold = True
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    buf = io.BytesIO(); doc.save(buf); buf.seek(0)
    return buf.getvalue()

# ================= UI =================
st.set_page_config(page_title="智飞投研系统", layout="wide", page_icon="📈")

st.markdown("""
<style>
    .stApp, section.main, .main, [data-testid="stAppViewContainer"] { background: #ffffff !important; transition: none !important; animation: none !important; will-change: auto !important; }
    [aria-label="Loading..."], [data-testid="stLoadingIndicator"] { opacity: 0 !important; pointer-events: none !important; }
    .stChatInputContainer { position: sticky !important; bottom: 0 !important; background: #ffffff !important; padding: 12px 0 8px 0 !important; z-index: 999 !important; border-top: 1px solid #e5e7eb !important; box-shadow: 0 -4px 10px rgba(0,0,0,0.03) !important; }
    .stChatMessage { margin-bottom: 8px; }
    img { max-width: 100% !important; height: auto !important; border-radius: 4px !important; }
    .file-status { font-size: 13px; color: #64748b; margin-top: 4px; }
    .stFileUploader > div:first-child { width: 40px !important; min-width: 40px !important; }
    .stFileUploader button { padding: 4px 8px !important; font-size: 18px !important; height: 38px !important; width: 40px !important; display: flex !important; align-items: center !important; justify-content: center !important; }
    .stFileUploader [data-testid="stFileUploadDropzone"] { width: 40px !important; min-width: 40px !important; padding: 2px !important; }
    .stFileUploader [data-testid="stFileUploadDropzone"] > div:first-child { display: none !important; }
</style>
""", unsafe_allow_html=True)

init_memory_db()

# ===== 初始化 =====
if "messages" not in st.session_state:
    st.session_state.messages = init_session_on_startup()
    if not st.session_state.messages:
        st.session_state.messages = []
        st.session_state.session_id = str(uuid.uuid4())
    if "rds_summary" not in st.session_state:
        st.session_state.rds_summary = ""

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
if "sync_status" not in st.session_state:
    st.session_state.sync_status = ""
if "last_activity" not in st.session_state:
    st.session_state.last_activity = datetime.now(BEIJING_TZ)

# ===== 侧边栏 =====
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

    st.divider()
    st.subheader("🔄 数据同步")
    col_sync_oss, col_sync_rds = st.columns(2)
    with col_sync_oss:
        if st.button("📤 同步到OSS", use_container_width=True):
            with st.spinner("同步中..."):
                sync_to_oss()
                st.session_state.sync_status = "✅ OSS同步完成"
                st.rerun()
    with col_sync_rds:
        if st.button("📥 从OSS导入", use_container_width=True):
            with st.spinner("导入中..."):
                sync_from_oss()
                st.session_state.sync_status = "✅ OSS导入完成"
                st.rerun()
    st.caption(st.session_state.sync_status or "💡 点击同步")

# ===== 渲染最近3轮 =====
RENDER_ROUNDS = 3
render_limit = RENDER_ROUNDS * 2
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

if st.session_state.compressed_summary:
    with st.chat_message("assistant"):
        st.markdown("📋 以下为已压缩的旧对话摘要，可复制后在新对话中粘贴使用：")
        st.text_area("摘要内容", value=st.session_state.compressed_summary, height=150, key="compressed_summary_display", disabled=True)
        st.caption("💡 选中上方文本后 Ctrl+C 复制，即可在新对话中粘贴使用")

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

# ===== 底部输入区 =====
st.divider()

if st.session_state.uploaded_files:
    file_names = [f["name"] for f in st.session_state.uploaded_files]
    st.caption("📎 " + " | ".join(file_names))
    if st.button("🗑️ 清空附件", key="clear_attachments", use_container_width=False):
        st.session_state.uploaded_files = []
        st.session_state.processed_files = set()
        st.rerun()

col_input, col_file, _ = st.columns([8, 1, 1])

with col_input:
    prompt = st.chat_input("输入股票/行业/事件，或描述你的需求...", key="main_input_fixed")
    if not prompt and st.session_state.quick_prompt:
        prompt = st.session_state.quick_prompt
        st.session_state.quick_prompt = None
        st.session_state.quick_scheme = None

with col_file:
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

# ===== 核心执行 =====
if prompt:
    session_id = get_or_create_session()
    round_num = len([m for m in st.session_state.messages if m.get("role") == "user"]) + 1

    uc = prompt
    if st.session_state.uploaded_files:
        file_contents = []
        for f in st.session_state.uploaded_files:
            if f["type"] == "txt":
                content = sanitize_text(f["data"], 800)
                file_contents.append(f"[{f['name']}]\n{content}")
        if file_contents:
            uc = uc + "\n\n【文件内容】\n" + "\n".join(file_contents)

    umsg = {
        "id": f"user_{datetime.now(BEIJING_TZ).strftime('%Y%m%d%H%M%S%f')}",
        "role": "user",
        "content": uc,
        "date": datetime.now(BEIJING_TZ).strftime("%Y%m%d"),
        "timestamp": datetime.now(BEIJING_TZ).isoformat(),
        "txt_files": [f for f in st.session_state.uploaded_files if f["type"] == "txt"],
        "session_id": session_id,
        "round_num": round_num
    }
    st.session_state.messages.append(umsg)
    st.session_state.uploaded_files = []
    st.session_state.processed_files = set()
    save_current_session()

    ctx_messages = st.session_state.messages[-30:] if len(st.session_state.messages) > 30 else st.session_state.messages

    st.session_state.pending_generation = True
    st.session_state.last_activity = datetime.now(BEIJING_TZ)
    st.rerun()

if st.session_state.pending_generation:
    st.session_state.pending_generation = False
    if not is_model_healthy():
        st.error("🔴 服务暂时不可用，请稍后重试（模型服务已熔断）")
        st.stop()

    if 'ctx_messages' not in locals():
        ctx_messages = st.session_state.messages[-30:] if len(st.session_state.messages) > 30 else st.session_state.messages

    comp_msgs = compress_history_by_date(ctx_messages)
    memory_hint = inject_memory()

    # 获取当前轮次的 session_id 和 round_num
    user_msgs = [m for m in st.session_state.messages if m["role"] == "user"]
    session_id = user_msgs[-1].get("session_id", str(uuid.uuid4()))
    round_num = user_msgs[-1].get("round_num", len(user_msgs))

    with st.chat_message("assistant"):
        try:
            mvars = get_market_vars()
            full_txt, tok_used = call_bailian_once(comp_msgs, selected_scheme, mvars, memory_hint)
            st.markdown(full_txt)
            st.session_state.last_token_usage = tok_used
            if selected_scheme and tok_used >= TOKEN_DISPLAY_THRESHOLD:
                st.caption(f"🔢 本次消耗: ~{tok_used:,} tokens")

            assistant_msg = {
                "id": f"assistant_{datetime.now(BEIJING_TZ).strftime('%Y%m%d%H%M%S%f')}",
                "role": "assistant",
                "content": full_txt,
                "date": datetime.now(BEIJING_TZ).strftime("%Y%m%d"),
                "timestamp": datetime.now(BEIJING_TZ).isoformat(),
                "session_id": session_id,
                "round_num": round_num
            }
            st.session_state.messages.append(assistant_msg)
            save_current_session()

            # 双写：RDS + SQLite
            messages_dict = {
                "messages": [
                    {"role": umsg["role"], "content": umsg["content"]},
                    {"role": assistant_msg["role"], "content": assistant_msg["content"]}
                ]
            }
            save_to_rds(session_id, round_num, messages_dict, umsg["timestamp"])
            save_to_sqlite(session_id, round_num, messages_dict, umsg["timestamp"])

            # 每10轮自动同步 OSS
            if round_num % 10 == 0:
                sync_to_oss()

            st.session_state.last_activity = datetime.now(BEIJING_TZ)
        except RuntimeError as e:
            st.error(str(e))
            if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
                st.session_state.messages.pop()
        except Exception as e:
            st.error(f"❌ 未知错误: {e}")
            logger.error(f"未知错误: {e}", exc_info=True)

# ===== 底部导出 =====
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

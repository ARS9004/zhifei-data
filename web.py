#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端轻量版 v1.8 (2026-08-17)
- [v1.8] 聊天框历史对话永久保留，不清空
- [v1.8] 新建会话不清空 messages
- [v1.8] 侧边栏新增历史对话列表 + 清空按钮
- [v1.8] 上文恢复从 OSS chat_history.jsonl + chat_summary_window.json
"""

import os
import re
import json
import time
import uuid
import atexit
import logging
import threading
import io
import concurrent.futures
from datetime import datetime
from typing import List, Dict

import streamlit as st
import dashscope
from dashscope import Application
import oss2
import pytz
from http import HTTPStatus
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

from docx import Document
from docx.shared import Pt
from docx.oxml.ns import qn

load_dotenv()

_backup_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="backup")
atexit.register(_backup_executor.shutdown, wait=False)

_recovery_store = {}
_recovery_lock = threading.Lock()

_CHINESE_CHAR_RE = re.compile(r'[\u4e00-\u9fff]')

def get_secret_or_env(key, secrets_key=None, default=None):
    if secrets_key:
        parts = secrets_key.split('.')
        try:
            value = st.secrets
            for p in parts:
                value = value[p]
            if value:
                return value
        except Exception:
            pass
    return os.getenv(key, default)


DASHSCOPE_API_KEY = get_secret_or_env("DASHSCOPE_API_KEY", "dashscope.api_key")
if not DASHSCOPE_API_KEY:
    raise RuntimeError("⛔ 请配置 DASHSCOPE_API_KEY")

MODEL_NAME = get_secret_or_env("MODEL_NAME", "model.name", "qwen-plus")
BEIJING_TZ = pytz.timezone('Asia/Shanghai')

OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
OSS_FILENAME = "chat_history.jsonl"
OSS_SUMMARY_WINDOW_FILE = "chat_summary_window.json"
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")

if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET:
    raise RuntimeError("⛔ 请配置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


_FAIL_LOCK = threading.Lock()
_FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
_MODEL_HEALTHY = True
_MAX_CONSECUTIVE_FAILURES = {"network": 5, "api": 3, "model": 3}


def reset_health_status():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    with _FAIL_LOCK:
        _FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
        _MODEL_HEALTHY = True


def mark_failure(error_type: str):
    global _FAIL_COUNTER, _MODEL_HEALTHY
    if error_type not in _FAIL_COUNTER:
        return
    with _FAIL_LOCK:
        _FAIL_COUNTER[error_type] += 1
        if error_type == "model" and _FAIL_COUNTER["model"] >= _MAX_CONSECUTIVE_FAILURES["model"]:
            _MODEL_HEALTHY = False
            logger.warning("⛔ 模型服务熔断：连续 %d 次模型调用失败", _FAIL_COUNTER["model"])


def is_model_healthy() -> bool:
    with _FAIL_LOCK:
        return _MODEL_HEALTHY


def _classify_error(e: Exception) -> str:
    err_str = str(e).lower()
    if not err_str:
        return "network"
    if any(kw in err_str for kw in ["timeout", "connection", "network", "resolve", "refused"]):
        return "network"
    if any(kw in err_str for kw in ["rate", "quota", "throttle", "limit", "429"]):
        return "api"
    return "model"


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ch = len(_CHINESE_CHAR_RE.findall(text))
    return int(ch * 1.0 + (len(text) - ch) / 4)


def get_or_create_session() -> str:
    if "session_id" not in st.session_state or not st.session_state.session_id:
        st.session_state.session_id = str(uuid.uuid4())
    return st.session_state.session_id


# ================= OSS 客户端 =================
def get_oss_client():
    client = AcsClient(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(3600)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)


def oss_get_with_retry(bucket, remote_path, max_retry=3, delay=1, **kwargs):
    for attempt in range(max_retry):
        try:
            return bucket.get_object(remote_path, **kwargs)
        except oss2.exceptions.NoSuchKey:
            raise
        except Exception as e:
            if attempt == max_retry - 1:
                raise
            logger.warning(f"OSS 读取重试 {attempt+1}/{max_retry}: {e}")
            time.sleep(delay * (attempt + 1))


def oss_head_with_retry(bucket, remote_path, max_retry=3, delay=1):
    for attempt in range(max_retry):
        try:
            return bucket.head_object(remote_path)
        except oss2.exceptions.NoSuchKey:
            raise
        except Exception as e:
            if attempt == max_retry - 1:
                raise
            logger.warning(f"OSS head 重试 {attempt+1}/{max_retry}: {e}")
            time.sleep(delay * (attempt + 1))


def read_oss_full():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        result = oss_get_with_retry(bucket, remote, max_retry=2)
        if result is None:
            return []
        content = result.read().decode('utf-8')
        lines = []
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return lines
    except oss2.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.warning(f"read_oss_full 失败: {e}")
        return []


def read_oss_tail(size=40960):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        meta = oss_head_with_retry(bucket, remote, max_retry=2)
        length = meta.content_length
        read_size = min(length, size)
        start = length - read_size
        result = oss_get_with_retry(bucket, remote, byte_range=(start, length - 1), max_retry=2)
        if result is None:
            return []
        content = result.read().decode('utf-8')
        if start > 0:
            first_nl = content.find('\n')
            if first_nl >= 0:
                content = content[first_nl + 1:]
        last_nl = content.rfind('\n')
        if last_nl >= 0:
            content = content[:last_nl + 1]
        lines = []
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return lines
    except oss2.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.debug(f"read_oss_tail 失败: {e}")
        return []


def get_cumulative_summary_from_oss() -> str:
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
        result = oss_get_with_retry(bucket, remote, max_retry=2)
        if result is None:
            return ""
        data = json.loads(result.read().decode('utf-8'))
        return data.get("cumulative", "")
    except Exception as e:
        logger.debug(f"读取累积摘要失败: {e}")
        return ""


# ================= 从 OSS 恢复上文（给模型用） =================
def _filter_and_dedup(lines: List[Dict], session_id: str = None) -> List[Dict]:
    seen = set()
    result = []
    for item in lines:
        if not isinstance(item, dict):
            continue
        key = (item.get("session_id"), item.get("round_num"))
        if key in seen or key[0] is None or key[1] is None:
            continue
        seen.add(key)
        result.append(item)
    if session_id:
        result = [item for item in result if item.get("session_id") == session_id]
    return result


def get_recent_messages_from_oss(session_id: str = None, limit: int = 5) -> List[Dict]:
    lines = read_oss_tail()
    if not lines:
        lines = read_oss_full()
        if not lines:
            return []

    valid_lines = _filter_and_dedup(lines, session_id)
    if not valid_lines:
        full_lines = read_oss_full()
        if full_lines:
            valid_lines = _filter_and_dedup(full_lines, session_id)
    if not valid_lines:
        return []

    sorted_lines = sorted(valid_lines, key=lambda x: x.get("ts", ""), reverse=True)
    recent = sorted_lines[:limit]

    result = []
    for item in reversed(recent):
        msgs_data = item.get("messages", {})
        if isinstance(msgs_data, str):
            try:
                msgs_data = json.loads(msgs_data)
            except Exception:
                msgs_data = {}
        if isinstance(msgs_data, dict) and "messages" in msgs_data:
            msg_list = msgs_data["messages"]
        elif isinstance(msgs_data, list):
            msg_list = msgs_data
        else:
            msg_list = []
        for msg in msg_list:
            if isinstance(msg, dict):
                result.append({
                    "role": msg.get("role"),
                    "content": msg.get("content")
                })
    return result


# ================= 后加载 =================
SESSION_WINDOW_SIZE = 12

def _read_summary_window():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
        result = oss_get_with_retry(bucket, remote, max_retry=2)
        if result is None:
            return {"window": [], "cumulative": ""}, True
        data = json.loads(result.read().decode('utf-8'))
        data.setdefault("window", [])
        data.setdefault("cumulative", "")
        return data, True
    except oss2.exceptions.NoSuchKey:
        return {"window": [], "cumulative": ""}, True
    except Exception as e:
        logger.warning(f"读取摘要窗口失败: {e}")
        return {"window": [], "cumulative": ""}, False


def _save_summary_window(window, cumulative):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
        data = {"window": window, "cumulative": cumulative}
        content = json.dumps(data, ensure_ascii=False).encode('utf-8')
        retries, delay = 2, 1
        for attempt in range(retries):
            try:
                bucket.put_object(remote, content)
                return
            except Exception as e:
                if attempt == retries - 1:
                    raise
                logger.warning(f"_save_summary_window 重试 {attempt+1}/{retries}: {e}")
                time.sleep(delay * (attempt + 1))
    except Exception as e:
        logger.warning(f"保存摘要窗口失败: {e}")


def merge_cumulative(previous: str, new_summary: str) -> str:
    if not previous:
        return new_summary
    retries, delay = 2, 1
    for attempt in range(retries):
        try:
            prompt = f"将以下两份摘要合并为一份200字以内的整体摘要：\n{previous}\n---\n{new_summary}"
            dashscope.api_key = DASHSCOPE_API_KEY
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                return resp.output.choices[0].message.content
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            if attempt == retries - 1:
                logger.warning(f"merge_cumulative 失败，回退到 previous: {e}")
                return previous
            time.sleep(delay)
            delay *= 2
    return previous


def generate_summary(messages: List[Dict]) -> str:
    if not messages:
        return ""
    retries, delay = 2, 1
    for attempt in range(retries):
        try:
            prompt = f"将以下对话压缩成300字摘要，突出核心标的、数据和结论：\n{json.dumps(messages, ensure_ascii=False)[:5000]}"
            dashscope.api_key = DASHSCOPE_API_KEY
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                return resp.output.choices[0].message.content
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            if attempt == retries - 1:
                logger.warning(f"generate_summary 失败: {e}")
                return ""
            time.sleep(delay)
            delay *= 2
    return ""


def trigger_backup_and_restore(old_session_id: str):
    """
    后加载：从 OSS 恢复旧会话上文给模型
    """
    with _recovery_lock:
        _recovery_store["backup_status"] = {
            "running": True, "summary_restored": False,
            "rounds_restored": 0, "error": None, "source": None
        }

    if not old_session_id:
        with _recovery_lock:
            _recovery_store["backup_status"] = {
                "running": False, "summary_restored": False,
                "rounds_restored": 0, "error": "无旧会话ID", "source": None
            }
        return

    try:
        all_lines = read_oss_full()
        if not all_lines:
            all_lines = read_oss_tail()

        old_lines = [l for l in all_lines if l.get("session_id") == old_session_id]
        old_lines.sort(key=lambda x: x.get("ts", ""))

        all_messages = []
        for item in old_lines:
            msgs_data = item.get("messages", {})
            if isinstance(msgs_data, str):
                try:
                    msgs_data = json.loads(msgs_data)
                except Exception:
                    continue
            if isinstance(msgs_data, dict) and "messages" in msgs_data:
                all_messages.extend(msgs_data["messages"])
            elif isinstance(msgs_data, list):
                all_messages.extend(msgs_data)

        if not all_messages:
            with _recovery_lock:
                _recovery_store["backup_status"] = {
                    "running": False, "summary_restored": False,
                    "rounds_restored": 0, "error": "旧会话无有效消息", "source": None
                }
            return

        summary = generate_summary(all_messages)
        summary_restored = bool(summary)
        last_3_rounds = all_messages[-6:]
        rounds_restored = len(last_3_rounds) // 2 if last_3_rounds else 0

        hint_parts = []
        if summary:
            hint_parts.append(f"【历史对话摘要】{summary}")
        if last_3_rounds:
            recent_text = []
            for msg in last_3_rounds:
                role = "用户" if msg.get("role") == "user" else "助手"
                content = str(msg.get("content", ""))[:200]
                recent_text.append(f"{role}: {content}")
            if recent_text:
                hint_parts.append("【最近对话】\n" + "\n".join(recent_text))

        data, read_ok = _read_summary_window()
        window = data.get("window", [])
        cumulative = data.get("cumulative", "")

        if not read_ok:
            if "cached_summary" in st.session_state and st.session_state.cached_summary:
                new_cumulative = merge_cumulative(st.session_state.cached_summary, summary)
            else:
                new_cumulative = summary
            with _recovery_lock:
                _recovery_store["cached_summary"] = new_cumulative
            if hint_parts:
                with _recovery_lock:
                    _recovery_store["pending"] = "\n\n".join(hint_parts)
            with _recovery_lock:
                _recovery_store["backup_status"] = {
                    "running": False, "summary_restored": summary_restored,
                    "rounds_restored": rounds_restored, "error": None,
                    "source": "OSS chat_history"
                }
            return

        if not window and not cumulative:
            if "cached_summary" in st.session_state and st.session_state.cached_summary:
                cumulative = st.session_state.cached_summary

        existing_sids = [w.get("session_id") for w in window]
        if old_session_id not in existing_sids:
            cumulative = merge_cumulative(cumulative, summary)
            window.append({
                "session_id": old_session_id,
                "summary": summary,
                "created_at": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
            })
            if len(window) > SESSION_WINDOW_SIZE:
                window = window[-SESSION_WINDOW_SIZE:]
            _save_summary_window(window, cumulative)
            with _recovery_lock:
                _recovery_store["cached_summary"] = cumulative

        if hint_parts:
            with _recovery_lock:
                _recovery_store["pending"] = "\n\n".join(hint_parts)

        with _recovery_lock:
            _recovery_store["backup_status"] = {
                "running": False, "summary_restored": summary_restored,
                "rounds_restored": rounds_restored, "error": None,
                "source": "OSS chat_history"
            }

    except Exception as e:
        logger.warning(f"后加载失败: {e}")
        with _recovery_lock:
            _recovery_store["backup_status"] = {
                "running": False, "summary_restored": False,
                "rounds_restored": 0, "error": str(e), "source": None
            }


# ================= sync_to_oss =================
def sync_to_oss(lines):
    st.session_state.oss_write_status = {"running": True, "success": False, "lines": 0, "error": None}

    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        content = "\n".join(json.dumps(line, ensure_ascii=False) for line in lines) + "\n"
        content_bytes = content.encode('utf-8')

        try:
            meta = bucket.head_object(remote)
            if meta.headers.get('x-oss-object-type') != 'Appendable':
                logger.warning("⚠️ OSS 文件为 Normal 类型，正在迁移为 Appendable...")
                existing = read_oss_full()
                all_lines = existing + lines
                all_content = "\n".join(json.dumps(l, ensure_ascii=False) for l in all_lines) + "\n"
                tmp_remote = remote + '.tmp'
                bucket.put_object(tmp_remote, all_content.encode('utf-8'))
                bucket.delete_object(remote)
                bucket.append_object(remote, 0, all_content.encode('utf-8'))
                bucket.delete_object(tmp_remote)
            else:
                pos = meta.content_length
                bucket.append_object(remote, pos, content_bytes)
        except oss2.exceptions.NoSuchKey:
            bucket.append_object(remote, 0, content_bytes)

        logger.info(f"✅ OSS 追加写入成功: {len(lines)} 行")
        st.session_state.oss_write_status["success"] = True
        st.session_state.oss_write_status["lines"] = len(lines)
        st.session_state.oss_write_status["running"] = False
        return len(lines)
    except Exception as e:
        logger.error(f"❌ OSS 追加写入失败: {e}")
        st.session_state.oss_write_status["error"] = str(e)
        st.session_state.oss_write_status["running"] = False
        raise


# ================= 模型调用 =================
def _clean_for_api(raw_msgs: List[Dict]) -> List[Dict]:
    valid = []
    for m in raw_msgs:
        role = m.get("role", "")
        content = m.get("content")
        if role not in ("user", "assistant"):
            continue
        if content is None or str(content).strip() == "":
            continue
        valid.append({"role": role, "content": str(content).strip()})

    if not valid:
        return []

    while valid and valid[0]["role"] != "user":
        valid.pop(0)

    cleaned = [valid[0]] if valid else []
    for m in valid[1:]:
        if m["role"] != cleaned[-1]["role"]:
            cleaned.append(m)

    while cleaned and cleaned[-1]["role"] != "user":
        cleaned.pop()

    return cleaned


def _extract_text_from_response(resp) -> str:
    output = resp.output

    text = getattr(output, 'text', None)
    if text and isinstance(text, str) and text.strip():
        return text

    choices = getattr(output, 'choices', None)
    if choices and isinstance(choices, list) and len(choices) > 0:
        msg = getattr(choices[0], 'message', None)
        if msg:
            content = getattr(msg, 'content', None)
            if content and isinstance(content, str) and content.strip():
                return content
        assistant = getattr(choices[0], 'assistant', None)
        if assistant:
            if isinstance(assistant, dict):
                content = assistant.get('content') or assistant.get('text')
                if content and isinstance(content, str) and content.strip():
                    return content
            elif isinstance(assistant, str) and assistant.strip():
                return assistant

    if isinstance(output, str) and output.strip():
        return output

    if isinstance(output, dict):
        text = output.get('text')
        if not (text and isinstance(text, str) and text.strip()):
            text = output.get('content')
        if text and isinstance(text, str) and text.strip():
            return text

    assistant = getattr(output, 'assistant', None)
    if assistant:
        if isinstance(assistant, dict):
            text = assistant.get('content') or assistant.get('text')
            if text and isinstance(text, str) and text.strip():
                return text
        elif isinstance(assistant, str) and assistant.strip():
            return assistant

    return ""


def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy():
        raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY

    if st.session_state.get("is_restoring"):
        for _ in range(20):
            with _recovery_lock:
                if "pending" in _recovery_store:
                    break
            time.sleep(0.5)
        st.session_state.is_restoring = False

    with _recovery_lock:
        recovery_ctx = _recovery_store.pop("pending", None)
        cached_summary = _recovery_store.pop("cached_summary", None)

    if cached_summary is not None:
        st.session_state.cached_summary = cached_summary

    context_text = ""
    if recovery_ctx:
        context_text = f"【历史投研记录，仅供参考】\n{recovery_ctx}\n\n---\n\n"
    else:
        summary = get_cumulative_summary_from_oss()
        if summary:
            context_text = f"【历史对话摘要】\n{summary}\n\n---\n\n"

    cleaned = _clean_for_api(messages)
    if not cleaned:
        raise RuntimeError("上下文中没有有效的用户消息")

    if context_text and cleaned:
        first_user_content = context_text + cleaned[0]['content']
        cleaned[0] = {"role": "user", "content": first_user_content}

    full_msgs = cleaned

    BAILIAN_APP_ID = "45db2f797bfd49229f757b04ed13ac92"

    retries, delay = 3, 2
    for attempt in range(retries):
        try:
            resp = Application.call(
                app_id=BAILIAN_APP_ID,
                messages=full_msgs,
                stream=False
            )
            if resp.status_code == HTTPStatus.OK:
                full_text = _extract_text_from_response(resp)
                if not full_text or not full_text.strip():
                    raise RuntimeError("模型返回内容为空")

                reset_health_status()
                return full_text
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            err_type = _classify_error(e)
            if attempt == retries - 1:
                mark_failure(err_type)
                raise RuntimeError(f"模型连续{retries}次调用失败: {e}")
            logger.warning(f"call_bailian 重试 {attempt+1}/{retries} [{err_type}]: {e}")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("未知错误")


def call_bailian_with_token_check(messages: List[Dict]) -> str:
    MAX_INPUT_TOKENS = 16000
    SYSTEM_PROMPT_RESERVE = 2500

    total_tokens = sum(estimate_tokens(str(m.get("content", ""))) for m in messages)
    if total_tokens > MAX_INPUT_TOKENS - SYSTEM_PROMPT_RESERVE:
        logger.warning(f"⚠️ 输入超长 ({total_tokens} tokens)，自动截断最近消息")
        trimmed = []
        running_tokens = 0
        limit = MAX_INPUT_TOKENS - SYSTEM_PROMPT_RESERVE
        for m in reversed(messages):
            t = estimate_tokens(str(m.get("content", "")))
            if running_tokens + t > limit:
                break
            trimmed.insert(0, m)
            running_tokens += t
        messages = trimmed
        logger.info(f"✅ 截断后 {len(messages)} 条消息，~{running_tokens} tokens")

    return call_bailian(messages)


def export_docx(messages):
    doc = Document()
    style = doc.styles['Normal']
    style.font.name = '仿宋'
    style.element.rPr.rFonts.set(qn('w:eastAsia'), '仿宋')

    title = doc.add_heading('', level=1)
    title_run = title.add_run("智飞投研对话记录")
    title_run.font.name = '仿宋'
    title_run._element.rPr.rFonts.set(qn('w:eastAsia'), '仿宋')
    title_run.font.size = Pt(16)
    title_run.font.bold = True

    for m in messages:
        role = "用户" if m.get("role") == "user" else "助手"
        content = m.get("content") or ""
        ts = m.get("timestamp", "")[:16]
        prefix = f"[{ts}] " if ts else ""
        para = doc.add_paragraph()
        run = para.add_run(f"{prefix}{role}：{content}")
        run.font.name = '仿宋'
        run._element.rPr.rFonts.set(qn('w:eastAsia'), '仿宋')
        run.font.size = Pt(14)

    buffer = io.BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer


# ================= 启动初始化 =================
def init_session_on_startup():
    """
    启动初始化：不碰 messages，只确保 session_id 存在
    聊天框渲染的数据由 st.session_state.messages 自己保留
    """
    result = {"success": False, "rounds": 0, "error": None, "source": None}

    # 只初始化不存在的 key，不覆盖已有的 messages
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "history_loaded" not in st.session_state:
        st.session_state.history_loaded = False
    if "session_id" not in st.session_state:
        st.session_state.session_id = ""
    if "cached_summary" not in st.session_state:
        st.session_state.cached_summary = ""
    if "render_offset" not in st.session_state:
        st.session_state.render_offset = 0

    # 确保 session_id 存在
    if not st.session_state.session_id:
        st.session_state.session_id = str(uuid.uuid4())

    # 如果 messages 已有数据（上次渲染保留的），直接返回
    if st.session_state.messages:
        result["success"] = True
        result["rounds"] = len([m for m in st.session_state.messages if m.get("role") == "user"])
        result["source"] = "已缓存"
        return result

    # messages 为空，尝试从 OSS 加载历史对话渲染到聊天框
    try:
        lines = read_oss_full()
        if not lines:
            lines = read_oss_tail()

        if lines:
            seen = set()
            deduped = []
            for item in lines:
                if not isinstance(item, dict):
                    continue
                key = (item.get("session_id"), item.get("round_num"))
                if key in seen or key[0] is None or key[1] is None:
                    continue
                seen.add(key)
                deduped.append(item)

            deduped.sort(key=lambda x: x.get("ts", ""))

            msgs = []
            for item in deduped:
                msgs_data = item.get("messages", {})
                if isinstance(msgs_data, str):
                    try:
                        msgs_data = json.loads(msgs_data)
                    except Exception:
                        continue
                if isinstance(msgs_data, dict) and "messages" in msgs_data:
                    msg_list = msgs_data["messages"]
                elif isinstance(msgs_data, list):
                    msg_list = msgs_data
                else:
                    msg_list = []
                for msg in msg_list:
                    if isinstance(msg, dict):
                        msgs.append(msg)

            if msgs:
                for m in msgs:
                    if "timestamp" not in m:
                        m["timestamp"] = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.messages = msgs
                result["success"] = True
                result["rounds"] = len([m for m in msgs if m.get("role") == "user"])
                result["source"] = "OSS chat_history"
                logger.info(f"✅ 从 OSS 渲染 {len(msgs)} 条消息到聊天框")
            else:
                result["success"] = True
                result["rounds"] = 0
                result["source"] = "无历史数据"
        else:
            result["success"] = True
            result["rounds"] = 0
            result["source"] = "无历史数据"
    except Exception as e:
        result["error"] = str(e)
        logger.warning(f"OSS 加载历史对话失败: {e}")

    # 加载累积摘要
    try:
        cumulative = get_cumulative_summary_from_oss()
        if cumulative:
            st.session_state.cached_summary = cumulative
    except Exception:
        pass

    st.session_state.history_loaded = True
    return result


# ================= 从 OSS 读取历史对话列表（侧边栏用） =================
def get_history_sessions():
    """返回按 session 分组的历史对话摘要列表"""
    try:
        lines = read_oss_full()
        if not lines:
            lines = read_oss_tail()
        if not lines:
            return []

        sessions = {}
        for item in lines:
            if not isinstance(item, dict):
                continue
            sid = item.get("session_id", "")
            if sid not in sessions:
                ts = item.get("ts", "")
                # 取第一条用户消息作为标题
                msgs_data = item.get("messages", {})
                if isinstance(msgs_data, str):
                    try:
                        msgs_data = json.loads(msgs_data)
                    except Exception:
                        msgs_data = {}
                title = ""
                if isinstance(msgs_data, dict) and "messages" in msgs_data:
                    for m in msgs_data["messages"]:
                        if m.get("role") == "user":
                            title = str(m.get("content", ""))[:30]
                            break
                sessions[sid] = {"session_id": sid, "title": title, "ts": ts, "rounds": 0}
            sessions[sid]["rounds"] += 1

        return sorted(sessions.values(), key=lambda x: x.get("ts", ""), reverse=True)
    except Exception:
        return []


# ================= UI =================
st.set_page_config(page_title="智飞投研·云端", layout="centered")
st.title("📱 智飞投研")

restore_result = init_session_on_startup()

if "generating" not in st.session_state:
    st.session_state.generating = False
if "stop" not in st.session_state:
    st.session_state.stop = False
if "messages" not in st.session_state:
    st.session_state.messages = []

total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
st.caption(f"{total_rounds} 轮对话")

chat_container = st.container()
with chat_container:
    MAX_RENDER_MSGS = 60
    total_msgs = len(st.session_state.messages)
    render_count = (st.session_state.get("render_offset", 0) + 1) * MAX_RENDER_MSGS
    render_start = max(0, total_msgs - render_count)
    render_msgs = st.session_state.messages[render_start:] if st.session_state.messages else []

    if render_start > 0:
        if st.button("⬆️ 加载更早的对话", use_container_width=True):
            st.session_state.render_offset = st.session_state.get("render_offset", 0) + 1
            st.rerun()

    for m in render_msgs:
        with st.chat_message(m.get("role", "user")):
            content = str(m.get("content") or "")
            if content.startswith("❌"):
                st.error(content)
            else:
                st.markdown(content)

user_input = st.chat_input("输入消息...", disabled=st.session_state.generating)

if user_input and not st.session_state.generating:
    st.session_state.stop = False
    st.session_state.generating = True

    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"]) + 1

    user_msg = {
        "role": "user",
        "content": user_input,
        "timestamp": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    }
    st.session_state.messages.append(user_msg)
    st.rerun()

if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"])
    user_msg = st.session_state.messages[-1]

    ctx = [{"role": m["role"], "content": str(m.get("content") or "")} for m in st.session_state.messages]

    try:
        with st.spinner("💭 思考中..."):
            reply = call_bailian_with_token_check(ctx)

        assistant_msg = {
            "role": "assistant",
            "content": reply,
            "timestamp": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        }
        st.session_state.messages.append(assistant_msg)

        messages_list = [user_msg, assistant_msg]
        messages_dict = {"messages": messages_list}
        sync_to_oss([{
            "session_id": st.session_state.session_id,
            "round_num": round_num,
            "messages": messages_dict,
            "ts": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        }])

    except Exception as e:
        st.error(f"❌ 错误: {e}")
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"❌ 调用失败: {e}",
            "timestamp": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        })

    st.session_state.generating = False
    st.rerun()

st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("➕ 新建会话", use_container_width=True, disabled=st.session_state.generating):
        old_sid = st.session_state.session_id
        if old_sid and st.session_state.messages:
            st.session_state.is_restoring = True
            _backup_executor.submit(trigger_backup_and_restore, old_sid)
        # 新建会话：只改 session_id，不清空 messages
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.history_loaded = False
        st.session_state.generating = False
        st.session_state.stop = False
        st.session_state.render_offset = 0
        st.rerun()
with col2:
    if st.button("🔄 重新生成", use_container_width=True, disabled=st.session_state.generating):
        if st.session_state.messages and st.session_state.messages[-1]["role"] == "assistant":
            st.session_state.messages.pop()
            st.session_state.generating = True
            st.session_state.stop = False
            st.rerun()
with col3:
    st.download_button(
        label="📤 导出DOCX",
        data=export_docx(st.session_state.messages),
        file_name=f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}.docx",
        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        use_container_width=True,
        key="export_docx_btn"
    )

# ================= 侧边栏 =================
with st.sidebar:
    st.subheader("📋 系统状态")

    # ---- 启动恢复状态 ----
    if restore_result.get("error"):
        st.error(f"❌ 启动恢复失败: {restore_result['error']}")
    elif restore_result.get("success") and restore_result.get("rounds", 0) > 0:
        st.success("✅ 上文恢复成功")
        st.caption(f"💬 最近 {restore_result['rounds']} 轮对话已恢复")
        if restore_result.get("source"):
            st.caption(f"📦 来源: {restore_result['source']}")
    elif restore_result.get("success") and restore_result.get("rounds", 0) == 0:
        st.caption("⏳ 暂无历史对话")
    else:
        st.caption("⏳ 尚未恢复上文")

    # ---- 后加载状态 ----
    with _recovery_lock:
        backup_status = _recovery_store.get("backup_status", {})
    if backup_status:
        st.divider()
        st.caption("🔄 后加载状态")
        if backup_status.get("running"):
            st.info("⏳ 正在恢复旧会话上文...")
        elif backup_status.get("error"):
            st.error(f"❌ 后加载失败: {backup_status['error']}")
        elif backup_status.get("summary_restored") or backup_status.get("rounds_restored", 0) > 0:
            st.success("✅ 旧会话上文已恢复")
            if backup_status.get("summary_restored"):
                st.caption("📄 摘要已生成")
            if backup_status.get("rounds_restored", 0) > 0:
                st.caption(f"💬 最近 {backup_status['rounds_restored']} 轮对话")
            if backup_status.get("source"):
                st.caption(f"📦 来源: {backup_status['source']}")
        else:
            st.caption("⏳ 暂无后加载数据")

    # ---- OSS 写入状态 ----
    st.divider()
    st.caption("💾 OSS 同步状态")
    oss_status = st.session_state.get("oss_write_status", {})
    if oss_status.get("running"):
        st.info("⏳ 正在写入 OSS...")
    elif oss_status.get("success"):
        st.success(f"✅ OSS 写入成功: {oss_status.get('lines', 0)} 行")
    elif oss_status.get("error"):
        st.error(f"❌ OSS 写入失败: {oss_status['error']}")
    else:
        st.caption("⏳ 尚未写入 OSS")

    # ---- 历史对话列表 ----
    st.divider()
    st.subheader("📜 历史对话")
    history_sessions = get_history_sessions()
    if history_sessions:
        for s in history_sessions:
            title = s["title"] if s["title"] else "（无标题）"
            st.caption(f"• {title} ({s['rounds']}轮)")
    else:
        st.caption("暂无历史对话")

    # ---- 清空历史对话 ----
    st.divider()
    if st.button("🗑️ 清空历史对话", use_container_width=True):
        st.session_state.messages = []
        st.session_state.render_offset = 0
        st.rerun()

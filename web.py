# -*- coding: utf-8 -*-
"""
智飞投研 · 云端轻量版
- 手机端使用，只读 OSS，不依赖 RDS
- 启动时自动恢复上下文：摘要 + 最后3轮完整对话
- 内核：百炼 API 调用
"""

import streamlit as st
import json
import oss2
import dashscope
import os
from datetime import datetime
from http import HTTPStatus
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

# ================= 配置 =================
OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"
OSS_CHAT_FILE = "chat_history.jsonl"
OSS_SUMMARY_FILE = "chat_summary.json"

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
MODEL_NAME = os.getenv("MODEL_NAME", "qwen-plus")

# ================= OSS =================
def get_oss_client():
    client = AcsClient(
        st.secrets["oss"]["access_key_id"],
        st.secrets["oss"]["access_key_secret"],
        OSS_REGION
    )
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)


def read_oss_jsonl(filename: str):
    """从 OSS 读取 JSONL 文件，返回解析后的列表"""
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + filename
        result = bucket.get_object(remote)
        content = result.read().decode('utf-8')
        records = []
        for line in content.strip().split('\n'):
            if line.strip():
                records.append(json.loads(line))
        return records
    except Exception:
        return []


def read_oss_json(filename: str):
    """从 OSS 读取 JSON 文件"""
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + filename
        result = bucket.get_object(remote)
        return json.loads(result.read().decode('utf-8'))
    except Exception:
        return {}


# ================= 上下文恢复 =================
def parse_messages(records: list) -> list:
    """
    解析 JSONL 记录，提取扁平化消息列表。
    每条记录格式：{"session_id":"...","round_num":1,"messages":"{...}","ts":"..."}
    """
    msgs = []
    for record in records:
        inner = record.get("messages")
        if isinstance(inner, str):
            inner = json.loads(inner)
        for m in inner.get("messages", []):
            msgs.append({
                "role": m.get("role", "user"),
                "content": m.get("content", ""),
                "ts": record.get("ts", "")
            })
    return msgs


def build_context() -> dict:
    """
    从 OSS 恢复上下文：
    1. 读 chat_summary.json 取摘要
    2. 读 chat_history.jsonl 取最后 3 轮完整对话
    返回 {"summary": str, "recent": list, "total_rounds": int}
    """
    # 读摘要
    summary_data = read_oss_json(OSS_SUMMARY_FILE)
    summary = summary_data.get("summary", "")

    # 读全部对话
    records = read_oss_jsonl(OSS_CHAT_FILE)
    all_msgs = parse_messages(records)

    # 取最后 3 轮（6条消息）
    recent = all_msgs[-6:] if len(all_msgs) > 6 else all_msgs

    return {
        "summary": summary,
        "recent": recent,
        "total_rounds": len([m for m in all_msgs if m["role"] == "user"])
    }


# ================= 百炼调用 =================
def call_bailian(user_msg: str, context: dict, history: list) -> str:
    """调用百炼，注入上下文"""
    dashscope.api_key = DASHSCOPE_API_KEY

    # 构建 system prompt
    system_parts = ["你是智飞投研助手。"]

    if context["summary"]:
        system_parts.append(f"\n【历史对话摘要】\n{context['summary']}")

    if context["recent"]:
        system_parts.append("\n【最近对话（用于接续上文）】")
        for m in context["recent"]:
            role = "用户" if m["role"] == "user" else "助手"
            system_parts.append(f"{role}：{m['content']}")

    system_prompt = "\n".join(system_parts)

    # 构建消息列表
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history[-10:])  # 最近 10 条本轮对话
    messages.append({"role": "user", "content": user_msg})

    try:
        resp = dashscope.Generation.call(
            model=MODEL_NAME,
            messages=messages,
            result_format="message",
            stream=False
        )
        if resp.status_code == HTTPStatus.OK and resp.output.choices:
            return resp.output.choices[0].message.content
        return f"❌ 调用失败: {resp.code} {resp.message}"
    except Exception as e:
        return f"❌ 错误: {e}"


# ================= UI =================
st.set_page_config(page_title="智飞投研", layout="centered")
st.title("📱 智飞投研")

# 初始化
if "messages" not in st.session_state:
    st.session_state.messages = []
if "context_loaded" not in st.session_state:
    st.session_state.context_loaded = False
if "context" not in st.session_state:
    st.session_state.context = build_context()
    st.session_state.context_loaded = True

# 显示上下文恢复提示
ctx = st.session_state.context
if ctx["summary"] and not st.session_state.messages:
    st.info(f"📋 已恢复上文（共 {ctx['total_rounds']} 轮对话），可继续")

# 渲染历史消息
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 输入
if prompt := st.chat_input("输入消息..."):
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("思考中..."):
            reply = call_bailian(prompt, st.session_state.context, st.session_state.messages[:-1])
            st.markdown(reply)

    st.session_state.messages.append({"role": "assistant", "content": reply})
    st.rerun()

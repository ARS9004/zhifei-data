# -*- coding: utf-8 -*-
import os
import json
import time
import io
from datetime import datetime

import streamlit as st
import requests
import oss2
import pymysql
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

# ================= 配置 =================
OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"
MODEL_NAME = "qwen-plus"
RENDER_ROUNDS = 3
CONTEXT_ROUNDS = 5
RDS_HOST = "rm-2zeli1or40iqt7vq66o.mysql.rds.aliyuncs.com"
RDS_PORT = 3306
RDS_DATABASE = "stock_db"
RDS_USER = "zhuanz1"
RDS_PASSWORD = "zhuanz1_2026"

# ================= OSS 客户端 =================
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

def get_week():
    y, w, _ = datetime.now().isocalendar()
    return f"{y}-W{w:02d}"

def load_dialogues():
    try:
        bucket = get_oss_client()
        remote = f"{OSS_PREFIX}chat_history_{get_week()}.jsonl"
        result = bucket.get_object(remote)
        msgs = []
        for line in result.read().decode('utf-8').strip().split('\n'):
            if line.strip():
                msgs.append(json.loads(line))
        return msgs
    except:
        return []

def save_dialogues(msgs):
    bucket = get_oss_client()
    remote = f"{OSS_PREFIX}chat_history_{get_week()}.jsonl"
    content = "\n".join(json.dumps(m, ensure_ascii=False) for m in msgs) + "\n"
    bucket.put_object(remote, content.encode('utf-8'))

def load_summary():
    try:
        bucket = get_oss_client()
        remote = f"{OSS_PREFIX}chat_history_{get_week()}.summary.json"
        result = bucket.get_object(remote)
        return json.loads(result.read().decode('utf-8')).get("summary", "")
    except:
        return ""

def save_summary(text):
    bucket = get_oss_client()
    remote = f"{OSS_PREFIX}chat_history_{get_week()}.summary.json"
    bucket.put_object(remote, json.dumps({"summary": text}).encode('utf-8'))

# ================= 调用百炼 =================
def call_bailian(messages, stop_flag):
    url = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    headers = {
        "Authorization": f"Bearer {st.secrets['dashscope']['api_key']}",
        "Content-Type": "application/json"
    }
    sys_p = f"当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    full_msgs = [{"role": "system", "content": sys_p}] + messages
    payload = {
        "model": MODEL_NAME,
        "input": {"messages": full_msgs},
        "parameters": {"result_format": "message", "incremental_output": True}
    }
    with requests.post(url, headers=headers, json=payload, stream=True, timeout=120) as resp:
        if resp.status_code != 200:
            return f"❌ API错误: {resp.status_code}"
        full = ""
        buf = ""
        for chunk in resp.iter_content(chunk_size=1024, decode_unicode=True):
            if stop_flag():
                return full + "\n\n⏹ 已停止"
            if chunk:
                buf += chunk
                lines = buf.split('\n')
                buf = lines[-1] if lines else ""
                for line in lines[:-1]:
                    if line.startswith('data:') and line[5:].strip() not in ['', '[DONE]']:
                        try:
                            data = json.loads(line[5:].strip())
                            delta = data.get('output', {}).get('choices', [{}])[0].get('message', {}).get('content', '')
                            if delta:
                                full += delta
                        except:
                            pass
        return full

# ================= 导出TXT =================
def export_txt(msgs):
    out = ""
    for m in msgs:
        out += f"{'用户' if m['role']=='user' else '助手'}：{m.get('content','')}\n\n"
    return out

def write_rds(msgs):
    try:
        conn = pymysql.connect(host=RDS_HOST, port=RDS_PORT, user=RDS_USER, password=RDS_PASSWORD, database=RDS_DATABASE, charset='utf8mb4')
        for i, m in enumerate(msgs):
            if m.get("role") == "user" and i+1 < len(msgs) and msgs[i+1].get("role") == "assistant":
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO chat_memory (user_msg, assistant_msg) VALUES (%s, %s)", (m.get("content"), msgs[i+1].get("content")))
        conn.commit()
        conn.close()
        return True
    except:
        return False

# ================= 界面 =================
st.set_page_config(page_title="智飞投研·云端", layout="centered")

st.title("📱 智飞投研")

# 加载数据
if "msgs" not in st.session_state:
    st.session_state.msgs = load_dialogues()
if "summary" not in st.session_state:
    st.session_state.summary = load_summary()
if "generating" not in st.session_state:
    st.session_state.generating = False
if "stop" not in st.session_state:
    st.session_state.stop = False

# 状态提示
total = len([m for m in st.session_state.msgs if m.get("role") == "user"])
st.caption(f"{total}轮对话")

# 渲染最近3轮
for m in st.session_state.msgs[-RENDER_ROUNDS*2:]:
    with st.chat_message(m["role"]):
        st.markdown(m.get("content", ""))

# ---- 输入 ----
user_input = st.chat_input("输入消息...")

if user_input and not st.session_state.generating:
    st.session_state.stop = False
    st.session_state.generating = True
    
    # 写入用户消息
    st.session_state.msgs.append({"role": "user", "content": user_input, "timestamp": datetime.now().isoformat()})
    save_dialogues(st.session_state.msgs)
    
    # 构建上下文：摘要 + 最近5轮
    ctx = []
    if st.session_state.summary:
        ctx.append({"role": "system", "content": f"【历史摘要】{st.session_state.summary}"})
    ctx.extend(st.session_state.msgs[-CONTEXT_ROUNDS*2:-1])
    ctx.append({"role": "user", "content": user_input})
    
    # 调用模型
    def stop_flag():
        return st.session_state.stop
    
    reply = call_bailian(ctx, stop_flag)
    
    # 写入回复
    st.session_state.msgs.append({"role": "assistant", "content": reply, "timestamp": datetime.now().isoformat()})
    save_dialogues(st.session_state.msgs)
    
    # 简单摘要更新：每10轮更新一次
    if total % 10 == 0 and total > 0:
        try:
            summary_prompt = f"将以下对话压缩成200字摘要：{json.dumps(st.session_state.msgs[-50:], ensure_ascii=False)[:4000]}"
            summary = call_bailian([{"role": "user", "content": summary_prompt}], lambda: False)
            if summary and "已停止" not in summary:
                st.session_state.summary = summary
                save_summary(summary)
        except:
            pass
    
    st.session_state.generating = False
    st.rerun()

# ---- 生成中的暂停 ----
if st.session_state.generating:
    with st.chat_message("assistant"):
        st.markdown("⏳ 生成中...")
    if st.button("⏹ 暂停", use_container_width=True):
        st.session_state.stop = True
        st.rerun()

# ---- 操作按钮 ----
if not st.session_state.generating and st.session_state.msgs:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("🔄 重新生成", use_container_width=True):
            if st.session_state.msgs and st.session_state.msgs[-1]["role"] == "assistant":
                st.session_state.msgs.pop()
                st.session_state.generating = True
                st.session_state.stop = False
                st.rerun()
    with col2:
        if st.button("📤 导出TXT", use_container_width=True):
            st.download_button("下载", export_txt(st.session_state.msgs), f"对话_{datetime.now().strftime('%Y%m%d')}.txt", "text/plain", key="dl")
    with col3:
        if st.button("💾 写入RDS", use_container_width=True):
            if write_rds(st.session_state.msgs):
                st.success("✅ RDS已写入")
            else:
                st.error("❌ RDS写入失败")
    with col4:
        if st.button("📤 上传文件", use_container_width=True):
            st.info("⬇ 下方上传")

# ---- 文件上传 ----
with st.expander("📎 上传文件"):
    f = st.file_uploader("选择文件", type=['txt','log','csv','md','py','json'], label_visibility="collapsed")
    if f:
        try:
            content = f.read().decode('utf-8', errors='ignore')[:50000]
            st.session_state.msgs.append({"role": "user", "content": f"【文件】{f.name}\n```\n{content}\n```"})
            save_dialogues(st.session_state.msgs)
            st.success(f"✅ {f.name} 已挂载")
            st.rerun()
        except Exception as e:
            st.error(f"❌ {e}")
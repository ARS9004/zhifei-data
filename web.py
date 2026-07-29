import streamlit as st
import json
import oss2
from datetime import datetime
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

# ===== 配置 =====
OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"
WEEK_KEY = "2026-W31"
FILE_NAME = f"chat_history_{WEEK_KEY}.json"
REMOTE_PATH = OSS_PREFIX + FILE_NAME

# ===== 获取STS凭证 =====
def get_sts_token():
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
    return json.loads(resp)["Credentials"]

# ===== 获取OSS客户端 =====
def get_oss_client():
    creds = get_sts_token()
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

# ===== 读取OSS =====
def read_oss():
    bucket = get_oss_client()
    try:
        result = bucket.get_object(REMOTE_PATH)
        return json.loads(result.read().decode("utf-8"))
    except:
        return []

# ===== 写入OSS（覆盖） =====
def write_oss(data):
    bucket = get_oss_client()
    local_temp = f"temp_{FILE_NAME}"
    with open(local_temp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    with open(local_temp, "rb") as f:
        bucket.put_object(REMOTE_PATH, f)
    import os
    os.remove(local_temp)

# ===== 页面 =====
st.set_page_config(page_title="OSS 读写测试", layout="centered")
st.title(f"📦 OSS 读写测试 - {WEEK_KEY}")

# 读取数据
with st.spinner("读取中..."):
    messages = read_oss()

if not messages:
    st.warning("暂无数据")
    st.stop()

# 按时间排序取最近5轮（10条）
def get_time(m):
    ts = m.get("timestamp") or m.get("time") or m.get("date")
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except:
        return datetime.min

messages_sorted = sorted(messages, key=get_time)
recent = messages_sorted[-10:] if len(messages_sorted) >= 10 else messages_sorted

st.caption(f"共 {len(messages)} 条消息，显示最近 {len(recent)} 条（5轮）")

for msg in recent:
    with st.chat_message(msg.get("role", "unknown")):
        st.markdown(msg.get("content", ""))

# ===== 输入框 =====
user_input = st.chat_input("输入测试消息...")
if user_input:
    # 构造新消息
    new_msg = {
        "role": "user",
        "content": user_input,
        "timestamp": datetime.now().isoformat()
    }
    # 追加到列表
    messages.append(new_msg)
    # 写回OSS
    write_oss(messages)
    st.success("✅ 已写入 OSS")
    st.rerun()
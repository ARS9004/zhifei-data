# web.py —— 只读 W30，取最近10轮（20条消息）
import streamlit as st
import json
import oss2
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
from datetime import datetime

# ===== 配置 =====
OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"
WEEK_KEY = "2026-W30"  # 直接指定读 W30

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

# ===== 读取OSS文件 =====
def read_oss_file(week_key):
    bucket = get_oss_client()
    remote_path = OSS_PREFIX + f"chat_history_{week_key}.json"
    try:
        result = bucket.get_object(remote_path)
        content = result.read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        st.error(f"读取失败: {e}")
        return None

# ===== 页面 =====
st.set_page_config(page_title="智飞投研 · 最近对话", layout="centered")
st.title("💬 最近10轮对话")

with st.spinner("加载中..."):
    messages = read_oss_file(WEEK_KEY)
    if messages is None:
        st.error(f"❌ 读取 {WEEK_KEY} 失败")
        st.stop()
    
    if not messages:
        st.info("📭 文件为空")
        st.stop()

    # 按时间排序
    def get_time(m):
        ts = m.get("timestamp") or m.get("time") or m.get("date")
        if isinstance(ts, str):
            try:
                if 'T' in ts:
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except:
                return datetime.min
        return datetime.min

    messages_sorted = sorted(messages, key=get_time)
    # 取最近20条（10轮）
    recent = messages_sorted[-20:] if len(messages_sorted) >= 20 else messages_sorted

st.caption(f"📅 来源: {WEEK_KEY} ｜ 共 {len(recent)} 条消息（最近10轮）")

for msg in recent:
    role = msg.get("role", "")
    content = msg.get("content", "")
    with st.chat_message(role):
        st.markdown(content)

if st.button("🔄 刷新"):
    st.rerun()
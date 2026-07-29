# web.py —— 智飞投研 · OSS 读写测试 + STS 接口
import streamlit as st
import json
import oss2
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
from datetime import datetime

# ============================================================
#  检测 STS 凭证请求（通过 ?action=get_sts_token 触发）
# ============================================================
if "action" in st.query_params and st.query_params.action == "get_sts_token":
    try:
        client = AcsClient(
            st.secrets["oss"]["access_key_id"],
            st.secrets["oss"]["access_key_secret"],
            "cn-beijing"
        )
        req = AssumeRoleRequest.AssumeRoleRequest()
        req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
        req.set_RoleSessionName("web-oss-session")
        req.set_DurationSeconds(900)
        resp = client.do_action_with_exception(req)
        creds = json.loads(resp)["Credentials"]
        st.json({
            "AccessKeyId": creds["AccessKeyId"],
            "AccessKeySecret": creds["AccessKeySecret"],
            "SecurityToken": creds["SecurityToken"]
        })
    except Exception as e:
        st.json({"error": str(e)})
    st.stop()

# ============================================================
#  正常页面（OSS 读写测试界面）
# ============================================================
st.set_page_config(page_title="智飞投研 · OSS 测试", layout="centered")
st.title("📦 OSS 读写测试")

OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"

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

if st.button("🔍 读取 OSS chat_history.json"):
    with st.spinner("读取中..."):
        try:
            creds = get_sts_token()
            auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
            bucket = oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)
            result = bucket.get_object("chat_history.json")
            content = result.read().decode("utf-8")
            st.success("✅ 读取成功")
            st.json(json.loads(content))
        except oss2.exceptions.NoSuchKey:
            st.warning("⚠️ 文件 chat_history.json 不存在")
        except Exception as e:
            st.error(f"❌ 读取失败: {e}")

if st.button("📝 写入测试数据到 OSS"):
    with st.spinner("写入中..."):
        try:
            creds = get_sts_token()
            auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
            bucket = oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)
            test_data = {"test": "hello", "time": str(datetime.now())}
            bucket.put_object("chat_history.json", json.dumps(test_data, ensure_ascii=False).encode("utf-8"))
            st.success("✅ 写入成功")
        except Exception as e:
            st.error(f"❌ 写入失败: {e}")
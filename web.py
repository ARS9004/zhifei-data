# app.py —— 智飞投研 · Streamlit 入口（含 STS 接口）
import streamlit as st
from streamlit.starlette import App
from starlette.routing import Route
from starlette.responses import JSONResponse
import json
import os
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

# ===== 自定义路由：获取 STS 临时凭证 =====
async def get_sts_token(request):
    try:
        # 从 st.secrets 读取 OSS 凭证
        client = AcsClient(
            st.secrets["oss"]["access_key_id"],
            st.secrets["oss"]["access_key_secret"],
            st.secrets["oss"]["region"]           # 如 cn-hangzhou
        )
        req = AssumeRoleRequest.AssumeRoleRequest()
        req.set_RoleArn(st.secrets["oss"]["role_arn"])
        req.set_RoleSessionName("web-oss-session")
        req.set_DurationSeconds(900)              # 15分钟有效期
        resp = client.do_action_with_exception(req)
        creds = json.loads(resp)["Credentials"]
        return JSONResponse({
            "AccessKeyId": creds["AccessKeyId"],
            "AccessKeySecret": creds["AccessKeySecret"],
            "SecurityToken": creds["SecurityToken"]
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ===== 创建 App，挂载自定义路由，并指向原有的 web.py =====
app = App(
    "web.py",  # 你原有的 Streamlit 主文件
    routes=[
        Route("/api/sts", get_sts_token),
    ],
)
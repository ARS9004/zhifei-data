#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import streamlit as st

st.set_page_config(page_title="智飞投研 · 测试版", layout="centered")

st.title("✅ 智飞投研 · 基础版")
st.write("应用已成功启动，当前没有任何外部依赖。")
st.write("接下来可以逐步添加 RDS 和百炼功能。")

if st.button("点击测试"):
    st.info("按钮响应正常，Streamlit 工作正常。")